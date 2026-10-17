package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.node.clearTimeout
import io.github.davidepianca98.client.node.setImmediate
import io.github.davidepianca98.client.node.setTimeout
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.withTimeoutOrNull

/**
 * Хост клиента для Node.js (SEVEN-250, W6 §4.6 в однопоточной среде): цикл клиента — обороты на цикле событий Node,
 * своего потока нет (в Node он один; блокирующих вызовов нет ни у хоста, ни у [NodeTransport]).
 *
 * Оборот — то же, что у JVM-хоста `NioActorHost`: события транспорта → [HostDriver.runOnce] → действия по порядку
 * (`Connect` — новый транспорт из фабрики, прежний закрывается, исключение фабрики или `connect` — событие
 * [TransportEvent.Failed] следующим оборотом; `Write`, `PauseRead`, `CloseTransport`; `FlushAndClose` — транспорт
 * отдаётся на дописывание: его события отбрасываются, чтение включено, закрытие по пустой очереди или сроку).
 * Обороты планирует [wakeup] и события сокетов (`setImmediate`, лишние пробуждения сливаются в один оборот) и один
 * таймер на ближайший дедлайн (драйвера, подключения, дописывания).
 *
 * [close]: текущий транспорт закрывается сразу (идущее подключение прерывается, H5), отданные на дописывание
 * дописываются не дольше своих сроков, затем [HostDriver.onStopped]; приостановка — до этого момента, не дольше
 * таймаута. Вызов [close] из [HostDriver.runOnce] не ждёт (вернёт `false`): остановка — после текущего оборота.
 * Исключение из `runOnce` останавливает хост ([HostDriver.onStopped] с ним) и не уходит в цикл событий.
 * Одноразовый: после [close] не перезапускается.
 *
 * Браузер: нужен только WebSocket-транспорт, его нет (SEVEN-250, план подкарточек); этот хост — только Node.
 */
@OptIn(MqttCoreApi::class)
public class NodeHost internal constructor(
    private val clock: MqttClock,
    private val transportFactory: NodeTransportFactory
) : MqttHost {

    /** Хост с TCP-транспортом. [clock] должен совпадать с часами драйвера. */
    public constructor(clock: MqttClock = MqttClock.Monotonic) : this(clock, NodeTransportFactory.Tcp)

    private var state = NEW
    private var driver: HostDriver? = null
    private var transport: MqttTransport? = null
    private val flushing = ArrayList<Flushing>()
    private var pending = ArrayList<TransportEvent>()
    private var scheduled = false
    private var timer: dynamic = null
    private var timerAt: Long? = null
    private var stopRequested = false
    private var stopping = false
    private var inTurn = false
    private val stopped = CompletableDeferred<Unit>()
    private val context = NodeTransportContext(::schedule)

    @MqttCoreApi
    override fun start(driver: HostDriver) {
        check(state == NEW) { "NodeHost: start() once, not after close()" }
        state = STARTED
        this.driver = driver
        schedule()
    }

    override fun wakeup() {
        if (state == STARTED) schedule()
    }

    override suspend fun close(timeoutMs: Long): Boolean {
        if (state == NEW) {
            state = CLOSED
            stopped.complete(Unit)
            return true
        }
        state = CLOSED
        if (!stopped.isCompleted) {
            stopRequested = true
            schedule()
        }
        if (inTurn) return false
        return withTimeoutOrNull(timeoutMs.coerceAtLeast(1)) { stopped.await() } != null
    }

    private fun schedule() {
        if (scheduled || stopped.isCompleted) return
        scheduled = true
        setImmediate {
            scheduled = false
            turn()
        }
    }

    private fun turn() {
        if (stopped.isCompleted) return
        inTurn = true
        try {
            if (stopping || stopRequested) {
                stopTurn()
                return
            }
            val d = driver ?: return
            val now = clock.nowMs()
            serviceFlushing(now)
            transport?.let {
                it.poll(now)
                pending.addAll(it.events())
            }
            val events: List<TransportEvent> = pending
            pending = ArrayList()
            val step = d.runOnce(now, events)
            for (action in step.actions) {
                if (stopRequested) break
                when (action) {
                    is HostAction.Connect -> {
                        transport?.close()
                        transport = null
                        transport = openTransport(action, now)
                    }
                    is HostAction.Write -> transport?.write(action.bytes)
                    is HostAction.PauseRead -> transport?.setReadInterest(!action.paused)
                    HostAction.CloseTransport -> {
                        transport?.close()
                        transport = null
                    }
                    is HostAction.FlushAndClose -> {
                        transport?.let { startFlush(it, now + action.timeoutMs) }
                        transport = null
                    }
                }
            }
            if (stopRequested) {
                stopTurn()
                return
            }
            transport?.let { pending.addAll(it.events()) }
            if (pending.isNotEmpty()) schedule()
            armTimer(minDeadline(minDeadline(step.nextDeadlineMs, transport?.nextDeadlineMs()), flushDeadline()))
        } catch (t: Throwable) {
            finish(t)
        } finally {
            inTurn = false
        }
    }

    /** Остановка: текущий транспорт — сразу (H5), отданные на дописывание — до пустой очереди или срока. */
    private fun stopTurn() {
        stopping = true
        transport?.close()
        transport = null
        serviceFlushing(clock.nowMs())
        if (flushing.isEmpty()) {
            finish(null)
        } else {
            armTimer(flushDeadline())
        }
    }

    private fun finish(cause: Throwable?) {
        if (stopped.isCompleted) return
        state = CLOSED
        stopRequested = true
        cancelTimer()
        runCatching { transport?.close() }
        transport = null
        for (f in flushing) runCatching { f.transport.close() }
        flushing.clear()
        pending = ArrayList()
        try {
            driver?.onStopped(cause)
        } catch (_: Throwable) {
        }
        stopped.complete(Unit)
    }

    private fun openTransport(action: HostAction.Connect, now: Long): MqttTransport? {
        var created: MqttTransport? = null
        return try {
            created = transportFactory.create(context)
            created.connect(action.host, action.port, now + action.timeoutMs)
            created
        } catch (e: Throwable) {
            // Throwable, не Exception: ошибка JS (`Error`), брошенная синхронно из `net.connect`, — не `Exception`
            runCatching { created?.close() }
            pending += TransportEvent.Failed(MqttError.Io(NodeTransport.codeOf(e), e))
            schedule()
            null
        }
    }

    private class Flushing(val transport: MqttTransport, val deadlineMs: Long)

    private fun startFlush(transport: MqttTransport, deadlineMs: Long) {
        transport.closeOutput()
        transport.events()
        if (transport.pendingWriteBytes <= 0L) {
            transport.close()
            return
        }
        // Закрытие с непрочитанными входящими даёт RST (H1): пока дописываем — читаем и отбрасываем
        transport.setReadInterest(true)
        flushing += Flushing(transport, deadlineMs)
    }

    private fun serviceFlushing(now: Long) {
        if (flushing.isEmpty()) return
        val iterator = flushing.iterator()
        while (iterator.hasNext()) {
            val f = iterator.next()
            f.transport.poll(now)
            f.transport.events()
            if (f.transport.pendingWriteBytes <= 0L || now >= f.deadlineMs) {
                f.transport.close()
                iterator.remove()
            }
        }
    }

    private fun flushDeadline(): Long? = flushing.minOfOrNull { it.deadlineMs }

    private fun armTimer(deadline: Long?) {
        if (deadline == null) {
            cancelTimer()
            return
        }
        if (timer != null && timerAt == deadline) return
        cancelTimer()
        val delay = (deadline - clock.nowMs()).coerceIn(0L, MAX_TIMER_MS)
        timerAt = deadline
        timer = setTimeout({
            timer = null
            timerAt = null
            schedule()
        }, delay.toInt())
    }

    private fun cancelTimer() {
        val t = timer ?: return
        clearTimeout(t)
        timer = null
        timerAt = null
    }

    private companion object {
        const val NEW = 0
        const val STARTED = 1
        const val CLOSED = 2

        /** Предел `setTimeout` (2^31 − 1 мс); дальний дедлайн — переустановкой. */
        const val MAX_TIMER_MS = 2_147_483_647L

        fun minDeadline(a: Long?, b: Long?): Long? = when {
            a == null -> b
            b == null -> a
            else -> minOf(a, b)
        }
    }
}
