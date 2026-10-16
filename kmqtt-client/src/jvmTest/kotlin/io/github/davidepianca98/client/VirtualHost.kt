package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield

/**
 * Хост клиента на виртуальном времени (W6 §7.1, S06b-T05b): цикл [HostDriver.runOnce] — корутина в
 * `backgroundScope` теста (`runTest`, `StandardTestDispatcher`), без потоков. Время — `testScheduler.currentTime`
 * (хост сам — [MqttClock] клиента). Цикл повторяет `NioActorHost`: события транспорта → `runOnce` → действия по
 * порядку (`Connect` — новый [FakeTransport] брокера [broker], прежний закрывается) → если действия сразу дали
 * события — следующий оборот, иначе ждать [wakeup] не дольше ближайшего дедлайна (виртуальная задержка).
 *
 * Тест двигает время `advanceTimeBy`/`runCurrent` или просто ждёт исходов (`runTest` сам сдвигает время, когда все
 * корутины ждут). `advanceUntilIdle` с периодическими сроками (keepAlive, переподключение) не завершится.
 */
@OptIn(MqttCoreApi::class)
internal class VirtualHost(private val scope: TestScope, val broker: FakeBroker = FakeBroker()) : MqttHost, MqttClock {
    private val wake = Channel<Unit>(Channel.CONFLATED)
    private var job: Job? = null
    private var started = false
    private var closed = false
    private var stoppedNotified = false

    /** Сколько раз вызван `runOnce`. */
    var steps = 0
        private set

    /** Все `HostAction` по порядку. */
    val actions = ArrayList<HostAction>()

    val isRunning: Boolean get() = job?.isActive == true

    override fun nowMs(): Long = scope.testScheduler.currentTime

    override fun start(driver: HostDriver) {
        check(!started && !closed) { "VirtualHost: start() once, not after close()" }
        started = true
        job = scope.backgroundScope.launch { loop(driver) }
    }

    override fun wakeup() {
        if (started && !closed) wake.trySend(Unit)
    }

    /** Пробуждение от транспортов (в т. ч. дописывающихся после [close]). */
    private fun poke() {
        if (started) wake.trySend(Unit)
    }

    /** Как у `NioActorHost`: `runOnce` больше не вызывается, текущий транспорт закрывается, дописывание доводится. */
    override suspend fun close(timeoutMs: Long): Boolean {
        if (closed) return true
        closed = true
        val running = job ?: return true
        wake.trySend(Unit)
        val stopped = withTimeoutOrNull(timeoutMs) { running.join() } != null
        if (!stopped) running.cancelAndJoin()
        return stopped
    }

    /** Транспорты на дописывании ([HostAction.FlushAndClose]) с их сроками. */
    private val flushing = ArrayList<Pair<FakeTransport, Long>>()

    private fun startFlush(transport: FakeTransport, deadlineMs: Long) {
        transport.closeOutput()
        transport.events()
        if (transport.pendingWriteBytes <= 0L) {
            transport.close()
            return
        }
        transport.setReadInterest(true)
        flushing += transport to deadlineMs
    }

    private fun serviceFlushing(now: Long) {
        val iterator = flushing.iterator()
        while (iterator.hasNext()) {
            val (t, deadline) = iterator.next()
            t.poll(now)
            t.events()
            if (t.pendingWriteBytes <= 0L || now >= deadline) {
                t.close()
                iterator.remove()
            }
        }
    }

    private fun flushDeadline(): Long? =
        flushing.flatMap { (t, deadline) -> listOfNotNull(deadline, t.nextDeadlineMs()) }.minOrNull()

    private suspend fun waitUntil(deadline: Long?) {
        if (deadline == null) {
            wake.receive()
        } else {
            val wait = deadline - nowMs()
            if (wait > 0) withTimeoutOrNull(wait) { wake.receive() } else wake.tryReceive()
        }
    }

    private suspend fun loop(driver: HostDriver) {
        var transport: FakeTransport? = null
        var cause: Throwable? = null
        try {
            var pending = ArrayList<TransportEvent>()
            while (!closed) {
                val now = nowMs()
                serviceFlushing(now)
                transport?.let {
                    it.poll(now)
                    pending.addAll(it.events())
                }
                val events = pending
                pending = ArrayList()
                val step = driver.runOnce(now, events)
                steps++
                for (action in step.actions) {
                    if (closed) break
                    actions += action
                    when (action) {
                        is HostAction.Connect -> {
                            transport?.close()
                            val created = FakeTransport(broker, ::poke)
                            broker.transports += created
                            transport = created
                            created.connect(action.host, action.port, now + action.timeoutMs)
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
                if (closed) break
                transport?.let { pending.addAll(it.events()) }
                yield()
                if (pending.isNotEmpty()) continue
                waitUntil(listOfNotNull(step.nextDeadlineMs, transport?.nextDeadlineMs(), flushDeadline()).minOrNull())
            }
            transport?.close()
            transport = null
            while (true) {
                serviceFlushing(nowMs())
                if (flushing.isEmpty()) break
                waitUntil(flushDeadline())
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            cause = e
        } finally {
            transport?.close()
            flushing.forEach { it.first.close() }
            flushing.clear()
            if (!stoppedNotified) {
                stoppedNotified = true
                driver.onStopped(cause)
            }
        }
    }
}
