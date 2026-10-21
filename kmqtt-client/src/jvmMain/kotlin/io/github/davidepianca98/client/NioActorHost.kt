package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runInterruptible
import java.nio.channels.Selector
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/** Глобальный (на процесс) счётчик имён потоков хоста. */
private val hostThreadCounter = AtomicLong(0)

/** Следующее имя потока хоста: `kmqtt-io-<n>`, `n` растёт в процессе. */
internal fun nextHostThreadName(): String = NioActorHost.THREAD_NAME_PREFIX + hostThreadCounter.incrementAndGet()

/**
 * JVM-хост клиента (модель «поток-актор»): один daemon-поток `kmqtt-io-<n>` с [Selector].
 *
 * Цикл потока:
 * 1. события транспорта (результат резолва, дедлайн подключения, готовность сокета) → [HostDriver.runOnce];
 * 2. действия шага ([HostAction]) исполняются по порядку: `Connect` — новый транспорт из фабрики (прежний
 *    закрывается), неблокирующий connect с дедлайном (исключение фабрики или `connect` — событие
 *    [TransportEvent.Failed] следующим оборотом, хост живёт); `Write` — запись без блокировки, остаток по `OP_WRITE`;
 *    `PauseRead` — снять/вернуть `OP_READ` текущего транспорта; `CloseTransport` — закрыть канал;
 *    `FlushAndClose` — отдать текущий транспорт на дописывание: он больше не текущий, его события
 *    отбрасываются, чтение включено, канал закрывается на обороте, где `pendingWriteBytes == 0`, или по сроку;
 * 3. `select` до ближайшего дедлайна (драйвера, подключения или дописывания); [wakeup] прерывает его сразу.
 *    Если действия сразу дали события — `selectNow` и следующий оборот без сна.
 *
 * Резолв имени — на `Dispatchers.IO`, остальное — на потоке хоста. [close] прерывает и идущее
 * подключение: текущий транспорт закрывается сразу, дописывающиеся — не дольше своих сроков, затем поток
 * выходит, закрывая селектор; приостановка до выхода потока — не дольше таймаута.
 * Одноразовый: после [close] не перезапускается.
 *
 * `PauseRead` относится к текущему транспорту: новый транспорт (`Connect`) начинает с включённым чтением.
 */
@OptIn(MqttCoreApi::class)
public class NioActorHost internal constructor(
    /** Имя потока хоста. */
    public val name: String,
    private val clock: MqttClock,
    private val transportFactory: NioTransportFactory,
    private val readBufferSize: Int
) : MqttHost {

    /** Хост с TCP-транспортом. [clock] должен совпадать с часами драйвера. */
    public constructor(
        name: String = nextHostThreadName(),
        clock: MqttClock = MqttClock.Monotonic
    ) : this(name, clock, NioTransportFactory.Tcp, DEFAULT_READ_BUFFER)

    private val state = AtomicInteger(NEW)

    @Volatile
    private var stopRequested = false

    @Volatile
    private var selector: Selector? = null

    @Volatile
    private var thread: Thread? = null

    @MqttCoreApi
    override fun start(driver: HostDriver) {
        check(state.compareAndSet(NEW, STARTED)) { "NioActorHost: start() once, not after close()" }
        val sel = try {
            Selector.open()
        } catch (e: Throwable) {
            state.set(CLOSED)
            throw e
        }
        selector = sel
        val dnsScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        val context = NioTransportContext(sel, ::wakeup, dnsScope, readBufferSize)
        val t = Thread({ loop(driver, sel, context, dnsScope) }, name)
        t.isDaemon = true
        thread = t
        try {
            t.start()
        } catch (e: Throwable) {
            state.set(CLOSED)
            thread = null
            dnsScope.cancel()
            sel.close()
            throw e
        }
    }

    override fun wakeup() {
        try {
            selector?.wakeup()
        } catch (_: Exception) {
            // Закрыт параллельно: будить нечего
        }
    }

    override suspend fun close(timeoutMs: Long): Boolean {
        if (state.compareAndSet(NEW, CLOSED)) return true
        state.set(CLOSED)
        stopRequested = true
        wakeup()
        val t = thread ?: return true
        // С потока хоста (из драйвера) ждать самого себя нельзя: цикл выйдет после текущего runOnce
        if (t === Thread.currentThread()) return false
        if (!t.isAlive) return true
        runInterruptible(Dispatchers.IO) { t.join(timeoutMs.coerceAtLeast(1)) }
        return !t.isAlive
    }

    @OptIn(MqttCoreApi::class)
    private fun loop(driver: HostDriver, sel: Selector, context: NioTransportContext, dnsScope: CoroutineScope) {
        var cause: Throwable? = null
        var transport: MqttTransport? = null
        val flushing = ArrayList<Flushing>()
        try {
            var pending: MutableList<TransportEvent> = ArrayList()
            while (!stopRequested) {
                val now = clock.nowMs()
                serviceFlushing(flushing, now)
                transport?.let {
                    it.poll(now)
                    pending.addAll(it.events())
                }
                val events: List<TransportEvent> = pending
                pending = ArrayList()
                val step = driver.runOnce(now, events)
                for (action in step.actions) {
                    if (stopRequested) break
                    when (action) {
                        is HostAction.Connect -> {
                            transport?.close()
                            transport = null
                            transport = openTransport(context, action, now, pending)
                        }
                        is HostAction.Write -> transport?.write(action.bytes)
                        is HostAction.PauseRead -> transport?.setReadInterest(!action.paused)
                        HostAction.CloseTransport -> {
                            transport?.close()
                            transport = null
                        }
                        is HostAction.FlushAndClose -> {
                            transport?.let { startFlush(flushing, it, now + action.timeoutMs) }
                            transport = null
                        }
                    }
                }
                if (stopRequested) break
                transport?.let { pending.addAll(it.events()) }
                val deadline = minDeadline(
                    minDeadline(step.nextDeadlineMs, transport?.nextDeadlineMs()),
                    flushDeadline(flushing)
                )
                select(sel, deadline, now = pending.isNotEmpty())
            }
            // Остановка: текущий транспорт — сразу, отданные на дописывание — до опустевшей очереди или срока
            transport?.close()
            transport = null
            while (flushing.isNotEmpty()) {
                serviceFlushing(flushing, clock.nowMs())
                if (flushing.isEmpty()) break
                select(sel, flushDeadline(flushing), now = false)
            }
        } catch (t: Throwable) {
            cause = t
        } finally {
            try {
                transport?.close()
            } catch (_: Throwable) {
            }
            for (f in flushing) {
                try {
                    f.transport.close()
                } catch (_: Throwable) {
                }
            }
            flushing.clear()
            try {
                sel.close()
            } catch (_: Throwable) {
            }
            selector = null
            dnsScope.cancel()
            try {
                driver.onStopped(cause)
            } catch (_: Throwable) {
            }
        }
    }

    /**
     * Новый транспорт из фабрики и начало подключения. Исключение фабрики или `connect`
     * (например `IOException` «Too many open files» — кончились дескрипторы) — не остановка хоста, а ошибка этого
     * подключения: [TransportEvent.Failed] с [MqttError.Io] в [pending] (драйвер получит его следующим оборотом,
     * клиент уйдёт в `Waiting` и повторит по политике), транспорта нет. `Error` (`OutOfMemoryError` и т. п.) не
     * перехватывается: хост останавливается, как раньше.
     */
    @OptIn(MqttCoreApi::class)
    private fun openTransport(
        context: NioTransportContext,
        action: HostAction.Connect,
        now: Long,
        pending: MutableList<TransportEvent>
    ): MqttTransport? {
        var created: MqttTransport? = null
        return try {
            created = transportFactory.create(context)
            created.connect(action.host, action.port, now + action.timeoutMs)
            created
        } catch (e: Exception) {
            try {
                created?.close()
            } catch (_: Exception) {
            }
            pending += TransportEvent.Failed(NioTransport.errorOf(e))
            null
        }
    }

    /** Транспорт, отданный на [HostAction.FlushAndClose]: не текущий, закрывается по пустой очереди или [deadlineMs]. */
    @OptIn(MqttCoreApi::class)
    private class Flushing(val transport: MqttTransport, val deadlineMs: Long)

    /** Отдать транспорт на дописывание; пустая очередь — закрыть сразу. Чтение включается (входящее отбрасывается). */
    @OptIn(MqttCoreApi::class)
    private fun startFlush(flushing: MutableList<Flushing>, transport: MqttTransport, deadlineMs: Long) {
        transport.closeOutput() // у TLS — close_notify в очередь за данными
        transport.events() // события транспорта драйверу больше не доставляются
        if (transport.pendingWriteBytes <= 0L) {
            transport.close()
            return
        }
        // Закрытие с непрочитанными входящими даёт RST: пока дописываем — читаем и отбрасываем
        transport.setReadInterest(true)
        flushing += Flushing(transport, deadlineMs)
    }

    /** Продвинуть дописывание: события отбрасываются; очередь пуста, транспорт упал или срок истёк — закрыть. */
    @OptIn(MqttCoreApi::class)
    private fun serviceFlushing(flushing: MutableList<Flushing>, now: Long) {
        if (flushing.isEmpty()) return
        val iterator = flushing.iterator()
        while (iterator.hasNext()) {
            val f = iterator.next()
            f.transport.poll(now)
            f.transport.events() // в т. ч. Failed: транспорт уже закрыт, pendingWriteBytes = 0
            if (f.transport.pendingWriteBytes <= 0L || now >= f.deadlineMs) {
                f.transport.close()
                iterator.remove()
            }
        }
    }

    private fun flushDeadline(flushing: List<Flushing>): Long? = flushing.minOfOrNull { it.deadlineMs }

    /** `select` до [deadline] (`null` — без срока), [now] — без ожидания; готовые ключи — их транспортам. */
    private fun select(sel: Selector, deadline: Long?, now: Boolean) {
        val selected = when {
            now -> sel.selectNow()
            deadline == null -> sel.select()
            else -> {
                val timeout = deadline - clock.nowMs()
                if (timeout <= 0) sel.selectNow() else sel.select(timeout)
            }
        }
        if (selected > 0) {
            val keys = sel.selectedKeys()
            for (key in keys) {
                (key.attachment() as? NioTransport)?.onSelected(key)
            }
            keys.clear()
        }
    }

    public companion object {
        /** Префикс имени потока хоста: `kmqtt-io-<n>`. */
        public const val THREAD_NAME_PREFIX: String = "kmqtt-io-"

        /** Буфер чтения сокета: 16 КиБ. */
        public const val DEFAULT_READ_BUFFER: Int = 16 * 1024

        private const val NEW = 0
        private const val STARTED = 1
        private const val CLOSED = 2

        private fun minDeadline(a: Long?, b: Long?): Long? = when {
            a == null -> b
            b == null -> a
            else -> minOf(a, b)
        }
    }
}
