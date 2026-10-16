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

/** Глобальный (на процесс) счётчик имён потоков хоста (H6). */
private val hostThreadCounter = AtomicLong(0)

/** Следующее имя потока хоста: `kmqtt-io-<n>`, `n` растёт в процессе. */
internal fun nextHostThreadName(): String = NioActorHost.THREAD_NAME_PREFIX + hostThreadCounter.incrementAndGet()

/**
 * JVM-хост клиента (W6 §2.1 «поток-актор», S06b-T04a): один daemon-поток `kmqtt-io-<n>` (H6) с [Selector].
 *
 * Цикл потока:
 * 1. события транспорта (результат резолва, дедлайн подключения, готовность сокета) → [HostDriver.runOnce];
 * 2. действия шага ([HostAction]) исполняются по порядку: `Connect` — новый транспорт из фабрики (прежний
 *    закрывается), неблокирующий connect с дедлайном; `Write` — запись без блокировки, остаток по `OP_WRITE`;
 *    `PauseRead` — снять/вернуть `OP_READ` текущего транспорта; `CloseTransport` — закрыть канал;
 * 3. `select` до ближайшего дедлайна (драйвера или подключения); [wakeup] прерывает его сразу (H4). Если действия
 *    сразу дали события — `selectNow` и следующий оборот без сна.
 *
 * Резолв имени — на `Dispatchers.IO` (W6 §4.6), остальное — на потоке хоста. [close] прерывает и идущее
 * подключение (H5): поток выходит, закрывая транспорт и селектор; приостановка до выхода потока — не дольше таймаута.
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
        try {
            var pending: MutableList<TransportEvent> = ArrayList()
            while (!stopRequested) {
                val now = clock.nowMs()
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
                            val created = transportFactory.create(context)
                            transport = created
                            created.connect(action.host, action.port, now + action.timeoutMs)
                        }
                        is HostAction.Write -> transport?.write(action.bytes)
                        is HostAction.PauseRead -> transport?.setReadInterest(!action.paused)
                        HostAction.CloseTransport -> {
                            transport?.close()
                            transport = null
                        }
                    }
                }
                if (stopRequested) break
                transport?.let { pending.addAll(it.events()) }
                val deadline = minDeadline(step.nextDeadlineMs, transport?.nextDeadlineMs())
                val selected = when {
                    pending.isNotEmpty() -> sel.selectNow()
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
        } catch (t: Throwable) {
            cause = t
        } finally {
            try {
                transport?.close()
            } catch (_: Throwable) {
            }
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

    public companion object {
        /** Префикс имени потока хоста: `kmqtt-io-<n>`. */
        public const val THREAD_NAME_PREFIX: String = "kmqtt-io-"

        /** Буфер чтения сокета (K7): 16 КиБ. */
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
