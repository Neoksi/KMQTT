package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import kotlinx.atomicfu.atomic
import kotlinx.cinterop.alloc
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.withTimeoutOrNull
import platform.posix.fd_set
import platform.posix.timeval
import kotlin.native.concurrent.ObsoleteWorkersApi
import kotlin.native.concurrent.Worker

/** Глобальный (на процесс) счётчик имён потоков хоста (H6). */
private val hostThreadCounter = atomic(0L)

/** Следующее имя потока хоста: `kmqtt-io-<n>`, `n` растёт в процессе. */
internal fun nextHostThreadName(): String = WinSockHost.THREAD_NAME_PREFIX + hostThreadCounter.incrementAndGet()

/**
 * Хост клиента для mingwX64 (Windows, SEVEN-250; перенос JVM-хоста `NioActorHost`, W6 §2.1 «поток-актор», §4.6): один
 * поток-воркер Kotlin/Native `kmqtt-io-<n>` с `select` WinSock.
 *
 * Цикл потока — тот же, что у JVM-хоста: события транспорта → [HostDriver.runOnce] → действия по порядку (`Connect` —
 * новый транспорт из фабрики, прежний закрывается; исключение фабрики или `connect` — [TransportEvent.Failed] следующим
 * оборотом, хост живёт; `Write`, `PauseRead`, `CloseTransport`; `FlushAndClose` — транспорт дописывается, его события
 * отбрасываются, чтение включено, закрытие по пустой очереди или сроку) → `select` до ближайшего дедлайна (драйвера,
 * подключения, дописывания). [wakeup] прерывает `select` сразу: байт в UDP-сокет на 127.0.0.1, соединённый сам с собой
 * ([WakeSocket]); пробуждение до сна не теряется.
 *
 * Резолв имени — на `Dispatchers.IO` (W6 §4.6), остальное — на потоке хоста. [close]: текущий транспорт закрывается
 * сразу (идущее подключение прерывается, H5), дописывающиеся — не дольше своих сроков, затем поток выходит, закрыв
 * сокет пробуждения; приостановка — до выхода цикла, не дольше таймаута. Вызов [close] с потока хоста (из драйвера) не
 * ждёт (вернёт `false`). Одноразовый: после [close] не перезапускается.
 *
 * TLS и WebSocket на mingwX64 нет (SEVEN-250, план подкарточек): фабрика `MqttClient(options)` такие опции отклоняет.
 */
@OptIn(MqttCoreApi::class, ObsoleteWorkersApi::class)
public class WinSockHost internal constructor(
    /** Имя потока хоста. */
    public val name: String,
    private val clock: MqttClock,
    private val transportFactory: WinSockTransportFactory,
    private val readBufferSize: Int
) : MqttHost {

    /** Хост с TCP-транспортом. [clock] должен совпадать с часами драйвера. */
    public constructor(
        name: String = nextHostThreadName(),
        clock: MqttClock = MqttClock.Monotonic
    ) : this(name, clock, WinSockTransportFactory.Tcp, DEFAULT_READ_BUFFER)

    private val state = atomic(NEW)
    private val stopRequested = atomic(false)
    private val wake = atomic<WakeSocket?>(null)
    private val worker = atomic<Worker?>(null)
    private val done = CompletableDeferred<Unit>()

    @MqttCoreApi
    override fun start(driver: HostDriver) {
        check(state.compareAndSet(NEW, STARTED)) { "WinSockHost: start() once, not after close()" }
        val wakeSocket = try {
            WakeSocket()
        } catch (e: Throwable) {
            state.value = CLOSED
            done.complete(Unit)
            throw e
        }
        wake.value = wakeSocket
        val dnsScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        val context = WinSockTransportContext(wakeSocket::signal, dnsScope, readBufferSize)
        try {
            val w = Worker.start(name = name)
            worker.value = w
            w.executeAfter(0L) { loop(driver, wakeSocket, context, dnsScope) }
        } catch (e: Throwable) {
            state.value = CLOSED
            dnsScope.cancel()
            wake.value = null
            wakeSocket.close()
            done.complete(Unit)
            throw e
        }
    }

    override fun wakeup() {
        wake.value?.signal()
    }

    override suspend fun close(timeoutMs: Long): Boolean {
        if (state.compareAndSet(NEW, CLOSED)) {
            done.complete(Unit)
            return true
        }
        state.value = CLOSED
        stopRequested.value = true
        wakeup()
        if (done.isCompleted) return true
        // С потока хоста (из драйвера) ждать самого себя нельзя: цикл выйдет после текущего runOnce
        val w = worker.value
        if (w != null && Worker.current == w) return false
        return withTimeoutOrNull(timeoutMs.coerceAtLeast(1)) { done.await() } != null
    }

    private fun loop(driver: HostDriver, wakeSocket: WakeSocket, context: WinSockTransportContext, dnsScope: CoroutineScope) {
        var cause: Throwable? = null
        var transport: MqttTransport? = null
        val flushing = ArrayList<Flushing>()
        try {
            var pending: MutableList<TransportEvent> = ArrayList()
            while (!stopRequested.value) {
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
                    if (stopRequested.value) break
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
                if (stopRequested.value) break
                transport?.let { pending.addAll(it.events()) }
                val deadline = minDeadline(
                    minDeadline(step.nextDeadlineMs, transport?.nextDeadlineMs()),
                    flushDeadline(flushing)
                )
                select(wakeSocket, context, deadline, now = pending.isNotEmpty())
            }
            // Остановка: текущий транспорт — сразу (H5), отданные на дописывание — до опустевшей очереди или срока
            transport?.close()
            transport = null
            while (flushing.isNotEmpty()) {
                serviceFlushing(flushing, clock.nowMs())
                if (flushing.isEmpty()) break
                select(wakeSocket, context, flushDeadline(flushing), now = false)
            }
        } catch (t: Throwable) {
            cause = t
        } finally {
            runCatching { transport?.close() }
            for (f in flushing) runCatching { f.transport.close() }
            flushing.clear()
            for (t in context.active.toList()) runCatching { t.close() }
            wake.value = null
            wakeSocket.close()
            dnsScope.cancel()
            try {
                driver.onStopped(cause)
            } catch (_: Throwable) {
            }
            worker.value?.requestTermination(processScheduledJobs = false)
            done.complete(Unit)
        }
    }

    private fun openTransport(
        context: WinSockTransportContext,
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
            runCatching { created?.close() }
            pending += TransportEvent.Failed(MqttError.Io(e::class.simpleName ?: "Exception", e))
            null
        }
    }

    private class Flushing(val transport: MqttTransport, val deadlineMs: Long)

    private fun startFlush(flushing: MutableList<Flushing>, transport: MqttTransport, deadlineMs: Long) {
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

    private fun serviceFlushing(flushing: MutableList<Flushing>, now: Long) {
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

    private fun flushDeadline(flushing: List<Flushing>): Long? = flushing.minOfOrNull { it.deadlineMs }

    /** `select` до [deadline] (`null` — без срока), [now] — без ожидания; готовность — транспортам из реестра. */
    private fun select(wakeSocket: WakeSocket, context: WinSockTransportContext, deadline: Long?, now: Boolean) {
        memScoped {
            val readSet = alloc<fd_set>()
            val writeSet = alloc<fd_set>()
            val exceptSet = alloc<fd_set>()
            WinSock.fdClear(readSet)
            WinSock.fdClear(writeSet)
            WinSock.fdClear(exceptSet)
            val wakeFd = wakeSocket.socket
            WinSock.fdAdd(readSet, wakeFd)
            val polled = context.active.toList()
            for (t in polled) {
                val fd = t.fd
                if (fd == WinSock.INVALID) continue
                if (t.wantsRead) WinSock.fdAdd(readSet, fd)
                if (t.wantsWrite) WinSock.fdAdd(writeSet, fd)
                if (t.wantsExcept) WinSock.fdAdd(exceptSet, fd)
            }
            val timeoutMs: Long? = when {
                now -> 0L
                deadline == null -> null
                else -> (deadline - clock.nowMs()).coerceIn(0L, MAX_SELECT_MS)
            }
            val tv = alloc<timeval>()
            if (timeoutMs != null) {
                tv.tv_sec = (timeoutMs / 1000).toInt()
                tv.tv_usec = ((timeoutMs % 1000) * 1000).toInt()
            }
            val rc = platform.windows.select(
                0,
                readSet.ptr,
                writeSet.ptr,
                exceptSet.ptr,
                if (timeoutMs == null) null else tv.ptr
            )
            if (rc == WinSock.SOCKET_ERROR) {
                error("select failed: ${WinSock.errorName(WinSock.lastError())}")
            }
            if (WinSock.fdHas(readSet, wakeFd)) wakeSocket.drain()
            if (rc > 0) {
                for (t in polled) {
                    val fd = t.fd
                    if (fd == WinSock.INVALID) continue
                    val readable = WinSock.fdHas(readSet, fd)
                    val writable = WinSock.fdHas(writeSet, fd)
                    val failed = WinSock.fdHas(exceptSet, fd)
                    if (readable || writable || failed) t.onReady(readable, writable, failed)
                }
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

        /** Предел одного ожидания `select` (дальний дедлайн — следующим оборотом). */
        private const val MAX_SELECT_MS = 24L * 60 * 60 * 1000

        private fun minDeadline(a: Long?, b: Long?): Long? = when {
            a == null -> b
            b == null -> a
            else -> minOf(a, b)
        }
    }
}
