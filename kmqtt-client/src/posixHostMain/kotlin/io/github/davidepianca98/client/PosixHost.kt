// size_t/ssize_t/nfds_t разной ширины на 64- и 32-битных целях (watchOS): значения приводятся convert()/toInt()
@file:OptIn(kotlinx.cinterop.UnsafeNumber::class)

package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import kotlinx.atomicfu.atomic
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import platform.posix.EINTR
import platform.posix.POLLERR
import platform.posix.POLLHUP
import platform.posix.POLLIN
import platform.posix.POLLNVAL
import platform.posix.POLLOUT
import platform.posix.pollfd
import kotlin.native.concurrent.ObsoleteWorkersApi
import kotlin.native.concurrent.Worker

/** Глобальный (на процесс) счётчик имён потоков хоста. */
private val hostThreadCounter = atomic(0L)

/** Следующее имя потока хоста: `kmqtt-io-<n>`, `n` растёт в процессе. */
internal fun nextHostThreadName(): String = PosixHost.THREAD_NAME_PREFIX + hostThreadCounter.incrementAndGet()

/**
 * Хост клиента для Linux (linuxX64, linuxArm64) и Apple (macOS, iOS, tvOS, watchOS) — перенос `WinSockHost` mingwX64,
 * тот — перенос JVM-хоста `NioActorHost` («поток-актор»): один поток-воркер Kotlin/Native `kmqtt-io-<n>` с `poll` POSIX.
 *
 * Цикл потока — тот же, что у JVM- и WinSock-хоста: события транспорта → [HostDriver.runOnce] → действия по порядку
 * (`Connect` — новый транспорт из фабрики, прежний закрывается; исключение фабрики или `connect` — [TransportEvent.Failed]
 * следующим оборотом, хост живёт; `Write`, `PauseRead`, `CloseTransport`; `FlushAndClose` — транспорт дописывается, его
 * события отбрасываются, чтение включено, закрытие по пустой очереди или сроку) → `poll` до ближайшего дедлайна
 * (драйвера, подключения, дописывания). [wakeup] прерывает `poll` сразу: байт в неблокирующий `pipe` ([WakePipe]);
 * пробуждение до сна не теряется. `poll`, а не `select`: нет предела `FD_SETSIZE` на номер дескриптора. Сокет без
 * интереса к чтению и записи в `poll` не ставится (иначе `POLLHUP`/`POLLERR`, которые `poll` сообщает всегда, крутили бы
 * цикл вхолостую).
 *
 * Резолв имени — на `Dispatchers.IO`, остальное — на потоке хоста. [close]: текущий транспорт закрывается
 * сразу (идущее подключение прерывается), дописывающиеся — не дольше своих сроков, затем цикл выходит, закрыв
 * канал пробуждения, а [close] завершает воркер и дожидается конца его потока; приостановка — не дольше таймаута.
 * Хост, остановленный исключением драйвера, держит простаивающий воркер до [close] (фасад всегда его вызывает). Вызов
 * [close] с потока хоста (из драйвера) не ждёт (вернёт `false`). Одноразовый: после [close] не перезапускается.
 *
 * TLS — транспорт `NativeTlsTransport` поверх [PosixTransport] (фабрика `PosixTransportFactory.tls`, OpenSSL с
 * memory-BIO). WebSocket на Linux и Apple нет: фабрика `MqttClient(options)` такие опции отклоняет. Код использует
 * только POSIX; различие платформ — защита от `SIGPIPE` (см. `PosixSockets`). Запуск проверяется тестами
 * `linuxX64Test` (Linux), `macosArm64Test`/`macosX64Test` и тестами симуляторов iOS/tvOS/watchOS (macOS с Xcode);
 * цели устройств Apple только компилируются.
 */
@OptIn(MqttCoreApi::class, ObsoleteWorkersApi::class)
public class PosixHost internal constructor(
    /** Имя потока хоста. */
    public val name: String,
    private val clock: MqttClock,
    private val transportFactory: PosixTransportFactory,
    private val readBufferSize: Int
) : MqttHost {

    /** Хост с TCP-транспортом. [clock] должен совпадать с часами драйвера. */
    public constructor(
        name: String = nextHostThreadName(),
        clock: MqttClock = MqttClock.Monotonic
    ) : this(name, clock, PosixTransportFactory.Tcp, DEFAULT_READ_BUFFER)

    private val state = atomic(NEW)
    private val stopRequested = atomic(false)
    private val wake = atomic<WakePipe?>(null)
    private val worker = atomic<Worker?>(null)
    private val done = CompletableDeferred<Unit>()

    @MqttCoreApi
    override fun start(driver: HostDriver) {
        check(state.compareAndSet(NEW, STARTED)) { "PosixHost: start() once, not after close()" }
        val wakePipe = try {
            WakePipe()
        } catch (e: Throwable) {
            state.value = CLOSED
            done.complete(Unit)
            throw e
        }
        wake.value = wakePipe
        val dnsScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        val context = PosixTransportContext(wakePipe::signal, dnsScope, readBufferSize)
        try {
            val w = Worker.start(name = name)
            worker.value = w
            w.executeAfter(0L) { loop(driver, wakePipe, context, dnsScope) }
        } catch (e: Throwable) {
            state.value = CLOSED
            dnsScope.cancel()
            wake.value = null
            wakePipe.close()
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
        // С потока хоста (из драйвера) ждать самого себя нельзя: цикл выйдет после текущего runOnce
        val w = worker.value
        if (w != null && Worker.current == w) return false
        val stopped = withTimeoutOrNull(timeoutMs.coerceAtLeast(1)) { done.await() } != null
        if (stopped) terminateWorker()
        return stopped
    }

    /** Завершить воркер, чей цикл уже вышел, и дождаться конца потока (освобождает его ресурсы). Один раз. */
    private suspend fun terminateWorker() {
        val w = worker.getAndSet(null) ?: return
        val future = try {
            w.requestTermination(processScheduledJobs = false)
        } catch (_: IllegalStateException) {
            return // уже завершён
        }
        withContext(Dispatchers.IO) { future.result }
    }

    private fun loop(driver: HostDriver, wakePipe: WakePipe, context: PosixTransportContext, dnsScope: CoroutineScope) {
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
                waitReady(wakePipe, context, deadline, now = pending.isNotEmpty())
            }
            // Остановка: текущий транспорт — сразу, отданные на дописывание — до опустевшей очереди или срока
            transport?.close()
            transport = null
            while (flushing.isNotEmpty()) {
                serviceFlushing(flushing, clock.nowMs())
                if (flushing.isEmpty()) break
                waitReady(wakePipe, context, flushDeadline(flushing), now = false)
            }
        } catch (t: Throwable) {
            cause = t
        } finally {
            runCatching { transport?.close() }
            for (f in flushing) runCatching { f.transport.close() }
            flushing.clear()
            for (t in context.active.toList()) runCatching { t.close() }
            wake.value = null
            wakePipe.close()
            dnsScope.cancel()
            try {
                driver.onStopped(cause)
            } catch (_: Throwable) {
            }
            // Воркер завершает close() снаружи: самозавершение изнутри задачи оставляет ресурсы потока
            done.complete(Unit)
        }
    }

    private fun openTransport(
        context: PosixTransportContext,
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
        // Закрытие с непрочитанными входящими даёт RST: пока дописываем — читаем и отбрасываем
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

    /** `poll` до [deadline] (`null` — без срока), [now] — без ожидания; готовность — транспортам из реестра. */
    private fun waitReady(wakePipe: WakePipe, context: PosixTransportContext, deadline: Long?, now: Boolean) {
        val polled = ArrayList<PosixTransport>(context.active.size)
        for (t in context.active) {
            if (t.fd != PosixSockets.INVALID && (t.wantsRead || t.wantsWrite)) polled += t
        }
        memScoped {
            val fds = allocArray<pollfd>(polled.size + 1)
            fds[0].fd = wakePipe.fd
            fds[0].events = POLLIN.convert()
            fds[0].revents = 0
            for ((i, t) in polled.withIndex()) {
                val slot = fds[i + 1]
                slot.fd = t.fd
                var interest = 0
                if (t.wantsRead) interest = interest or POLLIN
                if (t.wantsWrite) interest = interest or POLLOUT
                slot.events = interest.convert()
                slot.revents = 0
            }
            val timeoutMs: Int = when {
                now -> 0
                deadline == null -> -1
                else -> (deadline - clock.nowMs()).coerceIn(0L, MAX_POLL_MS).toInt()
            }
            val rc = platform.posix.poll(fds, (polled.size + 1).convert(), timeoutMs)
            if (rc < 0) {
                val code = PosixSockets.lastError()
                // EINTR (сигнал) и потерянный код — просто следующий оборот
                if (code == EINTR || code == 0) return@memScoped
                error("poll failed: ${PosixSockets.errorName(code)}")
            }
            if (fds[0].revents.toInt() and (POLLIN or POLLERR or POLLHUP) != 0) wakePipe.drain()
            if (rc > 0) {
                for ((i, t) in polled.withIndex()) {
                    val revents = fds[i + 1].revents.toInt()
                    if (revents == 0 || t.fd == PosixSockets.INVALID) continue
                    val readable = revents and POLLIN != 0
                    val writable = revents and POLLOUT != 0
                    val failed = revents and (POLLERR or POLLNVAL) != 0
                    val hangup = revents and POLLHUP != 0
                    t.onReady(readable, writable, failed, hangup)
                }
            }
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

        /** Предел одного ожидания `poll` (дальний дедлайн — следующим оборотом). */
        private const val MAX_POLL_MS = 24L * 60 * 60 * 1000

        private fun minDeadline(a: Long?, b: Long?): Long? = when {
            a == null -> b
            b == null -> a
            else -> minOf(a, b)
        }
    }
}
