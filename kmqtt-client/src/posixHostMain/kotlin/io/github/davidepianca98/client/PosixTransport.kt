package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch

/**
 * Окружение транспорта внутри [PosixHost]: пробуждение, область для резолва имён на `Dispatchers.IO`, размер буфера
 * чтения и реестр сокетов, которые хост ставит в `poll`. Создаёт только хост; транспорту передаётся через
 * [PosixTransportFactory].
 */
@MqttCoreApi
public class PosixTransportContext internal constructor(
    internal val wakeup: () -> Unit,
    internal val dnsScope: CoroutineScope,
    internal val readBufferSize: Int
) {
    /** Живые транспорты (только поток хоста): хост опрашивает их готовность, даже если они обёрнуты (TLS и т. п.). */
    internal val active: MutableList<PosixTransport> = ArrayList()
}

/** Фабрика транспорта для [PosixHost]: новый транспорт на каждое [HostAction.Connect]. */
@MqttCoreApi
public fun interface PosixTransportFactory {
    public fun create(context: PosixTransportContext): MqttTransport

    public companion object {
        /** Обычный TCP. */
        public val Tcp: PosixTransportFactory = PosixTransportFactory { PosixTransport(it) }
    }
}

/**
 * TCP-транспорт Linux и Apple на неблокирующем сокете POSIX под `poll` потока хоста (перенос `WinSockTransport`, тот —
 * перенос `NioTransport`).
 *
 * - Резолв имени (`getaddrinfo`, блокирующий) — отдельной задачей на `Dispatchers.IO`; результат забирает [poll] на
 *   потоке хоста. Берётся первый IPv4, иначе первый адрес. Срок — общий дедлайн [connect] (резолв + TCP): по нему
 *   [MqttError.ConnectTimeout], поздний ответ резолвера отбрасывается (`getaddrinfo` не прерывается: поток `IO` занят до
 *   ответа ОС, как на JVM).
 * - Подключение неблокирующее (`EINPROGRESS`): итог — готовность к записи (`POLLOUT`, при отказе ещё `POLLERR`/
 *   `POLLHUP`) и `SO_ERROR`: 0 — подключено, иначе отказ с этим кодом. Отказ loopback может прийти и сразу из `connect`.
 *   Закрытие во время подключения мгновенное.
 * - Чтение: буфер [PosixTransportContext.readBufferSize] (16 КиБ), до [READS_PER_READY] чтений за готовность, каждое —
 *   [TransportEvent.Bytes] с копией; `recv == 0` — [TransportEvent.Eof]. Запись: очередь массивов вызывающего (без
 *   копирования), новые байты не обгоняют накопленные (как `PendingSendQueue` в `kmqtt-common`): `send`
 *   только из головы очереди, порциями не больше [MAX_SEND_CHUNK]; неполная запись оставляет остаток до готовности.
 * - Ошибки: `getaddrinfo` `EAI_NONAME`/`EAI_AGAIN`/`EAI_NODATA` → [MqttError.UnknownHost]; прочие → [MqttError.Io] с
 *   именем кода `errno` (`ECONNREFUSED`, `ECONNRESET`, `EPIPE`…) в `kind`, без адреса. Код 0 после неудачного вызова
 *   (затёртый `errno`) — «повторить позже», как `isTransientSocketError`.
 * - `TCP_NODELAY` включён, запись без `SIGPIPE` (`MSG_NOSIGNAL` на Linux, `SO_NOSIGPIPE` на Apple). [close]: `shutdown(SHUT_WR)` (FIN за отданными
 *   ОС данными) и `close`; если в буфере приёма остались непрочитанные данные, ОС ответит RST — поэтому хост при
 *   `FlushAndClose` читает и отбрасывает входящее до закрытия.
 *
 * Все методы — только на потоке хоста.
 */
@MqttCoreApi
public class PosixTransport internal constructor(
    private val context: PosixTransportContext,
    private val resolver: (String, Int) -> PosixSockets.Resolved
) : MqttTransport {

    public constructor(context: PosixTransportContext) : this(context, PosixSockets::resolve)

    private enum class Phase { Idle, Resolving, Connecting, Connected, Closed }

    private var phase = Phase.Idle
    private var socket: Int = PosixSockets.INVALID
    private var deadlineMs: Long? = null
    private var dnsJob: Job? = null
    private val dnsResult = atomic<Any?>(null)
    private val events = ArrayList<TransportEvent>()
    private val writeQueue = ArrayDeque<ByteArray>()
    private var writeOffset = 0
    private var readBuffer: ByteArray? = null
    private var readInterest = true
    private var eof = false

    override var pendingWriteBytes: Long = 0
        private set

    /** Сокет для `poll` или [PosixSockets.INVALID]. */
    internal val fd: Int get() = socket

    internal val wantsRead: Boolean get() = phase == Phase.Connected && readInterest && !eof

    internal val wantsWrite: Boolean
        get() = phase == Phase.Connecting || (phase == Phase.Connected && pendingWriteBytes > 0)

    override fun connect(host: String, port: Int, deadlineMs: Long) {
        check(phase == Phase.Idle) { "PosixTransport: connect() once per transport" }
        this.deadlineMs = deadlineMs
        phase = Phase.Resolving
        dnsJob = context.dnsScope.launch {
            val result: Any = try {
                resolver(host, port)
            } catch (e: Throwable) {
                e
            }
            dnsResult.value = result
            context.wakeup()
        }
    }

    override fun write(bytes: ByteArray): Int {
        if (phase == Phase.Closed || bytes.isEmpty()) return 0
        writeQueue.addLast(bytes)
        pendingWriteBytes += bytes.size
        if (phase != Phase.Connected) return 0
        val before = pendingWriteBytes
        flush()
        return (before - pendingWriteBytes).toInt()
    }

    override fun setReadInterest(enabled: Boolean) {
        readInterest = enabled
    }

    override fun poll(nowMs: Long) {
        if (phase == Phase.Resolving) {
            val result = dnsResult.getAndSet(null)
            if (result != null) {
                dnsJob = null
                when (result) {
                    is PosixSockets.Resolved.Found -> open(result.address)
                    is PosixSockets.Resolved.Error -> fail(resolveError(result.code))
                    is Throwable -> fail(MqttError.Io(result::class.simpleName ?: "Throwable", result))
                }
            }
        }
        val deadline = deadlineMs
        if ((phase == Phase.Resolving || phase == Phase.Connecting) && deadline != null && nowMs >= deadline) {
            fail(MqttError.ConnectTimeout)
        }
    }

    override fun nextDeadlineMs(): Long? =
        if (phase == Phase.Resolving || phase == Phase.Connecting) deadlineMs else null

    override fun events(): List<TransportEvent> {
        if (events.isEmpty()) return emptyList()
        val out = ArrayList(events)
        events.clear()
        return out
    }

    override fun close() {
        closeSocket(orderly = true)
    }

    /**
     * Готовность сокета по итогам `poll`; вызывает хост. [failed] — `POLLERR`, [hangup] — `POLLHUP` (их `poll`
     * сообщает всегда, поэтому хост ставит сокет в `poll` только при [wantsRead] или [wantsWrite]).
     */
    internal fun onReady(readable: Boolean, writable: Boolean, failed: Boolean, hangup: Boolean) {
        when (phase) {
            Phase.Connecting -> {
                if (writable || failed || hangup) {
                    // Итог connect — по SO_ERROR: код — отказ; 0 и готовность к записи — подключено; 0 без
                    // готовности к записи — ждём дальше, срок подключения всё равно ограничивает ожидание
                    val error = PosixSockets.pendingError(socket)
                    when {
                        error != 0 && error != PosixSockets.UNKNOWN_ERROR -> {
                            fail(PosixSockets.ioError(error))
                            return
                        }
                        writable && error == 0 -> onConnected()
                    }
                }
            }
            Phase.Connected -> {
                if (failed) {
                    // POLLERR: ошибка сокета в SO_ERROR (RST и т. п.); без кода — её покажут send/recv ниже
                    val error = PosixSockets.pendingError(socket)
                    if (error != 0 && error != PosixSockets.UNKNOWN_ERROR) {
                        fail(PosixSockets.ioError(error))
                        return
                    }
                }
                if (writable || ((failed || hangup) && pendingWriteBytes > 0)) flush()
                if ((readable || failed || hangup) && phase == Phase.Connected) readSome()
            }
            else -> Unit
        }
    }

    private fun open(address: PosixSockets.Address) {
        phase = Phase.Connecting
        val (s, openError) = PosixSockets.openStream(address.family)
        if (s == PosixSockets.INVALID) {
            fail(PosixSockets.ioError(openError))
            return
        }
        socket = s
        context.active += this
        PosixSockets.noDelay(s)
        val error = PosixSockets.connectTo(s, address)
        if (error == 0) {
            onConnected()
            return
        }
        if (!PosixSockets.isConnectPending(error)) fail(PosixSockets.ioError(error))
    }

    private fun onConnected() {
        phase = Phase.Connected
        deadlineMs = null
        events += TransportEvent.Connected
        flush()
    }

    private fun flush() {
        val s = socket
        while (writeQueue.isNotEmpty() && phase == Phase.Connected) {
            val bytes = writeQueue.first()
            val n = PosixSockets.sendSome(s, bytes, writeOffset, minOf(bytes.size - writeOffset, MAX_SEND_CHUNK))
            if (n < 0) {
                if (!PosixSockets.isRetry(-n)) fail(PosixSockets.ioError(-n))
                return
            }
            writeOffset += n
            pendingWriteBytes -= n
            if (writeOffset < bytes.size) return
            writeQueue.removeFirst()
            writeOffset = 0
        }
    }

    private fun readSome() {
        val buffer = readBuffer ?: ByteArray(context.readBufferSize).also { readBuffer = it }
        repeat(READS_PER_READY) {
            if (phase != Phase.Connected || !readInterest || eof) return
            val n = PosixSockets.recvSome(socket, buffer)
            when {
                n > 0 -> events += TransportEvent.Bytes(buffer.copyOf(n))
                n == 0 -> {
                    eof = true
                    events += TransportEvent.Eof
                    return
                }
                else -> {
                    if (!PosixSockets.isRetry(-n)) fail(PosixSockets.ioError(-n))
                    return
                }
            }
        }
    }

    private fun fail(error: MqttError) {
        if (phase == Phase.Closed) return
        events += TransportEvent.Failed(error)
        closeSocket(orderly = false)
    }

    private fun closeSocket(orderly: Boolean) {
        if (phase == Phase.Closed) return
        val wasConnected = phase == Phase.Connected
        phase = Phase.Closed
        dnsJob?.cancel()
        dnsJob = null
        context.active -= this
        val s = socket
        socket = PosixSockets.INVALID
        PosixSockets.close(s, orderly && wasConnected)
        writeQueue.clear()
        writeOffset = 0
        pendingWriteBytes = 0
        readBuffer = null
    }

    private fun resolveError(code: Int): MqttError =
        if (PosixSockets.isUnknownHost(code)) {
            MqttError.UnknownHost
        } else {
            MqttError.Io("getaddrinfo-$code", IllegalStateException("getaddrinfo error $code"))
        }

    internal companion object {
        /** Чтений за одну готовность сокета (как у `NioTransport`): 4 × 16 КиБ. */
        const val READS_PER_READY: Int = 4

        /** Предел одного `send` (как у `WinSockTransport`): остаток ждёт в очереди, обратное давление видно. */
        const val MAX_SEND_CHUNK: Int = 64 * 1024
    }
}
