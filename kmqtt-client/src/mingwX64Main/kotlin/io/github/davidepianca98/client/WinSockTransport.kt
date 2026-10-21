package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch

/**
 * Окружение транспорта внутри [WinSockHost]: пробуждение, область для резолва имён на `Dispatchers.IO`, размер буфера
 * чтения и реестр сокетов, которые хост ставит в `select`. Создаёт только хост; транспорту передаётся через
 * [WinSockTransportFactory].
 */
@MqttCoreApi
public class WinSockTransportContext internal constructor(
    internal val wakeup: () -> Unit,
    internal val dnsScope: CoroutineScope,
    internal val readBufferSize: Int
) {
    /** Живые транспорты (только поток хоста): хост опрашивает их готовность, даже если они обёрнуты (TLS и т. п.). */
    internal val active: MutableList<WinSockTransport> = ArrayList()
}

/** Фабрика транспорта для [WinSockHost]: новый транспорт на каждое [HostAction.Connect]. */
@MqttCoreApi
public fun interface WinSockTransportFactory {
    public fun create(context: WinSockTransportContext): MqttTransport

    public companion object {
        /** Обычный TCP. */
        public val Tcp: WinSockTransportFactory = WinSockTransportFactory { WinSockTransport(it) }
    }
}

/**
 * TCP-транспорт mingwX64 на неблокирующем сокете WinSock под `select` потока хоста (перенос `NioTransport`).
 *
 * - Резолв имени (`getaddrinfo`, блокирующий) — отдельной задачей на `Dispatchers.IO`; результат забирает [poll] на
 *   потоке хоста. Берётся первый IPv4, иначе первый адрес. Срок — общий дедлайн [connect] (резолв + TCP): по нему
 *   [MqttError.ConnectTimeout], поздний ответ резолвера отбрасывается (`getaddrinfo` не прерывается: поток `IO` занят до
 *   ответа ОС, как на JVM).
 * - Подключение неблокирующее: успех — сокет готов к записи и `SO_ERROR == 0`; отказ WinSock сообщает через
 *   `exceptfds` (не `writefds`, как POSIX), код — `SO_ERROR`. Закрытие во время подключения мгновенное.
 * - Чтение: буфер [WinSockTransportContext.readBufferSize] (16 КиБ), до [READS_PER_READY] чтений за готовность, каждое —
 *   [TransportEvent.Bytes] с копией; `recv == 0` — [TransportEvent.Eof]. Запись: очередь массивов вызывающего (без
 *   копирования), `send` порциями не больше [MAX_SEND_CHUNK]; неполная запись оставляет остаток до готовности к записи.
 * - Ошибки: `getaddrinfo` `WSAHOST_NOT_FOUND`/`WSATRY_AGAIN`/`WSANO_DATA` → [MqttError.UnknownHost]; прочие →
 *   [MqttError.Io] с именем кода WinSock (`WSAECONNREFUSED`, `WSAECONNRESET`…) в `kind`, без адреса.
 * - `TCP_NODELAY` включён. [close]: `shutdown(SD_SEND)` (FIN за отданными ОС данными) и `closesocket`; если в
 *   буфере приёма остались непрочитанные данные, ОС всё равно ответит RST — поэтому хост при `FlushAndClose` читает и
 *   отбрасывает входящее до закрытия.
 *
 * Все методы — только на потоке хоста.
 */
@MqttCoreApi
public class WinSockTransport internal constructor(
    private val context: WinSockTransportContext,
    private val resolver: (String, Int) -> WinSock.Resolved
) : MqttTransport {

    public constructor(context: WinSockTransportContext) : this(context, WinSock::resolve)

    private enum class Phase { Idle, Resolving, Connecting, Connected, Closed }

    private var phase = Phase.Idle
    private var socket: ULong = WinSock.INVALID
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

    /** Сокет для `select` или [WinSock.INVALID]. */
    internal val fd: ULong get() = socket

    internal val wantsRead: Boolean get() = phase == Phase.Connected && readInterest && !eof

    internal val wantsWrite: Boolean
        get() = phase == Phase.Connecting || (phase == Phase.Connected && pendingWriteBytes > 0)

    internal val wantsExcept: Boolean get() = phase == Phase.Connecting

    override fun connect(host: String, port: Int, deadlineMs: Long) {
        check(phase == Phase.Idle) { "WinSockTransport: connect() once per transport" }
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
                    is WinSock.Resolved.Found -> open(result.address)
                    is WinSock.Resolved.Error -> fail(resolveError(result.code))
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

    /** Готовность сокета по итогам `select`; вызывает хост. */
    internal fun onReady(readable: Boolean, writable: Boolean, failed: Boolean) {
        when (phase) {
            Phase.Connecting -> {
                if (failed || writable) {
                    // Итог connect — по SO_ERROR: код — отказ; 0 и готовность к записи — подключено; 0 при одном
                    // exceptfds (без кода) — ждём дальше, срок подключения всё равно ограничивает ожидание
                    val error = WinSock.pendingError(socket)
                    when {
                        error != 0 -> {
                            fail(WinSock.ioError(error))
                            return
                        }
                        writable -> onConnected()
                    }
                }
            }
            Phase.Connected -> {
                if (writable) flush()
                if (readable && phase == Phase.Connected) readSome()
            }
            else -> Unit
        }
    }

    private fun open(address: WinSock.Address) {
        phase = Phase.Connecting
        val (s, openError) = WinSock.openStream(address.family)
        if (s == WinSock.INVALID) {
            fail(WinSock.ioError(openError))
            return
        }
        socket = s
        context.active += this
        val nonBlocking = WinSock.setNonBlocking(s)
        if (nonBlocking != 0) {
            fail(WinSock.ioError(nonBlocking))
            return
        }
        WinSock.noDelay(s)
        val error = WinSock.connectTo(s, address)
        if (error == 0) {
            onConnected()
            return
        }
        if (error != WinSock.WSAEWOULDBLOCK && error != WinSock.WSAEINPROGRESS && error != WinSock.WSAEALREADY) {
            fail(WinSock.ioError(error))
        }
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
            val n = WinSock.sendSome(s, bytes, writeOffset, minOf(bytes.size - writeOffset, MAX_SEND_CHUNK))
            if (n < 0) {
                if (-n != WinSock.WSAEWOULDBLOCK) fail(WinSock.ioError(-n))
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
            val n = WinSock.recvSome(socket, buffer)
            when {
                n > 0 -> events += TransportEvent.Bytes(buffer.copyOf(n))
                n == 0 -> {
                    eof = true
                    events += TransportEvent.Eof
                    return
                }
                else -> {
                    if (-n != WinSock.WSAEWOULDBLOCK) fail(WinSock.ioError(-n))
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
        socket = WinSock.INVALID
        WinSock.close(s, orderly && wasConnected)
        writeQueue.clear()
        writeOffset = 0
        pendingWriteBytes = 0
        readBuffer = null
    }

    private fun resolveError(code: Int): MqttError = when (code) {
        WinSock.WSAHOST_NOT_FOUND, WinSock.WSATRY_AGAIN, WinSock.WSANO_DATA -> MqttError.UnknownHost
        else -> MqttError.Io("getaddrinfo-$code", IllegalStateException("getaddrinfo error $code"))
    }

    internal companion object {
        /** Чтений за одну готовность сокета (как у `NioTransport`): 4 × 16 КиБ. */
        const val READS_PER_READY: Int = 4

        /**
         * Предел одного `send`: WinSock принимает неблокирующий `send` любой длины целиком, пока буфер отправки не полон
         * (данные копируются в память ядра). Порциями по 64 КиБ в ядре лежит не больше `SO_SNDBUF` + порция, а остаток
         * ждёт в очереди транспорта — обратное давление видно (`pendingWriteBytes`), память ядра ограничена.
         */
        const val MAX_SEND_CHUNK: Int = 64 * 1024
    }
}
