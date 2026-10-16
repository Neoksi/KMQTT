package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import java.net.Inet4Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.StandardSocketOptions
import java.net.UnknownHostException
import java.nio.ByteBuffer
import java.nio.channels.CancelledKeyException
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.SocketChannel
import java.nio.channels.UnresolvedAddressException
import java.util.concurrent.atomic.AtomicReference

/**
 * Окружение транспорта внутри [NioActorHost]: селектор потока хоста, пробуждение, область для резолва имён
 * на `Dispatchers.IO`, размер буфера чтения. Создаёт только хост; транспорту передаётся через [NioTransportFactory].
 */
@MqttCoreApi
public class NioTransportContext internal constructor(
    internal val selector: Selector,
    internal val wakeup: () -> Unit,
    internal val dnsScope: CoroutineScope,
    internal val readBufferSize: Int
)

/**
 * Фабрика транспорта для [NioActorHost]: новый транспорт на каждое [HostAction.Connect].
 * TLS (S06b-T04b) — `TlsTransport(NioTransport(context), settings)`.
 */
@MqttCoreApi
public fun interface NioTransportFactory {
    public fun create(context: NioTransportContext): MqttTransport

    public companion object {
        /** Обычный TCP. */
        public val Tcp: NioTransportFactory = NioTransportFactory { NioTransport(it) }
    }
}

/**
 * TCP-транспорт на неблокирующем [SocketChannel] под селектором потока хоста (W6 §2.1, S06b-T04a).
 *
 * - Резолв имени (`InetAddress.getAllByName`, блокирующий) — отдельной задачей на `Dispatchers.IO`; результат
 *   забирает [poll] на потоке хоста. Из адресов берётся первый IPv4, иначе первый (как `InetSocketAddress(host, port)`
 *   у старого клиента). Срок — общий дедлайн [connect] (резолв + TCP): по нему [MqttError.ConnectTimeout], поздний
 *   ответ резолвера отбрасывается.
 * - Подключение неблокирующее (`OP_CONNECT`), поэтому закрытие во время подключения мгновенное (H5).
 * - Чтение: буфер [NioTransportContext.readBufferSize] (16 КиБ), до [READS_PER_READY] чтений за одну готовность,
 *   каждое — событие [TransportEvent.Bytes] с копией байтов. Запись: очередь `ByteBuffer`-обёрток над массивами
 *   вызывающего (без копирования, K7); неполная запись оставляет остаток и ставит `OP_WRITE`.
 * - Ошибки: `UnresolvedAddressException`/`UnknownHostException` → [MqttError.UnknownHost], прочие исключения →
 *   [MqttError.Io] с простым именем класса исключения (без адреса в `kind`); канал закрывается сразу.
 * - `TCP_NODELAY` включён: пакеты MQTT мелкие, задержка «команда → пакет» важнее (H4).
 *
 * Все методы — только на потоке хоста.
 */
@MqttCoreApi
public class NioTransport internal constructor(
    private val context: NioTransportContext,
    private val resolver: (String) -> Array<InetAddress>
) : MqttTransport {

    public constructor(context: NioTransportContext) : this(context, DefaultResolver)

    private enum class Phase { Idle, Resolving, Connecting, Connected, Closed }

    private var phase = Phase.Idle
    private var channel: SocketChannel? = null
    private var key: SelectionKey? = null
    private var port = 0
    private var deadlineMs: Long? = null
    private var dnsJob: Job? = null
    private val dnsResult = AtomicReference<Any?>(null)
    private val events = ArrayList<TransportEvent>()
    private val writeQueue = ArrayDeque<ByteBuffer>()
    private var readBuffer: ByteBuffer? = null
    private var readInterest = true
    private var eof = false

    override var pendingWriteBytes: Long = 0
        private set

    override fun connect(host: String, port: Int, deadlineMs: Long) {
        check(phase == Phase.Idle) { "NioTransport: connect() once per transport" }
        this.port = port
        this.deadlineMs = deadlineMs
        phase = Phase.Resolving
        dnsJob = context.dnsScope.launch {
            val result: Any = try {
                resolver(host)
            } catch (e: Throwable) {
                e
            }
            dnsResult.set(result)
            context.wakeup()
        }
    }

    override fun write(bytes: ByteArray): Int {
        if (phase == Phase.Closed || bytes.isEmpty()) return 0
        writeQueue.addLast(ByteBuffer.wrap(bytes))
        pendingWriteBytes += bytes.size
        if (phase != Phase.Connected) return 0
        val before = pendingWriteBytes
        try {
            flush()
        } catch (e: Exception) {
            fail(errorOf(e))
        }
        return (before - pendingWriteBytes).toInt()
    }

    override fun setReadInterest(enabled: Boolean) {
        readInterest = enabled
        updateInterest()
    }

    override fun poll(nowMs: Long) {
        if (phase == Phase.Resolving) {
            val result = dnsResult.getAndSet(null)
            if (result != null) {
                dnsJob = null
                if (result is Throwable) {
                    fail(errorOf(result))
                } else {
                    @Suppress("UNCHECKED_CAST")
                    openChannel(result as Array<InetAddress>)
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
        if (phase == Phase.Closed) return
        phase = Phase.Closed
        dnsJob?.cancel()
        dnsJob = null
        try {
            key?.cancel()
        } catch (_: Exception) {
        }
        try {
            channel?.close()
        } catch (_: Exception) {
        }
        key = null
        channel = null
        writeQueue.clear()
        pendingWriteBytes = 0
        readBuffer = null
    }

    /** Готовность ключа этого транспорта; вызывает хост после `select`. */
    internal fun onSelected(selected: SelectionKey) {
        if (phase == Phase.Closed || selected !== key) return
        val ch = channel ?: return
        try {
            if (phase == Phase.Connecting && selected.isConnectable) {
                if (ch.finishConnect()) onConnected()
            }
            if (phase == Phase.Connected && selected.isValid && selected.isWritable) flush()
            if (phase == Phase.Connected && selected.isValid && selected.isReadable) readSome(ch)
        } catch (e: CancelledKeyException) {
            fail(MqttError.Io(e.javaClass.simpleName, e))
        } catch (e: Exception) {
            fail(errorOf(e))
        }
    }

    private fun openChannel(addresses: Array<InetAddress>) {
        val address = addresses.firstOrNull { it is Inet4Address } ?: addresses.firstOrNull()
        if (address == null) {
            fail(MqttError.UnknownHost)
            return
        }
        val ch = SocketChannel.open()
        channel = ch
        phase = Phase.Connecting
        try {
            ch.configureBlocking(false)
            ch.setOption(StandardSocketOptions.TCP_NODELAY, true)
            val connected = ch.connect(InetSocketAddress(address, port))
            key = ch.register(context.selector, SelectionKey.OP_CONNECT, this)
            if (connected) onConnected()
        } catch (e: Exception) {
            fail(errorOf(e))
        }
    }

    private fun onConnected() {
        phase = Phase.Connected
        deadlineMs = null
        events += TransportEvent.Connected
        flush()
        updateInterest()
    }

    private fun flush() {
        val ch = channel ?: return
        while (writeQueue.isNotEmpty()) {
            val buffer = writeQueue.first()
            val written = ch.write(buffer)
            pendingWriteBytes -= written
            if (buffer.hasRemaining()) break
            writeQueue.removeFirst()
        }
        updateInterest()
    }

    private fun readSome(ch: SocketChannel) {
        val buffer = readBuffer ?: ByteBuffer.allocate(context.readBufferSize).also { readBuffer = it }
        repeat(READS_PER_READY) {
            if (phase != Phase.Connected || !readInterest || eof) return
            buffer.clear()
            val count = ch.read(buffer)
            when {
                count > 0 -> events += TransportEvent.Bytes(buffer.array().copyOf(count))
                count < 0 -> {
                    eof = true
                    events += TransportEvent.Eof
                    updateInterest()
                    return
                }
                else -> return
            }
        }
    }

    private fun updateInterest() {
        val k = key ?: return
        if (!k.isValid) return
        val ops = when (phase) {
            Phase.Connecting -> SelectionKey.OP_CONNECT
            Phase.Connected -> {
                var ops = 0
                if (readInterest && !eof) ops = ops or SelectionKey.OP_READ
                if (pendingWriteBytes > 0) ops = ops or SelectionKey.OP_WRITE
                ops
            }
            else -> 0
        }
        try {
            k.interestOps(ops)
        } catch (_: CancelledKeyException) {
        }
    }

    private fun fail(error: MqttError) {
        if (phase == Phase.Closed) return
        events += TransportEvent.Failed(error)
        close()
    }

    internal companion object {
        /** Чтений за одну готовность сокета: ограничивает работу и память одного оборота цикла (4 × 16 КиБ). */
        const val READS_PER_READY: Int = 4

        val DefaultResolver: (String) -> Array<InetAddress> = { host -> InetAddress.getAllByName(host) }

        fun errorOf(e: Throwable): MqttError = when (e) {
            is UnresolvedAddressException, is UnknownHostException -> MqttError.UnknownHost
            else -> MqttError.Io(e.javaClass.simpleName.ifEmpty { e.javaClass.name }, e)
        }
    }
}
