package io.github.davidepianca98.client

import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.thread

// Перенос `shared/jvmTest/.../testbed/TcpProxy.kt` с добавлением «полуоткрытого» соединения
// ([TcpProxy.holdDownstream]/[TcpProxy.holdUpstream]: байты одного направления задерживаются, сокеты открыты, RST нет),
// счётчика байт брокер → клиент и списка типов пакетов клиент → брокер.

/**
 * TCP-прокси клиент → брокер на 127.0.0.1: [cut] обрывает все соединения и отвергает новые (потеря сети у клиента,
 * брокер при этом жив), [restore] снова пропускает. [holdDownstream]/[holdUpstream] — «полуоткрытое»: байты
 * соответствующего направления копятся и не доходят до адресата, пока флаг не снят, соединение остаётся открытым.
 *
 * Прокси ещё и считает: каждое принятое и проброшенное брокеру соединение — [ProxiedConnection] ([connections],
 * [accepted], [open]) с разобранными пакетами направления клиент → брокер ([ClientPacket]: CONNECT с учётными
 * данными, SUBSCRIBE, UNSUBSCRIBE, PUBLISH, DISCONNECT) — так тесты стенда видят на проводе, сколько подключений
 * у клиента, какие фильтры он подписал и снял и какие команды с какой нагрузкой отправил.
 */
internal class TcpProxy(private val targetHost: String, private val targetPort: Int) : AutoCloseable {
    private val server = ServerSocket(0, 50, InetAddress.getByName(HOST))
    private val sockets = CopyOnWriteArrayList<Socket>()

    /** Все проброшенные брокеру соединения клиентов в порядке подключения (в том числе уже закрытые). */
    val connections = CopyOnWriteArrayList<ProxiedConnection>()

    @Volatile
    private var cut = false

    /** Не пропускать байты брокер → клиент (PUBACK, SUBACK и т. д. не доходят; клиент → брокер идёт как обычно). */
    @Volatile
    var holdDownstream = false

    /** Не пропускать байты клиент → брокер. */
    @Volatile
    var holdUpstream = false

    /** Сколько байт брокер → клиент проброшено за всё время (для проверки обратного давления). */
    val downstreamBytes = java.util.concurrent.atomic.AtomicLong()

    val port: Int get() = server.localPort

    /** Сколько TCP-соединений клиентов было проброшено брокеру. */
    val accepted: Int get() = connections.size

    /** Сколько из них открыто сейчас. */
    val open: Int get() = connections.count { it.isOpen }

    init {
        thread(name = "tcp-proxy-$port", isDaemon = true) {
            while (!server.isClosed) {
                val client = runCatching { server.accept() }.getOrNull() ?: break
                if (cut) {
                    client.close()
                    continue
                }
                val upstream =
                    runCatching { Socket(targetHost, targetPort) }.getOrElse {
                        client.close()
                        continue
                    }
                sockets += client
                sockets += upstream
                val connection = ProxiedConnection(connections.size + 1)
                connections += connection
                pump(client.getInputStream(), upstream.getOutputStream(), client, upstream, connection, { holdUpstream }) {
                    connection.feed(it)
                }
                pump(upstream.getInputStream(), client.getOutputStream(), client, upstream, connection, { holdDownstream }) {
                    downstreamBytes.addAndGet(it.size.toLong())
                }
            }
        }
    }

    private fun pump(
        from: InputStream,
        to: OutputStream,
        client: Socket,
        upstream: Socket,
        connection: ProxiedConnection,
        held: () -> Boolean,
        tap: ((ByteArray) -> Unit)? = null
    ) = thread(isDaemon = true) {
        try {
            val buffer = ByteArray(1024)
            while (true) {
                val read = from.read(buffer)
                if (read < 0) break
                var waited = false
                while (held() && !client.isClosed) {
                    waited = true
                    Thread.sleep(5)
                }
                // Задержанные байты не доходят до адресата, если соединение оборвано, пока они ждали (cut)
                if (waited && client.isClosed) break
                to.write(buffer, 0, read)
                to.flush()
                tap?.invoke(buffer.copyOf(read))
            }
        } catch (e: Exception) {
            // обрыв — закрываем пару
        } finally {
            connection.isOpen = false
            runCatching { client.close() }
            runCatching { upstream.close() }
        }
    }

    fun cut() {
        cut = true
        sockets.forEach { runCatching { it.close() } }
        sockets.clear()
    }

    fun restore() {
        cut = false
    }

    override fun close() {
        cut()
        server.close()
    }

    companion object {
        const val HOST = "127.0.0.1"
    }
}

/** Пакет направления клиент → брокер, разобранный прокси; [atMs] — момент разбора (часы системы). */
internal sealed interface ClientPacket {
    val atMs: Long

    data class Connect(
        val clientId: String,
        val login: String?,
        val password: String?,
        val protocolLevel: Int,
        override val atMs: Long,
        val cleanStart: Boolean = true,
    ) : ClientPacket

    data class Subscribe(val filters: List<String>, override val atMs: Long) : ClientPacket

    data class Unsubscribe(val filters: List<String>, override val atMs: Long) : ClientPacket

    data class Publish(
        val topic: String,
        val payload: String,
        val qos: Int,
        val retain: Boolean,
        override val atMs: Long,
        val dup: Boolean = false,
        val packetId: Int? = null,
    ) : ClientPacket

    data class Disconnect(override val atMs: Long) : ClientPacket
}

/** Соединение клиента через [TcpProxy]: открыто ли оно и что клиент отправил брокеру ([packets], по порядку). */
internal class ProxiedConnection(val number: Int) {
    @Volatile
    var isOpen: Boolean = true
        internal set

    val packets = CopyOnWriteArrayList<ClientPacket>()

    /** Типы всех пакетов клиент → брокер по порядку (старший полубайт: 1 CONNECT, 3 PUBLISH, 14 DISCONNECT…). */
    val packetTypes = CopyOnWriteArrayList<Int>()

    val connect: ClientPacket.Connect? get() = packets.firstOrNull() as? ClientPacket.Connect

    val subscribed: List<String> get() = packets.filterIsInstance<ClientPacket.Subscribe>().flatMap { it.filters }

    val unsubscribed: List<String> get() = packets.filterIsInstance<ClientPacket.Unsubscribe>().flatMap { it.filters }

    val published: List<ClientPacket.Publish> get() = packets.filterIsInstance<ClientPacket.Publish>()

    /** Последняя ошибка разбора пакета (диагностика теста; пакет при этом пропущен). */
    @Volatile
    var parseError: Throwable? = null
        private set

    private var pending = ByteArray(0)
    private var protocolLevel = 0

    // Только из потока прокси этого соединения: куски потока TCP складываются в целые пакеты MQTT
    internal fun feed(chunk: ByteArray) {
        pending += chunk
        while (true) {
            val packet = takePacket() ?: return
            runCatching { parse(packet) }.onFailure { parseError = it }
        }
    }

    private fun takePacket(): ByteArray? {
        if (pending.size < 2) return null
        var length = 0
        var shift = 0
        var index = 1
        while (true) {
            if (index >= pending.size) return null
            val byte = pending[index++].toInt() and 0xFF
            length = length or ((byte and 0x7F) shl shift)
            if (byte and 0x80 == 0) break
            shift += 7
            if (shift > 21) return null
        }
        val total = index + length
        if (pending.size < total) return null
        val packet = pending.copyOfRange(0, total)
        pending = pending.copyOfRange(total, pending.size)
        return packet
    }

    private fun parse(bytes: ByteArray) {
        val reader = PacketReader(bytes)
        val header = reader.u8()
        packetTypes += header shr 4
        reader.varint()
        val now = System.currentTimeMillis()
        when (header shr 4) {
            TYPE_CONNECT -> {
                reader.string()
                protocolLevel = reader.u8()
                val flags = reader.u8()
                reader.u16()
                if (protocolLevel == 5) reader.skipProperties()
                val clientId = reader.string()
                if (flags and WILL_FLAG != 0) {
                    if (protocolLevel == 5) reader.skipProperties()
                    reader.string()
                    reader.binary()
                }
                val login = if (flags and USER_FLAG != 0) reader.string() else null
                val password = if (flags and PASSWORD_FLAG != 0) String(reader.binary()) else null
                packets += ClientPacket.Connect(clientId, login, password, protocolLevel, now, flags and CLEAN_START != 0)
            }

            TYPE_PUBLISH -> {
                val qos = (header shr 1) and 3
                val topic = reader.string()
                val packetId = if (qos > 0) reader.u16() else null
                if (protocolLevel == 5) reader.skipProperties()
                val dup = header and DUP_FLAG != 0
                packets += ClientPacket.Publish(topic, String(reader.rest()), qos, header and 1 != 0, now, dup, packetId)
            }

            TYPE_SUBSCRIBE, TYPE_UNSUBSCRIBE -> {
                reader.u16()
                if (protocolLevel == 5) reader.skipProperties()
                val filters = mutableListOf<String>()
                while (reader.hasMore()) {
                    filters += reader.string()
                    if (header shr 4 == TYPE_SUBSCRIBE) reader.u8()
                }
                packets += if (header shr 4 == TYPE_SUBSCRIBE) {
                    ClientPacket.Subscribe(filters, now)
                } else {
                    ClientPacket.Unsubscribe(filters, now)
                }
            }

            TYPE_DISCONNECT -> packets += ClientPacket.Disconnect(now)
        }
    }

    private class PacketReader(private val bytes: ByteArray) {
        private var position = 0

        fun hasMore() = position < bytes.size

        fun u8() = bytes[position++].toInt() and 0xFF

        fun u16() = (u8() shl 8) or u8()

        fun varint(): Int {
            var value = 0
            var shift = 0
            while (true) {
                val byte = u8()
                value = value or ((byte and 0x7F) shl shift)
                if (byte and 0x80 == 0) return value
                shift += 7
            }
        }

        fun binary(): ByteArray {
            val length = u16()
            return bytes.copyOfRange(position, position + length).also { position += length }
        }

        fun string() = String(binary(), Charsets.UTF_8)

        fun skipProperties() {
            val length = varint()
            position += length
        }

        fun rest(): ByteArray = bytes.copyOfRange(position, bytes.size).also { position = bytes.size }
    }

    private companion object {
        const val TYPE_CONNECT = 1
        const val TYPE_PUBLISH = 3
        const val TYPE_SUBSCRIBE = 8
        const val TYPE_UNSUBSCRIBE = 10
        const val TYPE_DISCONNECT = 14
        const val USER_FLAG = 0x80
        const val PASSWORD_FLAG = 0x40
        const val WILL_FLAG = 0x04
        const val CLEAN_START = 0x02
        const val DUP_FLAG = 0x08
    }
}
