package io.github.davidepianca98.client

import io.github.davidepianca98.client.NativeTlsTestCerts.CA
import io.github.davidepianca98.client.NativeTlsTestCerts.SERVER
import io.github.davidepianca98.client.NativeTlsTestCerts.SERVER_KEY
import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.socket.tls.TLSClientSettings
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * [NativeTlsTransport] поверх фейкового TCP-транспорта, соединённого с сервером OpenSSL в памяти: порядок
 * событий, очередь до рукопожатия, обратное давление, close_notify, EOF, сроки, ошибки настроек.
 */
@OptIn(MqttCoreApi::class)
class NativeTlsTransportTest {

    /** TCP-транспорт без сокета: запись уходит серверу, ответ сервера — событиями [TransportEvent.Bytes]. */
    private class FakeTcp(private val server: NativeTlsTestServer) : MqttTransport {
        var connectCalled = false
        var closed = false
        var readInterest = true
        var tcpConnected = false
        private val events = ArrayList<TransportEvent>()

        override fun connect(host: String, port: Int, deadlineMs: Long) {
            connectCalled = true
        }

        fun completeConnect() {
            tcpConnected = true
            events += TransportEvent.Connected
        }

        override fun write(bytes: ByteArray): Int {
            if (closed) return 0
            server.feed(bytes)
            return bytes.size
        }

        override val pendingWriteBytes: Long get() = 0

        override fun setReadInterest(enabled: Boolean) {
            readInterest = enabled
        }

        override fun poll(nowMs: Long) {}

        override fun nextDeadlineMs(): Long? = null

        /** Отдать клиенту то, что сервер зашифровал (если клиент читает). */
        fun deliver() {
            if (closed || !tcpConnected || !readInterest) return
            val out = server.takeOutput()
            if (out.isNotEmpty()) events += TransportEvent.Bytes(out)
        }

        fun eof() {
            events += TransportEvent.Eof
        }

        override fun events(): List<TransportEvent> {
            val out = ArrayList(events)
            events.clear()
            return out
        }

        override fun close() {
            closed = true
        }
    }

    private val servers = ArrayList<NativeTlsTestServer>()

    @AfterTest
    fun freeServers() {
        servers.forEach { it.free() }
    }

    private fun server(): NativeTlsTestServer = NativeTlsTestServer(SERVER, SERVER_KEY).also { servers += it }

    private fun transport(tcp: FakeTcp, settings: TLSClientSettings = TLSClientSettings(serverCertificate = CA)) =
        NativeTlsTransport(tcp, openSslSessionFactory(settings, true))

    /** Несколько оборотов «хоста»: доставка ответов сервера и сбор событий транспорта. */
    private fun spin(tcp: FakeTcp, tls: NativeTlsTransport, now: Long = 0, rounds: Int = 20): List<TransportEvent> {
        val out = ArrayList<TransportEvent>()
        repeat(rounds) {
            tcp.deliver()
            tls.poll(now)
            out += tls.events()
        }
        return out
    }

    private fun bytesOf(events: List<TransportEvent>): ByteArray =
        events.filterIsInstance<TransportEvent.Bytes>().flatMap { it.bytes.toList() }.toByteArray()

    @Test
    fun connectedOnlyAfterHandshakeAndQueuedDataGoesFirst() {
        val server = server()
        val tcp = FakeTcp(server)
        val tls = transport(tcp)
        tls.connect("localhost", 8883, 10_000)
        assertTrue(tcp.connectCalled)
        assertEquals(0, tls.write("early".encodeToByteArray()), "до рукопожатия — в очередь")
        assertEquals(5L, tls.pendingWriteBytes)
        assertTrue(tls.events().isEmpty())

        tcp.completeConnect()
        val events = spin(tcp, tls)
        assertEquals(listOf<TransportEvent>(TransportEvent.Connected), events.filter { it !is TransportEvent.Bytes })
        assertEquals("early", server.received.toByteArray().decodeToString(), "очередь ушла после рукопожатия")
        assertEquals(0L, tls.pendingWriteBytes)
        assertEquals(5, tls.write("later".encodeToByteArray()), "после рукопожатия — сразу")
        assertEquals("earlylater", server.received.toByteArray().decodeToString())
        tls.close()
        assertTrue(tcp.closed)
        assertTrue(server.closeNotifyReceived, "close() после рукопожатия шлёт close_notify")
    }

    @Test
    fun largeWritesAndReadsInParts() {
        val server = server()
        val tcp = FakeTcp(server)
        val tls = transport(tcp)
        tls.connect("localhost", 8883, 10_000)
        tcp.completeConnect()
        spin(tcp, tls)
        val up = ByteArray(1_000_000) { (it * 7).toByte() }
        tls.write(up)
        assertContentEquals(up, server.received.toByteArray())

        val down = ByteArray(300_000) { (it * 13).toByte() }
        server.send(down)
        val events = spin(tcp, tls)
        assertContentEquals(down, bytesOf(events))
        assertTrue(events.filterIsInstance<TransportEvent.Bytes>().all { it.bytes.size <= 16 * 1024 }, "по записи TLS")
        tls.close()
    }

    @Test
    fun pauseReadHoldsDataUntilResumed() {
        val server = server()
        val tcp = FakeTcp(server)
        val tls = transport(tcp)
        tls.connect("localhost", 8883, 10_000)
        tcp.completeConnect()
        spin(tcp, tls)
        tls.setReadInterest(false)
        assertFalse(tcp.readInterest, "после рукопожатия пауза доходит до сокета")
        server.send("held".encodeToByteArray())
        // Уже прочитанный сокетом шифротекст при паузе не расшифровывается
        tcp.readInterest = true
        tcp.deliver()
        tcp.readInterest = false
        assertTrue(spin(tcp, tls).none { it is TransportEvent.Bytes }, "при паузе данных нет")
        tls.setReadInterest(true)
        val events = tls.events() + spin(tcp, tls)
        assertEquals("held", bytesOf(events).decodeToString())
        tls.close()
    }

    @Test
    fun closeOutputQueuesCloseNotifyAfterData() {
        val server = server()
        val tcp = FakeTcp(server)
        val tls = transport(tcp)
        tls.connect("localhost", 8883, 10_000)
        tcp.completeConnect()
        spin(tcp, tls)
        tls.write("last".encodeToByteArray())
        tls.closeOutput()
        assertEquals("last", server.received.toByteArray().decodeToString())
        assertTrue(server.closeNotifyReceived, "close_notify после данных")
        assertEquals(0, tls.write("after".encodeToByteArray()), "после closeOutput не пишем")
        tls.close()
    }

    @Test
    fun serverCloseNotifyGivesEofAfterData() {
        val server = server()
        val tcp = FakeTcp(server)
        val tls = transport(tcp)
        tls.connect("localhost", 8883, 10_000)
        tcp.completeConnect()
        spin(tcp, tls)
        server.send("bye".encodeToByteArray())
        server.shutdown()
        val events = spin(tcp, tls)
        assertEquals("bye", bytesOf(events).decodeToString())
        assertEquals(TransportEvent.Eof, events.last(), "Eof — после данных")
        assertEquals(1, events.count { it == TransportEvent.Eof })
        tls.close()
    }

    @Test
    fun tcpEofAfterHandshakeGivesEof() {
        val server = server()
        val tcp = FakeTcp(server)
        val tls = transport(tcp)
        tls.connect("localhost", 8883, 10_000)
        tcp.completeConnect()
        spin(tcp, tls)
        server.send("tail".encodeToByteArray())
        tcp.deliver()
        tcp.eof()
        val events = spin(tcp, tls)
        assertEquals("tail", bytesOf(events).decodeToString())
        assertEquals(TransportEvent.Eof, events.last())
        tls.close()
    }

    @Test
    fun eofDuringHandshakeFails() {
        val server = server()
        val tcp = FakeTcp(server)
        val tls = transport(tcp)
        tls.connect("localhost", 8883, 10_000)
        tcp.completeConnect()
        tcp.eof()
        val failed = tls.events().single()
        assertIs<TransportEvent.Failed>(failed)
        assertEquals("ssl", (failed.error as MqttError.Io).kind)
        assertTrue(tcp.closed)
    }

    @Test
    fun handshakeTimeoutIsConnectTimeout() {
        val server = server()
        val tcp = FakeTcp(server)
        val tls = transport(tcp)
        tls.connect("localhost", 8883, 1_000)
        tcp.completeConnect()
        tls.events() // начать рукопожатие, ответ сервера не доставляем
        assertEquals(1_000L, tls.nextDeadlineMs())
        tls.poll(1_000)
        assertEquals(listOf<TransportEvent>(TransportEvent.Failed(MqttError.ConnectTimeout)), tls.events())
        assertTrue(tcp.closed)
    }

    @Test
    fun hostnameMismatchFailsWithoutAddress() {
        val server = server()
        val tcp = FakeTcp(server)
        val tls = transport(tcp)
        tls.connect("other.example", 8883, 10_000)
        tcp.completeConnect()
        val failed = spin(tcp, tls).filterIsInstance<TransportEvent.Failed>().single()
        val error = failed.error as MqttError.Io
        assertEquals("ssl-hostname", error.kind)
        assertFalse("other.example" in error.toString() || "other.example" in (error.cause?.message ?: ""), "без адреса")
        assertTrue(tcp.closed)
    }

    @Test
    fun configErrorOpensNoConnection() {
        val server = server()
        val tcp = FakeTcp(server)
        val tls = transport(tcp, TLSClientSettings(serverCertificate = "/nonexistent/pinned.pem"))
        tls.connect("localhost", 8883, 10_000)
        assertFalse(tcp.connectCalled, "сокет не открывается")
        val failed = tls.events().single()
        assertIs<TransportEvent.Failed>(failed)
        assertEquals("ssl-config", (failed.error as MqttError.Io).kind)
    }
}
