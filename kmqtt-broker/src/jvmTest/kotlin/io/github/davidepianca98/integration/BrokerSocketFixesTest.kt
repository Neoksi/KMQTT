package io.github.davidepianca98.integration

import io.github.davidepianca98.mqtt.broker.interfaces.Authentication
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.EOFException
import java.net.InetAddress
import java.net.Socket
import java.security.MessageDigest
import java.util.Base64
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

// встроенный брокер форка - WebSocket по RFC 6455 (полный кадр закрытия, FIN и продолжение, pong) и CONNACK
// на отказ во входе в MQTT 3.1.1. Клиенты пишутся вручную, чтобы тесты не зависели от клиентов форка.

private const val KEY = "dGhlIHNhbXBsZSBub25jZQ=="
private const val GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

private class RawFrame(val fin: Boolean, val opcode: Int, val payload: ByteArray)

/** Клиент WebSocket брокера: рукопожатие, замаскированные кадры, чтение кадров брокера. */
private class RawWebSocket(port: Int) : AutoCloseable {
    private val socket = Socket(InetAddress.getLoopbackAddress(), port).apply { soTimeout = RAW_READ_TIMEOUT_MS }
    private val input = DataInputStream(socket.getInputStream().buffered())
    private val output = socket.getOutputStream()

    init {
        output.write(
            (
                "GET /mqtt HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
                    "Sec-WebSocket-Key: $KEY\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n"
                ).toByteArray()
        )
        output.flush()
        val head = ByteArrayOutputStream()
        while (!head.toString(Charsets.ISO_8859_1).endsWith("\r\n\r\n")) head.write(input.readUnsignedByte())
        val text = head.toString(Charsets.ISO_8859_1)
        assertTrue(text.startsWith("HTTP/1.1 101"), text)
        val accept = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-1").digest((KEY + GUID).toByteArray()))
        assertTrue("Sec-WebSocket-Accept: $accept" in text, text)
    }

    fun send(opcode: Int, payload: ByteArray, fin: Boolean = true) {
        val mask = byteArrayOf(0x12, 0x34, 0x56, 0x78)
        val out = ByteArrayOutputStream()
        out.write((if (fin) 0x80 else 0) or opcode)
        if (payload.size <= 125) {
            out.write(0x80 or payload.size)
        } else {
            out.write(0x80 or 126)
            out.write(payload.size shr 8)
            out.write(payload.size and 0xFF)
        }
        out.write(mask)
        out.write(ByteArray(payload.size) { (payload[it].toInt() xor mask[it and 3].toInt()).toByte() })
        output.write(out.toByteArray())
        output.flush()
    }

    fun read(): RawFrame {
        val b1 = input.readUnsignedByte()
        var length = input.readUnsignedByte() and 0x7F
        if (length == 126) length = input.readUnsignedShort()
        val payload = ByteArray(length)
        input.readFully(payload)
        return RawFrame(b1 and 0x80 != 0, b1 and 0x0F, payload)
    }

    /** Конец потока после кадров (брокер закрыл соединение). */
    fun assertEnd() {
        assertFailsWith<EOFException> { input.readUnsignedByte() }
    }

    override fun close() = socket.close()
}

private fun connect(version: Int, clientId: String, user: String? = null, password: String? = null): ByteArray {
    fun utf8(text: String) = text.toByteArray().let { byteArrayOf((it.size shr 8).toByte(), it.size.toByte()) + it }
    val body = ByteArrayOutputStream()
    body.write(utf8("MQTT"))
    body.write(version)
    var flags = 0x02
    if (user != null) flags = flags or 0x80
    if (password != null) flags = flags or 0x40
    body.write(flags)
    body.write(byteArrayOf(0x00, 0x3C))
    if (version == 5) body.write(0x00)
    body.write(utf8(clientId))
    user?.let { body.write(utf8(it)) }
    password?.let { body.write(utf8(it)) }
    return byteArrayOf(0x10, body.size().toByte()) + body.toByteArray()
}

class BrokerSocketFixesTest {
    private val brokers = mutableListOf<RawTestBroker>()

    @AfterTest
    fun tearDown() {
        brokers.forEach { it.close() }
        brokers.clear()
    }

    private fun webSocketBroker() = RawTestBroker(webSocket = true).also { brokers += it }

    private fun assertConnack(frame: RawFrame, code: Int = 0) {
        assertEquals(2, frame.opcode, "двоичный кадр")
        assertContentEquals(byteArrayOf(0x20, 0x02, 0x00, code.toByte()), frame.payload)
    }

    @Test
    fun fragmentedConnectIsAssembledAcrossContinuationFrames() {
        val broker = webSocketBroker()
        RawWebSocket(broker.webSocketPort!!).use { ws ->
            val packet = connect(4, "frag")
            ws.send(0x2, packet.copyOfRange(0, 3), fin = false)
            ws.send(0x0, packet.copyOfRange(3, 9), fin = false)
            ws.send(0x0, packet.copyOfRange(9, packet.size), fin = true)
            assertConnack(ws.read())
        }
    }

    @Test
    fun pongIsIgnoredAndPingIsAnswered() {
        val broker = webSocketBroker()
        RawWebSocket(broker.webSocketPort!!).use { ws ->
            ws.send(0xA, "unsolicited".toByteArray())
            ws.send(0x9, "hi".toByteArray())
            val pong = ws.read()
            assertEquals(0xA, pong.opcode, "ответ на ping, а не обрыв после pong")
            assertEquals("hi", pong.payload.decodeToString())
            ws.send(0x2, connect(4, "afterpong"))
            assertConnack(ws.read())
        }
    }

    @Test
    fun continuationWithoutStartedMessageClosesTheConnectionWithACompleteCloseFrame() {
        val broker = webSocketBroker()
        RawWebSocket(broker.webSocketPort!!).use { ws ->
            ws.send(0x0, byteArrayOf(1, 2, 3))
            val close = ws.read()
            assertEquals(0x8, close.opcode)
            assertTrue(close.fin)
            assertEquals(0, close.payload.size)
            ws.assertEnd()
        }
    }

    @Test
    fun clientCloseIsAnsweredWithTheSameStatusCode() {
        val broker = webSocketBroker()
        RawWebSocket(broker.webSocketPort!!).use { ws ->
            ws.send(0x8, byteArrayOf(0x03, 0xE8.toByte()))
            val close = ws.read()
            assertEquals(0x8, close.opcode)
            assertContentEquals(byteArrayOf(0x03, 0xE8.toByte()), close.payload)
        }
    }

    /** Закрытие со стороны брокера (после DISCONNECT): кадр закрытия из двух байт 0x88 0x00, затем конец потока. */
    @Test
    fun brokerCloseSendsACompleteCloseFrame() {
        val broker = webSocketBroker()
        RawWebSocket(broker.webSocketPort!!).use { ws ->
            ws.send(0x2, connect(4, "bye"))
            assertConnack(ws.read())
            ws.send(0x2, byteArrayOf(0xE0.toByte(), 0x00)) // DISCONNECT
            val close = ws.read()
            assertEquals(0x8, close.opcode)
            assertTrue(close.fin)
            assertEquals(0, close.payload.size)
            ws.assertEnd()
        }
    }

    // ---- CONNACK на отказ во входе в MQTT 3.1.1 ----

    private val authentication = object : Authentication {
        override fun authenticate(clientId: String, username: String?, password: UByteArray?) =
            username == "user" && password?.toByteArray()?.decodeToString() == "secret"
    }

    private fun refusedConnack(packet: ByteArray): ByteArray {
        val broker = RawTestBroker(authentication = authentication).also { brokers += it }
        Socket(InetAddress.getLoopbackAddress(), broker.port).use { socket ->
            socket.soTimeout = RAW_READ_TIMEOUT_MS
            socket.getOutputStream().apply {
                write(packet)
                flush()
            }
            val input = DataInputStream(socket.getInputStream())
            val connack = ByteArray(4)
            input.readFully(connack)
            while (true) if (input.read() == -1) break // после отказа брокер закрывает соединение (иначе - тайм-аут)
            return connack
        }
    }

    @Test
    fun wrongPasswordInMqtt311GetsConnackNotAuthorized() {
        assertContentEquals(
            byteArrayOf(0x20, 0x02, 0x00, 0x05),
            refusedConnack(connect(4, "c1", user = "user", password = "wrong"))
        )
    }

    @Test
    fun missingCredentialsInMqtt311GetConnackNotAuthorized() {
        assertContentEquals(byteArrayOf(0x20, 0x02, 0x00, 0x05), refusedConnack(connect(4, "c2")))
    }

    @Test
    fun wrongPasswordInMqtt5StillGetsConnackNotAuthorized() {
        val connack = refusedConnack(connect(5, "c3", user = "user", password = "wrong"))
        assertEquals(0x20, connack[0].toInt())
        assertEquals(0x87, connack[3].toInt() and 0xFF, "MQTT 5: NOT_AUTHORIZED в причине CONNACK")
    }
}
