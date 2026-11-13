@file:OptIn(MqttCoreApi::class)

package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import kotlinx.coroutines.runBlocking
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.net.SocketTimeoutException
import java.security.MessageDigest
import java.util.Base64
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * `WebSocketTransport` (над `NioTransport`) в `NioActorHost` против локального WebSocket-сервера на
 * блокирующих сокетах ([Peer]): запрос `Upgrade`, проверки ответа, маскирование, длины 7/16/64 бит, фрагментация и
 * ping посреди неё, нарушения протокола, close-кадры в обе стороны, EOF и обрыв, срок `Upgrade`.
 */
class WebSocketTransportTest {

    private class TestDriver : HostDriver {
        val commands = ConcurrentLinkedQueue<HostAction>()
        val events = LinkedBlockingQueue<TransportEvent>()
        val firstRun = CountDownLatch(1)

        override fun runOnce(nowMs: Long, events: List<TransportEvent>): HostStep {
            this.events.addAll(events)
            firstRun.countDown()
            val actions = ArrayList<HostAction>()
            while (true) actions += commands.poll() ?: break
            return HostStep(null, actions)
        }

        fun send(host: MqttHost, vararg actions: HostAction) {
            commands.addAll(actions)
            host.wakeup()
        }

        fun awaitEvent(timeoutMs: Long = 5_000): TransportEvent =
            events.poll(timeoutMs, TimeUnit.MILLISECONDS) ?: fail("нет события транспорта за $timeoutMs мс")

        /** Собрать [count] байт из событий `Bytes` (данные могут прийти несколькими событиями). */
        fun readBytes(count: Int, timeoutMs: Long = 10_000): ByteArray {
            val out = ByteArrayOutputStream()
            while (out.size() < count) out.write(assertIs<TransportEvent.Bytes>(awaitEvent(timeoutMs)).bytes)
            assertEquals(count, out.size(), "лишние байты")
            return out.toByteArray()
        }
    }

    /** Кадр, прочитанный сервером. */
    private class Frame(val fin: Boolean, val opcode: Int, val masked: Boolean, val lengthForm: Int, val payload: ByteArray)

    /** Сторона сервера одного соединения: HTTP `Upgrade` и кадры RFC 6455 на блокирующем сокете. */
    private class Peer(val socket: Socket) {
        val input: InputStream = socket.getInputStream()
        val output: OutputStream = socket.getOutputStream()
        var request: String = ""
            private set

        fun readRequest(): String {
            val out = ByteArrayOutputStream()
            while (true) {
                val b = input.read()
                if (b < 0) fail("EOF в запросе Upgrade")
                out.write(b)
                val bytes = out.toByteArray()
                val n = bytes.size
                if (n >= 4 && bytes[n - 4] == '\r'.code.toByte() && bytes[n - 3] == '\n'.code.toByte() &&
                    bytes[n - 2] == '\r'.code.toByte() && bytes[n - 1] == '\n'.code.toByte()
                ) {
                    request = String(bytes, Charsets.US_ASCII)
                    return request
                }
            }
        }

        fun header(name: String): String? = request.split("\r\n").drop(1)
            .firstOrNull { it.substringBefore(':').trim().equals(name, ignoreCase = true) }
            ?.substringAfter(':')?.trim()

        fun accept(): String = acceptFor(header("Sec-WebSocket-Key") ?: fail("нет Sec-WebSocket-Key"))

        fun okResponse(extra: String = "", protocol: String? = "mqtt", accept: String = accept()): String =
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
                "Sec-WebSocket-Accept: $accept\r\n" +
                (if (protocol != null) "Sec-WebSocket-Protocol: $protocol\r\n" else "") + extra + "\r\n"

        fun write(bytes: ByteArray) {
            output.write(bytes)
            output.flush()
        }

        fun upgrade(extraBytes: ByteArray = ByteArray(0)) {
            readRequest()
            write(okResponse().toByteArray(Charsets.US_ASCII) + extraBytes)
        }

        fun readExactly(count: Int): ByteArray {
            val out = ByteArray(count)
            var read = 0
            while (read < count) {
                val n = input.read(out, read, count - read)
                if (n < 0) fail("EOF после $read из $count")
                read += n
            }
            return out
        }

        fun readFrame(): Frame {
            val head = readExactly(2)
            val b0 = head[0].toInt() and 0xFF
            val b1 = head[1].toInt() and 0xFF
            val len7 = b1 and 0x7F
            val length = when (len7) {
                126 -> readExactly(2).fold(0L) { acc, b -> (acc shl 8) or (b.toLong() and 0xFF) }
                127 -> readExactly(8).fold(0L) { acc, b -> (acc shl 8) or (b.toLong() and 0xFF) }
                else -> len7.toLong()
            }
            val masked = b1 and 0x80 != 0
            val mask = if (masked) readExactly(4) else ByteArray(4)
            val payload = readExactly(length.toInt())
            for (i in payload.indices) payload[i] = (payload[i].toInt() xor mask[i and 3].toInt()).toByte()
            return Frame(b0 and 0x80 != 0, b0 and 0x0F, masked, len7, payload)
        }

        /** Чтение после конца: `-1` — FIN клиента, исключение `Connection reset` — RST, таймаут — соединение открыто. */
        fun readEnd(timeoutMs: Int): String {
            socket.soTimeout = timeoutMs
            return try {
                val b = input.read()
                if (b < 0) "EOF" else "DATA($b)"
            } catch (_: SocketTimeoutException) {
                "OPEN"
            } catch (e: SocketException) {
                "RESET(${e.message})"
            }
        }
    }

    private val toClose: MutableList<AutoCloseable> = java.util.Collections.synchronizedList(ArrayList())
    private val hosts = ArrayList<MqttHost>()

    @AfterTest
    fun tearDown() {
        runBlocking { hosts.forEach { it.close(5_000) } }
        synchronized(toClose) { toClose.toList() }.forEach { runCatching { it.close() } }
    }

    private fun server(): ServerSocket = ServerSocket(0, 50, InetAddress.getLoopbackAddress()).also { toClose += it }

    private fun startedHost(driver: TestDriver, settings: MqttWebSocket): MqttHost {
        val host = NioActorHost(
            nextHostThreadName(),
            MqttClock.Monotonic,
            NioTransportFactory.webSocket(settings),
            NioActorHost.DEFAULT_READ_BUFFER
        )
        hosts += host
        host.start(driver)
        assertTrue(driver.firstRun.await(5, TimeUnit.SECONDS), "первый runOnce")
        return host
    }

    /** Подключение до принятого сокета (без `Upgrade`): сервер, драйвер, хост, сторона сервера. */
    private fun accepted(
        settings: MqttWebSocket = MqttWebSocket(),
        timeoutMs: Long = 10_000,
        vararg extra: HostAction
    ): Triple<TestDriver, MqttHost, Peer> {
        val server = server()
        server.soTimeout = 10_000
        val driver = TestDriver()
        val host = startedHost(driver, settings)
        driver.send(host, HostAction.Connect("127.0.0.1", server.localPort, timeoutMs), *extra)
        val socket = server.accept()
        toClose += socket
        socket.soTimeout = 10_000
        socket.tcpNoDelay = true
        return Triple(driver, host, Peer(socket))
    }

    private fun opened(settings: MqttWebSocket = MqttWebSocket()): Triple<TestDriver, MqttHost, Peer> {
        val (driver, host, peer) = accepted(settings)
        peer.upgrade()
        assertEquals(TransportEvent.Connected, driver.awaitEvent())
        return Triple(driver, host, peer)
    }

    /** Кадр сервера (без маски, если не просили). */
    private fun frame(
        opcode: Int,
        payload: ByteArray,
        fin: Boolean = true,
        rsv: Int = 0,
        mask: Boolean = false
    ): ByteArray {
        val out = ByteArrayOutputStream()
        out.write((if (fin) 0x80 else 0) or (rsv shl 4) or opcode)
        val m = if (mask) 0x80 else 0
        when {
            payload.size <= 125 -> out.write(m or payload.size)
            payload.size <= 0xFFFF -> {
                out.write(m or 126)
                out.write(payload.size shr 8)
                out.write(payload.size and 0xFF)
            }
            else -> {
                out.write(m or 127)
                for (shift in 56 downTo 0 step 8) out.write(((payload.size.toLong() shr shift) and 0xFF).toInt())
            }
        }
        if (mask) out.write(byteArrayOf(1, 2, 3, 4))
        out.write(payload)
        return out.toByteArray()
    }

    private fun assertWebSocketFailure(event: TransportEvent, vararg forbidden: String): MqttError.Io {
        val failed = assertIs<TransportEvent.Failed>(event, "ожидалась ошибка: $event")
        val error = assertIs<MqttError.Io>(failed.error)
        assertEquals("websocket", error.kind)
        val text = "$error ${error.cause?.message}"
        for (secret in listOf("127.0.0.1", "localhost") + forbidden) assertFalse(secret in text, "ошибка раскрывает $secret: $text")
        return error
    }

    @Test
    fun upgradeRequestCarriesPathHeadersKeyAndSubprotocol() {
        val settings = MqttWebSocket("/mqtt/x?token=1", linkedMapOf("Authorization" to "Bearer abc", "X-Extra" to "1"))
        val (driver, _, peer) = accepted(settings)
        val request = peer.readRequest()
        assertTrue(request.startsWith("GET /mqtt/x?token=1 HTTP/1.1\r\n"), request)
        assertEquals("127.0.0.1:${peer.socket.localPort}", peer.header("Host"))
        assertEquals("websocket", peer.header("Upgrade"))
        assertEquals("Upgrade", peer.header("Connection"))
        assertEquals("13", peer.header("Sec-WebSocket-Version"))
        assertEquals("mqtt", peer.header("Sec-WebSocket-Protocol"))
        assertEquals("Bearer abc", peer.header("Authorization"))
        assertEquals("1", peer.header("X-Extra"))
        assertEquals(16, Base64.getDecoder().decode(peer.header("Sec-WebSocket-Key")).size)
        peer.write(peer.okResponse().toByteArray())
        assertEquals(TransportEvent.Connected, driver.awaitEvent())
    }

    @Test
    fun writesBeforeUpgradeWaitAndGoAsMaskedBinaryFrames() {
        val early = "early".encodeToByteArray()
        val (driver, host, peer) = accepted(MqttWebSocket(), 10_000, HostAction.Write(early))
        peer.upgrade()
        assertEquals(TransportEvent.Connected, driver.awaitEvent())
        val first = peer.readFrame()
        assertTrue(first.fin && first.masked, "кадр клиента: FIN и маска")
        assertEquals(0x2, first.opcode)
        assertContentEquals(early, first.payload)

        driver.send(host, HostAction.Write("later".encodeToByteArray()))
        assertContentEquals("later".encodeToByteArray(), peer.readFrame().payload)
        peer.write(frame(0x2, "from-server".encodeToByteArray()))
        assertContentEquals("from-server".encodeToByteArray(), driver.readBytes(11))
    }

    @Test
    fun responseAndFirstFrameInOneSegment() {
        val (driver, _, peer) = accepted()
        peer.upgrade(extraBytes = frame(0x2, byteArrayOf(1, 2, 3)) + frame(0x2, byteArrayOf(4)))
        assertEquals(TransportEvent.Connected, driver.awaitEvent())
        assertContentEquals(byteArrayOf(1, 2, 3, 4), driver.readBytes(4))
    }

    @Test
    fun lengthFormsBothDirections() {
        val (driver, host, peer) = opened()
        for ((size, form) in listOf(125 to 125, 126 to 126, 65_535 to 126, 65_536 to 127, 300_000 to 127)) {
            val payload = Random(size).nextBytes(size)
            driver.send(host, HostAction.Write(payload))
            val received = peer.readFrame()
            assertEquals(form, received.lengthForm, "форма длины для $size")
            assertContentEquals(payload, received.payload, "клиент → сервер, $size")

            peer.write(frame(0x2, payload))
            assertContentEquals(payload, driver.readBytes(size), "сервер → клиент, $size")
        }
    }

    @Test
    fun fragmentedMessageWithPingInsideAndSplitHeaders() {
        val (driver, _, peer) = opened()
        val wire = frame(0x2, "abc".encodeToByteArray(), fin = false) +
            frame(0x9, "p1".encodeToByteArray()) +
            frame(0x0, "def".encodeToByteArray(), fin = false) +
            frame(0xA, "unsolicited".encodeToByteArray()) +
            frame(0x0, "ghi".encodeToByteArray(), fin = true) +
            frame(0x2, "jk".encodeToByteArray())
        // По байту: заголовки кадров режутся между чтениями
        for (b in wire) {
            peer.write(byteArrayOf(b))
        }
        assertContentEquals("abcdefghijk".encodeToByteArray(), driver.readBytes(11))
        val pong = peer.readFrame()
        assertEquals(0xA, pong.opcode, "ответ на ping — pong")
        assertTrue(pong.masked)
        assertContentEquals("p1".encodeToByteArray(), pong.payload)
    }

    @Test
    fun protocolViolationsFail() {
        val cases = listOf(
            "маска у кадра сервера" to frame(0x2, byteArrayOf(1), mask = true),
            "текстовый кадр" to frame(0x1, "t".encodeToByteArray()),
            "бит RSV" to frame(0x2, byteArrayOf(1), rsv = 4),
            "продолжение без начала" to frame(0x0, byteArrayOf(1)),
            "новый кадр данных посреди фрагментированного" to
                frame(0x2, byteArrayOf(1), fin = false) + frame(0x2, byteArrayOf(2)),
            "фрагментированный ping" to frame(0x9, byteArrayOf(1), fin = false),
            "длинный ping" to frame(0x9, ByteArray(126)),
            "неизвестный opcode" to frame(0x3, byteArrayOf(1)),
            "64-битная длина со старшим битом" to byteArrayOf(0x82.toByte(), 127, -128, 0, 0, 0, 0, 0, 0, 1),
        )
        for ((name, bytes) in cases) {
            val (driver, host, peer) = opened()
            peer.write(bytes)
            var event = driver.awaitEvent()
            while (event is TransportEvent.Bytes) event = driver.awaitEvent()
            assertWebSocketFailure(event)
            assertEquals("EOF", peer.readEnd(5_000), "$name: соединение закрыто клиентом")
            runBlocking { host.close(5_000) }
        }
    }

    @Test
    fun refusedOrInvalidUpgradeFails() {
        val cases = listOf<Pair<String, (Peer) -> String>>(
            "статус 404" to { _ -> "HTTP/1.1 404 Not Found 127.0.0.1\r\nContent-Length: 0\r\n\r\n" },
            "неверный Accept" to { p -> p.okResponse(accept = "AAAA") },
            "другой подпротокол" to { p -> p.okResponse(protocol = "wamp") },
            "расширение" to { p -> p.okResponse(extra = "Sec-WebSocket-Extensions: permessage-deflate\r\n") },
            "без Upgrade" to { p -> p.okResponse().replace("Upgrade: websocket\r\n", "") },
            "не HTTP" to { _ -> "SSH-2.0-OpenSSH\r\n\r\n" },
        )
        for ((name, response) in cases) {
            val (driver, host, peer) = accepted()
            peer.readRequest()
            peer.write(response(peer).toByteArray(Charsets.US_ASCII))
            val error = assertWebSocketFailure(driver.awaitEvent(), "Not Found")
            if (name == "статус 404") assertTrue("404" in error.cause?.message.orEmpty(), "код статуса в ошибке")
            runBlocking { host.close(5_000) }
        }
    }

    @Test
    fun subprotocolMayBeOmittedByTheServer() {
        val (driver, _, peer) = accepted()
        peer.readRequest()
        peer.write(peer.okResponse(protocol = null).toByteArray())
        assertEquals(TransportEvent.Connected, driver.awaitEvent())
    }

    @Test
    fun oversizedResponseHeaderFails() {
        val (driver, _, peer) = accepted()
        peer.readRequest()
        peer.write(("HTTP/1.1 101 Switching Protocols\r\nX-Pad: " + "a".repeat(10_000)).toByteArray())
        assertWebSocketFailure(driver.awaitEvent())
    }

    @Test
    fun upgradeWithoutAnswerIsConnectTimeout() {
        val (driver, _, peer) = accepted(timeoutMs = 600)
        peer.readRequest()
        val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent(5_000))
        assertEquals(MqttError.ConnectTimeout, failed.error)
    }

    @Test
    fun eofDuringUpgradeFails() {
        val (driver, _, peer) = accepted()
        peer.readRequest()
        peer.socket.shutdownOutput()
        assertWebSocketFailure(driver.awaitEvent())
    }

    @Test
    fun serverCloseFrameIsAnsweredThenEofAndCloseWithoutReset() {
        val (driver, host, peer) = opened()
        peer.write(frame(0x2, byteArrayOf(7)) + frame(0x8, byteArrayOf(0x03, 0xE9.toByte(), 'b'.code.toByte())))
        assertContentEquals(byteArrayOf(7), driver.readBytes(1))
        assertEquals(TransportEvent.Eof, driver.awaitEvent())
        val reply = peer.readFrame()
        assertEquals(0x8, reply.opcode)
        assertTrue(reply.masked)
        assertContentEquals(byteArrayOf(0x03, 0xE9.toByte()), reply.payload, "ответ тем же кодом 1001")
        // Данные после close-кадра сервера не принимаются
        peer.write(frame(0x2, byteArrayOf(9)))
        Thread.sleep(200) // клиент дочитывает (и отбрасывает) кадр, иначе закрытие с непрочитанным дало бы RST
        driver.send(host, HostAction.CloseTransport)
        assertEquals("EOF", peer.readEnd(5_000), "закрытие без RST")
        assertTrue(driver.events.none { it is TransportEvent.Bytes }, "после close-кадра данных нет: ${driver.events}")
    }

    @Test
    fun flushAndCloseSendsCloseFrameLastAndWaitsForTheReply() {
        val (driver, host, peer) = opened()
        val payload = Random(5).nextBytes(200_000)
        driver.send(host, HostAction.Write(payload), HostAction.FlushAndClose(5_000))
        assertContentEquals(payload, peer.readFrame().payload)
        val close = peer.readFrame()
        assertEquals(0x8, close.opcode, "close-кадр — последним")
        assertContentEquals(byteArrayOf(0x03, 0xE8.toByte()), close.payload, "код 1000")
        // Пока ответа нет, клиент TCP не закрывает
        assertEquals("OPEN", peer.readEnd(400), "клиент ждёт ответного close-кадра")
        peer.socket.soTimeout = 10_000
        peer.write(frame(0x8, byteArrayOf(0x03, 0xE8.toByte())))
        assertEquals("EOF", peer.readEnd(5_000), "после ответа — FIN, не RST")
    }

    @Test
    fun flushAndCloseWithoutReplyEndsByTheDeadline() {
        val (driver, host, peer) = opened()
        val started = System.currentTimeMillis()
        driver.send(host, HostAction.FlushAndClose(500))
        assertEquals(0x8, peer.readFrame().opcode)
        assertEquals("EOF", peer.readEnd(5_000))
        val took = System.currentTimeMillis() - started
        assertTrue(took in 300..4_000, "закрытие по сроку дописывания: $took мс")
    }

    @Test
    fun flushAndCloseAfterServerEofSendsNoCloseFrame() {
        val (driver, host, peer) = opened()
        // Как старый встроенный брокер форка (до этого изменения): усечённый close-кадр из одного байта, затем FIN
        peer.write(byteArrayOf(0x88.toByte()))
        peer.socket.shutdownOutput()
        assertEquals(TransportEvent.Eof, driver.awaitEvent(), "усечённый кадр + EOF — это Eof, не ошибка")
        driver.send(host, HostAction.FlushAndClose(5_000))
        assertEquals("EOF", peer.readEnd(5_000), "без close-кадра и без RST")
    }

    @Test
    fun directCloseSendsCloseFrame() {
        val (driver, host, peer) = opened()
        driver.send(host, HostAction.CloseTransport)
        val close = peer.readFrame()
        assertEquals(0x8, close.opcode)
        assertContentEquals(byteArrayOf(0x03, 0xE8.toByte()), close.payload)
        assertEquals("EOF", peer.readEnd(5_000))
    }

    @Test
    fun resetIsAFailureOrEof() {
        val (driver, _, peer) = opened()
        peer.socket.setSoLinger(true, 0)
        peer.socket.close()
        val event = driver.awaitEvent()
        assertTrue(event is TransportEvent.Failed || event == TransportEvent.Eof, "обрыв: $event")
    }

    @Test
    fun pauseReadHoldsFramesUntilResumed() {
        val (driver, host, peer) = opened()
        driver.send(host, HostAction.PauseRead(true))
        Thread.sleep(100)
        peer.write(frame(0x2, byteArrayOf(1, 2)))
        assertEquals(null, driver.events.poll(300, TimeUnit.MILLISECONDS), "чтение приостановлено")
        driver.send(host, HostAction.PauseRead(false))
        assertContentEquals(byteArrayOf(1, 2), driver.readBytes(2))
    }

    @Test
    fun hostHeaderBracketsIpv6AndOmitsDefaultPorts() {
        assertEquals("example.org", WebSocketTransport.hostHeader("example.org", 80, secure = false))
        assertEquals("example.org:443", WebSocketTransport.hostHeader("example.org", 443, secure = false))
        assertEquals("example.org", WebSocketTransport.hostHeader("example.org", 443, secure = true))
        assertEquals("[::1]:8083", WebSocketTransport.hostHeader("::1", 8083, secure = false))
        assertEquals("[::1]:9001", WebSocketTransport.hostHeader("[::1]", 9001, secure = true))
        // RFC 6455 §1.3, пример ключа и ответа
        assertEquals("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", WebSocketTransport.acceptFor("dGhlIHNhbXBsZSBub25jZQ=="))
    }

    @Test
    fun settingsValidationAndToString() {
        val settings = MqttWebSocket("/mqtt?token=secret-token", mapOf("Authorization" to "Bearer secret-bearer"))
        val text = settings.toString()
        assertFalse("secret" in text, "toString раскрывает путь или заголовки: $text")
        assertEquals(settings, MqttWebSocket("/mqtt?token=secret-token", mapOf("Authorization" to "Bearer secret-bearer")))
        val invalid = listOf<() -> MqttWebSocket>(
            { MqttWebSocket("mqtt") },
            { MqttWebSocket("/a b") },
            { MqttWebSocket("/a\r\nX: y") },
            { MqttWebSocket("/пусть") },
            { MqttWebSocket(headers = mapOf("Host" to "x")) },
            { MqttWebSocket(headers = mapOf("sec-websocket-protocol" to "x")) },
            { MqttWebSocket(headers = mapOf("Bad Name" to "x")) },
            { MqttWebSocket(headers = mapOf("X-A" to "v\r\nInjected: 1")) },
            { MqttWebSocket(headers = mapOf("X-A" to "1", "x-a" to "2")) },
        )
        for (build in invalid) {
            val error = assertFailsWith<IllegalArgumentException> { build() }
            assertFalse("Injected" in error.message.orEmpty() || "пусть" in error.message.orEmpty(), error.message)
        }
        val options = MqttClientOptions(host = "h", port = 1)
        assertEquals(MqttTransportKind.Tcp, options.transport, "умолчание — TCP")
        assertEquals(null, options.webSocket)
    }

    private companion object {
        fun acceptFor(key: String): String = Base64.getEncoder().encodeToString(
            MessageDigest.getInstance("SHA-1").digest((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").toByteArray())
        )
    }
}
