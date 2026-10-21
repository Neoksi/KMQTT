@file:OptIn(MqttCoreApi::class)

package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.hostcontract.ContractDriver
import io.github.davidepianca98.client.hostcontract.EndOfStream
import io.github.davidepianca98.client.hostcontract.PeerEnd
import io.github.davidepianca98.client.node.jsObject
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.socket.tls.TLSClientSettings
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.TestResult
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * WebSocket на JS — [NodeWebSocketTransport] (Node, свой разбор RFC 6455 над `net`/`tls`) и
 * [BrowserWebSocketTransport] / [BrowserMqttClient] (WebSocket API; здесь — глобальный `WebSocket` Node) против
 * [WsContractServer]: запрос `Upgrade`, отказ `Upgrade`, нарушения протокола, ping, фасад через фабрики, `wss://`.
 */
internal class JsWebSocketTransportTest {

    private fun wsTest(block: suspend CoroutineScope.() -> Unit): TestResult = runTest(timeout = 30.seconds) {
        withContext(Dispatchers.Default) { block() }
    }

    private suspend fun withHost(host: MqttHost, block: suspend (ContractDriver) -> Unit) {
        val driver = ContractDriver()
        try {
            host.start(driver)
            driver.awaitRun(5_000)
            block(driver)
        } finally {
            host.close(5_000)
        }
    }

    private fun nodeWsHost(settings: MqttWebSocket = MqttWebSocket()): MqttHost =
        NodeHost(MqttClock.Monotonic, NodeTransportFactory.webSocket(settings))

    private fun browserHost(): MqttHost = BrowserHost(MqttClock.Monotonic) { wakeup ->
        BrowserWebSocketTransport(wakeup, MqttWebSocket(), secure = false)
    }

    private fun options(port: Int, webSocket: MqttWebSocket?, tls: TLSClientSettings? = null) = MqttClientOptions(
        host = if (tls != null) "localhost" else "127.0.0.1",
        port = port,
        version = MQTTVersion.MQTT3_1_1,
        clientId = "ws-contract",
        keepAlive = 0.seconds,
        reconnect = ReconnectPolicy.Never,
        tls = tls,
        webSocket = webSocket,
    )

    private suspend fun connected(
        host: MqttHost,
        server: WsContractServer,
        block: suspend (ContractDriver, WsPeer) -> Unit
    ) = withHost(host) { driver ->
        driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
        val peer = server.accept()
        assertEquals(TransportEvent.Connected, driver.awaitEvent())
        block(driver, peer)
    }

    @Test
    fun nodeUpgradeRequestAndDataInBinaryFrames() = wsTest {
        val server = WsContractServer.listen()
        try {
            val settings = MqttWebSocket("/mqtt?token=t", mapOf("Authorization" to "Bearer x"))
            val host = nodeWsHost(settings)
            connected(host, server) { driver, peer ->
                val head = peer.requestHead.orEmpty()
                assertTrue(head.startsWith("GET /mqtt?token=t HTTP/1.1\r\n"), "путь запроса")
                assertTrue("\r\nHost: 127.0.0.1:${server.port}\r\n" in head, "Host")
                assertTrue("\r\nSec-WebSocket-Protocol: mqtt\r\n" in head, "подпротокол")
                assertTrue("\r\nSec-WebSocket-Version: 13\r\n" in head, "версия")
                assertTrue("\r\nAuthorization: Bearer x" in head, "дополнительный заголовок")
                driver.send(host, HostAction.Write("hello".encodeToByteArray()))
                assertContentEquals("hello".encodeToByteArray(), peer.readExactly(5))
                peer.write("world".encodeToByteArray())
                assertContentEquals("world".encodeToByteArray(), assertIs<TransportEvent.Bytes>(driver.awaitEvent()).bytes)
            }
        } finally {
            server.close()
        }
    }

    @Test
    fun nodeUpgradeRefusedIsWebSocketErrorWithStatusOnly() = wsTest {
        val server = WsContractServer.listen { "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n" }
        try {
            val host = nodeWsHost()
            withHost(host) { driver ->
                driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
                val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent())
                val io = assertIs<MqttError.Io>(failed.error)
                assertEquals("websocket", io.kind)
                assertTrue("404" in io.cause?.message.orEmpty(), "код статуса в ошибке")
                assertTrue("127.0.0.1" !in io.cause?.message.orEmpty(), "адреса в ошибке нет")
            }
        } finally {
            server.close()
        }
    }

    @Test
    fun nodeWrongAcceptIsWebSocketError() = wsTest {
        val server = WsContractServer.listen { WsContractServer.switchingProtocols("wrong-key") }
        try {
            val host = nodeWsHost()
            withHost(host) { driver ->
                driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
                val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent())
                assertEquals("websocket", assertIs<MqttError.Io>(failed.error).kind)
            }
        } finally {
            server.close()
        }
    }

    @Test
    fun nodeTextFrameIsProtocolError() = wsTest {
        val server = WsContractServer.listen()
        try {
            val host = nodeWsHost()
            connected(host, server) { driver, peer ->
                peer.writeFrame(WsPeer.OP_TEXT, "x".encodeToByteArray())
                val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent())
                assertEquals("websocket", assertIs<MqttError.Io>(failed.error).kind)
            }
        } finally {
            server.close()
        }
    }

    @Test
    fun nodePingIsAnsweredWithPongAndFragmentsAreJoined() = wsTest {
        val server = WsContractServer.listen()
        try {
            val host = nodeWsHost()
            connected(host, server) { driver, peer ->
                peer.writeFrame(WsPeer.OP_PING, byteArrayOf(1, 2, 3))
                // Фрагментированный бинарный кадр: начало без FIN, затем продолжение с FIN
                val first = WsPeer.frame(WsPeer.OP_BINARY, "ab".encodeToByteArray())
                first[0] = WsPeer.OP_BINARY.toByte()
                peer.writeRaw(first)
                peer.writeFrame(WsPeer.OP_CONTINUATION, "cd".encodeToByteArray())
                val received = StringBuilder()
                while (received.length < 4) {
                    received.append(assertIs<TransportEvent.Bytes>(driver.awaitEvent()).bytes.decodeToString())
                }
                assertEquals("abcd", received.toString())
                peer.readExactly(0)
                withTimeout(5_000) { while (peer.pongs.isEmpty()) delay(5) }
                assertContentEquals(byteArrayOf(1, 2, 3), peer.pongs.single())
            }
        } finally {
            server.close()
        }
    }

    /** FlushAndClose: данные, close-кадр 1000, ответ сервера, затем FIN. */
    @Test
    fun nodeFlushAndCloseSendsCloseFrameAfterData() = wsTest {
        val server = WsContractServer.listen()
        try {
            val host = nodeWsHost()
            connected(host, server) { driver, peer ->
                driver.send(host, HostAction.Write("last".encodeToByteArray()))
                driver.send(host, HostAction.FlushAndClose(5_000))
                assertContentEquals("last".encodeToByteArray(), peer.readExactly(4))
                assertEquals(EndOfStream(PeerEnd.Fin, 0), peer.awaitEnd())
                assertEquals(1000, peer.closeCodeReceived, "close-кадр 1000")
            }
        } finally {
            server.close()
        }
    }

    /** Фабрика JS `MqttClient(options)` с `webSocket` на Node: CONNECT…DISCONNECT в бинарных кадрах, close-кадр, FIN. */
    @Test
    fun facadeOverWebSocketWithJsFactory() = wsTest {
        val server = WsContractServer.listen()
        try {
            facadeRoundTrip(server, MqttClient(options(server.port, MqttWebSocket())))
        } finally {
            server.close()
        }
    }

    /** `wss://` на Node: WebSocket поверх `tls.TLSSocket` с закреплённым сертификатом. */
    @Test
    fun facadeOverSecureWebSocketWithJsFactory() = wsTest {
        val keys = jsObject()
        keys.key = NodeTlsTransportTest.SERVER_KEY
        keys.cert = NodeTlsTransportTest.SERVER_CERT
        val server = WsContractServer.listen(keys)
        try {
            val tls = TLSClientSettings(serverCertificate = NodeTlsTransportTest.SERVER_CERT)
            val client = MqttClient(options(server.port, MqttWebSocket(), tls))
            assertEquals(MqttTransportKind.SecureWebSocket, client.config.transport)
            facadeRoundTrip(server, client)
        } finally {
            server.close()
        }
    }

    /** [BrowserMqttClient] на глобальном `WebSocket` (здесь — Node): то же самое через WebSocket API. */
    @Test
    fun facadeOverBrowserClient() = wsTest {
        val server = WsContractServer.listen()
        try {
            facadeRoundTrip(server, BrowserMqttClient(options(server.port, MqttWebSocket())))
        } finally {
            server.close()
        }
    }

    private suspend fun CoroutineScope.facadeRoundTrip(server: WsContractServer, client: MqttClient) {
        try {
            val connecting = async { client.connect() }
            val peer = server.accept()
            assertEquals(0x10, peer.readExactly(1)[0].toInt() and 0xF0, "CONNECT")
            val length = peer.readExactly(1)[0].toInt()
            peer.readExactly(length)
            peer.write(byteArrayOf(0x20, 0x02, 0x00, 0x00))
            val connected = withTimeout(5_000) { connecting.await() }
            assertIs<ConnectResult.Connected>(connected, "CONNACK: $connected")
            val sent = withTimeout(5_000) { client.publish("t", byteArrayOf(1), Qos.AT_MOST_ONCE) }
            assertIs<PublishResult.Sent>(sent, "PUBLISH: $sent")
            val closing = async { client.close() }
            val publish = peer.readExactly(2)
            assertEquals(0x30, publish[0].toInt() and 0xF0, "PUBLISH")
            peer.readExactly(publish[1].toInt())
            assertContentEquals(byteArrayOf(0xE0.toByte(), 0x00), peer.readExactly(2), "DISCONNECT")
            val end = peer.awaitEnd()
            assertEquals(EndOfStream(PeerEnd.Fin, 0), end, "close-кадр клиента, ответ сервера, FIN")
            assertEquals(1000, peer.closeCodeReceived, "код close-кадра клиента")
            withTimeout(10_000) { closing.await() }
            assertEquals(ConnectionState.Closed(null), client.connection.value, "состояние после close()")
        } finally {
            withTimeout(5_000) { client.close() }
        }
    }

    @Test
    fun browserTextMessageIsProtocolError() = wsTest {
        val server = WsContractServer.listen()
        try {
            val host = browserHost()
            connected(host, server) { driver, peer ->
                peer.writeFrame(WsPeer.OP_TEXT, "x".encodeToByteArray())
                val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent())
                assertEquals("websocket", assertIs<MqttError.Io>(failed.error).kind)
            }
        } finally {
            server.close()
        }
    }

    @Test
    fun browserUpgradeRefusedIsWebSocketError() = wsTest {
        val server = WsContractServer.listen { "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n" }
        try {
            val host = browserHost()
            withHost(host) { driver ->
                driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
                val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent())
                assertEquals("websocket", assertIs<MqttError.Io>(failed.error).kind)
            }
        } finally {
            server.close()
        }
    }

    @Test
    fun browserClientRefusesWhatTheBrowserCannotDo() = runTest {
        val ws = MqttWebSocket()
        assertFailsWith<IllegalArgumentException>("без webSocket") { BrowserMqttClient(options(1883, null)) }
        assertFailsWith<IllegalArgumentException>("заголовки") {
            BrowserMqttClient(options(8083, MqttWebSocket(headers = mapOf("Authorization" to "x"))))
        }
        val refusedTls = listOf(
            TLSClientSettings(serverCertificate = "pem"),
            TLSClientSettings(clientCertificate = "pem", clientCertificateKey = "key"),
            TLSClientSettings(alpnProtocols = listOf("mqtt")),
            TLSClientSettings(serverNameIndications = "example.org"),
        )
        for (tls in refusedTls) {
            assertFailsWith<IllegalArgumentException>("TLS $tls") { BrowserMqttClient(options(8084, ws, tls)) }
        }
        assertFailsWith<IllegalArgumentException>("доверять всем") {
            BrowserMqttClient(
                options(8084, ws, TLSClientSettings(checkServerCertificate = false)).copy(verifyHostname = false)
            )
        }
        assertFailsWith<IllegalArgumentException>("без проверки имени") {
            BrowserMqttClient(options(8084, ws, TLSClientSettings()).copy(verifyHostname = false))
        }
        // По умолчанию — можно (wss:// с проверками браузера)
        BrowserMqttClient(options(8084, ws, TLSClientSettings())).close()
        BrowserMqttClient(options(8083, ws)).close()
    }
}
