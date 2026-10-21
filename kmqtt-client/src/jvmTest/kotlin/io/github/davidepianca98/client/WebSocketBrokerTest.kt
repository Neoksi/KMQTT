package io.github.davidepianca98.client

import io.github.davidepianca98.client.internal.MqttClientImpl
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.socket.tls.TLSClientSettings
import io.github.davidepianca98.socket.tls.TLSSettings
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.File
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * фасад `MqttClient(options)` с `webSocket` против встроенного брокера форка с WebSocket-портом
 * (`Broker(webSocketPort = …)`; с `tlsSettings` тот же порт — `wss`). Обмен, большие кадры (> 64 КиБ — 64-битная
 * длина в обе стороны), штатное закрытие, обрыв через [TcpProxy] и переподключение, `wss` с закреплённым сертификатом,
 * неверное имя хоста, отказ общей фабрики.
 */
internal class WebSocketBrokerTest : BrokerTestBase() {

    private fun timed(limitMs: Long = 40_000, block: suspend CoroutineScope.() -> Unit) {
        runBlocking { withTimeout(limitMs, block) }
    }

    private val serverPem: String by lazy {
        (javaClass.classLoader.getResourceAsStream("tls/server.pem") ?: fail("нет ресурса tls/server.pem"))
            .use { it.readBytes().decodeToString() }.replace("\r\n", "\n")
    }

    private fun brokerTlsSettings(): TLSSettings {
        val file = File.createTempFile("kmqtt-test-server", ".p12")
        file.deleteOnExit()
        (javaClass.classLoader.getResourceAsStream("tls/server.p12") ?: fail("нет ресурса tls/server.p12"))
            .use { input -> file.outputStream().use { input.copyTo(it) } }
        return TLSSettings(keyStoreFilePath = file.absolutePath, keyStorePassword = "kmqtt-test")
    }

    private fun wsClient(port: Int, tune: (MqttClientOptions) -> MqttClientOptions = { it }): MqttClient =
        trackClient(MqttClient(options(port) { tune(it.copy(webSocket = MqttWebSocket())) }))

    @Test
    fun exchangeWithLargeFramesAndOrderlyClose() = timed {
        val broker = broker(maximumPacketSize = TestBroker.LARGE_BUFFER, webSocket = true)
        val client = wsClient(broker.webSocketPort!!)
        assertEquals(MqttTransportKind.WebSocket, client.config.transport)
        val inbox = collect(client)
        val connected = assertIs<ConnectResult.Connected>(withTimeout(15_000) { client.connect() })

        client.subscriptions.set("g", setOf("ws/#"), Qos.AT_LEAST_ONCE)
        assertEquals(
            mapOf<String, TopicState>("ws/#" to TopicState.Granted(Qos.AT_LEAST_ONCE)),
            withTimeout(10_000) { client.subscriptions.awaitApplied("g") }
        )
        // 10 байт — 7-битная длина кадра, 1000 — 16-битная, 200 КиБ — 64-битная (и в ответе брокера тоже)
        val payloads = mapOf(
            "ws/small" to "hello".encodeToByteArray(),
            "ws/medium" to Random(1).nextBytes(1_000),
            "ws/large" to Random(2).nextBytes(200 * 1024),
        )
        for ((topic, payload) in payloads) {
            assertEquals(
                PublishResult.Delivered(connected.epoch),
                client.publish(topic, payload, Qos.AT_LEAST_ONCE, until = PublishStage.Delivered),
                topic
            )
        }
        BrokerTestKit.waitUntil(15_000, { "входящие: ${inbox.messages.map { it.topic }}" }) {
            inbox.messages.size >= payloads.size
        }
        for ((topic, payload) in payloads) {
            assertContentEquals(payload, inbox.messages.single { it.topic == topic }.payload, topic)
        }

        withTimeout(15_000) { client.close() }
        assertEquals(ConnectionState.Closed(null), client.connection.value)
    }

    @Test
    fun dropIsAnErrorAndTheClientReconnects() = timed {
        val broker = broker(webSocket = true)
        val proxy = proxy(broker.webSocketPort!!)
        val client = wsClient(proxy.port)
        val inbox = collect(client)
        assertIs<ConnectResult.Connected>(withTimeout(15_000) { client.connect() })
        client.subscriptions.set("g", setOf("drop/#"), Qos.AT_LEAST_ONCE)
        withTimeout(10_000) { client.subscriptions.awaitApplied("g") }

        proxy.cut()
        BrokerTestKit.waitUntil(10_000, "обрыв замечен") { client.connection.value !is ConnectionState.Connected }
        proxy.restore()
        BrokerTestKit.waitUntil(15_000, "переподключение") {
            client.connection.value is ConnectionState.Connected && proxy.accepted >= 2
        }
        withTimeout(10_000) { client.subscriptions.awaitApplied("g") }
        val epoch = (client.connection.value as ConnectionState.Connected).epoch
        assertEquals(
            PublishResult.Delivered(epoch),
            client.publish("drop/x", "after".encodeToByteArray(), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        )
        BrokerTestKit.waitUntil(10_000, "сообщение после переподключения") { inbox.messages.any { it.topic == "drop/x" } }
        val ended = inbox.items.filterIsInstance<MqttIncoming.SessionEnded>()
        assertTrue(ended.isNotEmpty() && ended.first().error != null, "первая сессия кончилась ошибкой: $ended")
    }

    @Test
    fun secureWebSocketWithPinnedCertificate() = timed {
        val broker = broker(tlsSettings = brokerTlsSettings(), webSocket = true)
        val client = wsClient(broker.webSocketPort!!) {
            it.copy(host = "localhost", tls = TLSClientSettings(serverCertificate = serverPem))
        }
        assertEquals(MqttTransportKind.SecureWebSocket, client.config.transport)
        assertTrue(client.config.verifyHostname)
        val inbox = collect(client)
        val connected = assertIs<ConnectResult.Connected>(withTimeout(15_000) { client.connect() })
        client.subscriptions.set("g", setOf("wss/#"), Qos.AT_LEAST_ONCE)
        withTimeout(10_000) { client.subscriptions.awaitApplied("g") }
        assertEquals(
            PublishResult.Delivered(connected.epoch),
            client.publish("wss/x", "secure".encodeToByteArray(), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        )
        BrokerTestKit.waitUntil(10_000, "сообщение по wss") { inbox.messages.any { it.topic == "wss/x" } }
        assertEquals("secure", inbox.messages.single { it.topic == "wss/x" }.payload.decodeToString())
        withTimeout(15_000) { client.close() }
        assertEquals(ConnectionState.Closed(null), client.connection.value)
    }

    /** Проверка имени хоста у `wss` включена по умолчанию: подключение по IP к сертификату с SAN `localhost` — отказ. */
    @Test
    fun secureWebSocketHostnameMismatchIsRefusedAndLeaksNoAddress() = timed {
        val broker = broker(tlsSettings = brokerTlsSettings(), webSocket = true)
        val client = wsClient(broker.webSocketPort!!) { it.copy(tls = TLSClientSettings(serverCertificate = serverPem)) }
        client.start()
        BrokerTestKit.waitUntil(10_000, "ошибка рукопожатия в состоянии клиента") {
            client.connection.value is ConnectionState.Waiting
        }
        val state = client.connection.value as ConnectionState.Waiting
        val text = state.toString()
        assertFalse(TestBroker.HOST in text || "localhost" in text, "ошибка раскрывает адрес или имя: $text")
        val error = assertIs<MqttError.Io>(state.lastError)
        assertEquals("ssl-hostname", error.kind, "ошибка TLS проходит сквозь WebSocket как есть")
    }

    /** Обычный MQTT-клиент к WebSocket-клиенту не приводит: общая фабрика с `webSocket` отказывает при создании. */
    @Test
    fun commonFactoryRefusesWebSocket() {
        val options = options(1883) { it.copy(webSocket = MqttWebSocket("/mqtt", mapOf("Authorization" to "Bearer t0ken"))) }
        val error = assertFailsWith<IllegalArgumentException> { MqttClient(options, NioActorHost()) }
        assertEquals(MqttClientImpl.WEBSOCKET_HOST_REQUIRED, error.message)
        val text = options.toString()
        assertFalse("t0ken" in text || "/mqtt" in text, "toString раскрывает путь или заголовки: $text")
    }
}
