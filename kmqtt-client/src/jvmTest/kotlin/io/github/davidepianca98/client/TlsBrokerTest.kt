package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.socket.tls.TLSClientSettings
import io.github.davidepianca98.socket.tls.TLSSettings
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.fail

/**
 * TLS-сценарий фасада против встроенного брокера форка с TLS (S06b-T05c): connect / subscribe / publish / incoming
 * через `NioActorHost` с `TlsTransport`. Ключ брокера — `tls/server.p12` (SAN только `localhost`), клиент закрепляет
 * сертификат `tls/server.pem`. Хост строится внутренним конструктором с `NioTransportFactory.tls(...)`: публичный
 * `NioActorHost()` создаёт только TCP.
 */
internal class TlsBrokerTest : BrokerTestBase() {

    private fun timed(limitMs: Long = 30_000, block: suspend CoroutineScope.() -> Unit) {
        runBlocking { withTimeout(limitMs, block) }
    }

    private val serverPem: String by lazy {
        (javaClass.classLoader.getResourceAsStream("tls/server.pem") ?: fail("нет ресурса tls/server.pem"))
            .use { it.readBytes().decodeToString() }.replace("\r\n", "\n")
    }

    /** Брокер читает хранилище по пути к файлу, поэтому ресурс копируется во временный файл. */
    private fun brokerTlsSettings(): TLSSettings {
        val file = File.createTempFile("kmqtt-test-server", ".p12")
        file.deleteOnExit()
        (javaClass.classLoader.getResourceAsStream("tls/server.p12") ?: fail("нет ресурса tls/server.p12"))
            .use { input -> file.outputStream().use { input.copyTo(it) } }
        return TLSSettings(keyStoreFilePath = file.absolutePath, keyStorePassword = "kmqtt-test")
    }

    private fun tlsClient(port: Int, host: String, verifyHostname: Boolean, settings: TLSClientSettings): MqttClient {
        val options = options(port) {
            it.copy(host = host, tls = settings, verifyHostname = verifyHostname)
        }
        val nioHost = NioActorHost(
            nextHostThreadName(),
            MqttClock.Monotonic,
            NioTransportFactory.tls(settings, verifyHostname),
            NioActorHost.DEFAULT_READ_BUFFER
        )
        return trackClient(MqttClient(options, nioHost))
    }

    @Test
    fun connectSubscribePublishAndCloseOverTls() = timed {
        val broker = broker(tlsSettings = brokerTlsSettings())
        val client = tlsClient(broker.port, "localhost", true, TLSClientSettings(serverCertificate = serverPem))
        val inbox = collect(client)
        val connected = withTimeout(15_000) { client.connect() }
        assertIs<ConnectResult.Connected>(connected, "подключение по TLS: $connected")

        client.subscriptions.set("g", setOf("tls/#"), Qos.AT_LEAST_ONCE)
        assertEquals(
            mapOf<String, TopicState>("tls/#" to TopicState.Granted(Qos.AT_LEAST_ONCE)),
            withTimeout(10_000) { client.subscriptions.awaitApplied("g") }
        )
        assertEquals(
            PublishResult.Delivered(connected.epoch),
            client.publish("tls/x", "secure".encodeToByteArray(), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        )
        BrokerTestKit.waitUntil(10_000, "сообщение по TLS") { inbox.messages.any { it.topic == "tls/x" } }
        assertEquals("secure", inbox.messages.single { it.topic == "tls/x" }.payload.decodeToString())

        val started = System.currentTimeMillis()
        withTimeout(15_000) { client.close() }
        println("TlsBrokerTest: close() по TLS занял ${System.currentTimeMillis() - started} мс")
        assertEquals(ConnectionState.Closed(null), client.connection.value)
    }

    /** Проверка имени хоста включена по умолчанию: подключение по IP к сертификату с SAN `localhost` отклоняется. */
    @Test
    fun hostnameMismatchIsRefusedAndTheErrorLeaksNoAddress() = timed {
        val broker = broker(tlsSettings = brokerTlsSettings())
        val strict = tlsClient(broker.port, TestBroker.HOST, true, TLSClientSettings(serverCertificate = serverPem))
        // reconnect по умолчанию бесконечен: ждём первую ошибку в состоянии
        strict.start()
        BrokerTestKit.waitUntil(10_000, "ошибка рукопожатия в состоянии клиента") {
            strict.connection.value is ConnectionState.Waiting
        }
        val state = strict.connection.value as ConnectionState.Waiting
        val text = state.toString()
        println("TlsBrokerTest: состояние после отказа TLS: $text")
        assertFalse(TestBroker.HOST in text || "localhost" in text, "ошибка TLS раскрывает адрес или имя: $text")
        assertIs<MqttError.Io>(state.lastError)
        assertEquals("ssl", (state.lastError as MqttError.Io).kind, "ошибка рукопожатия TLS, а не отказ подключения")
    }

    /** Без закрепления сертификата доверие — системное хранилище: самоподписанный сертификат брокера отклоняется. */
    @Test
    fun untrustedCertificateIsRefused() = timed {
        val broker = broker(tlsSettings = brokerTlsSettings())
        val client = tlsClient(broker.port, "localhost", true, TLSClientSettings())
        client.start()
        BrokerTestKit.waitUntil(10_000, "ошибка рукопожатия в состоянии клиента") {
            client.connection.value is ConnectionState.Waiting
        }
        val error = (client.connection.value as ConnectionState.Waiting).lastError
        assertIs<MqttError.Io>(error)
        assertEquals("ssl", error.kind, "ошибка рукопожатия TLS, а не отказ подключения")
    }
}
