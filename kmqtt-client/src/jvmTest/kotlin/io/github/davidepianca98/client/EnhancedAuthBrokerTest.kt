package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.broker.interfaces.EnhancedAuthenticationProvider
import io.github.davidepianca98.mqtt.broker.interfaces.EnhancedAuthenticationProvider.Result
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import io.github.davidepianca98.socket.tls.TLSClientSettings
import io.github.davidepianca98.socket.tls.TLSSettings
import kotlinx.coroutines.withTimeout
import java.io.File
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration.Companion.seconds

/**
 * SEVEN-249: расширенная аутентификация MQTT 5 (AUTH) фасада против встроенного брокера форка
 * (`Broker(enhancedAuthenticationProviders = …)`): многошаговый обмен, одношаговый, отказ брокера, неизвестный метод,
 * отказ и молчание провайдера клиента, повтор обмена после переподключения, тот же обмен по WebSocket, 3.1.1.
 */
internal class EnhancedAuthBrokerTest : BrokerTestBase() {

    private fun timed(limitMs: Long = 40_000, block: suspend CoroutineScope.() -> Unit) {
        runBlocking { withTimeout(limitMs, block) }
    }

    /** Провайдер брокера: CONNECT(hello-N) → вызов challenge-1 → answer-1 → challenge-2 → answer-2 → успех. */
    private class ScriptedProvider(private val steps: Int = 2, private val final: Result = Result.SUCCESS) :
        EnhancedAuthenticationProvider {
        val received = CopyOnWriteArrayList<String?>()
        private var step = 0

        override fun authReceived(
            clientId: String,
            authenticationData: UByteArray?,
            result: (completed: Result, authenticationData: UByteArray?) -> Unit,
        ) {
            received += authenticationData?.asByteArray()?.decodeToString()
            if (authenticationData?.asByteArray()?.decodeToString()?.startsWith("hello") == true) step = 0
            if (step < steps) {
                step++
                result(Result.NEEDS_MORE, "challenge-$step".encodeToByteArray().asUByteArray())
            } else {
                result(final, null)
            }
        }
    }

    /** Клиентская сторона: hello-N на каждое подключение, на challenge-K отвечает answer-K. */
    private class ClientSide {
        val connects = AtomicInteger()
        val challenges = CopyOnWriteArrayList<String?>()

        fun auth(method: String = METHOD) = MqttEnhancedAuth(
            method = method,
            initialData = { "hello-${connects.incrementAndGet()}".encodeToByteArray() },
            respond = { m, data ->
                assertEquals(method, m)
                val text = data?.decodeToString()
                challenges += text
                text?.replace("challenge", "answer")?.encodeToByteArray()
            },
        )
    }

    private fun authOptions(port: Int, auth: MqttEnhancedAuth, never: Boolean = false, ws: Boolean = false) =
        options(port) {
            it.copy(
                enhancedAuth = auth,
                reconnect = if (never) ReconnectPolicy.Never else it.reconnect,
                webSocket = if (ws) MqttWebSocket() else null,
            )
        }

    @Test
    fun multiStepExchangeAndRepeatAfterReconnect() = timed {
        val provider = ScriptedProvider()
        val broker = broker(enhancedAuth = mapOf(METHOD to provider))
        val proxy = proxy(broker.port)
        val side = ClientSide()
        val client = trackClient(MqttClient(authOptions(proxy.port, side.auth())))
        val inbox = collect(client)
        val connected = assertIs<ConnectResult.Connected>(withTimeout(15_000) { client.connect() })
        assertEquals(listOf<String?>("hello-1", "answer-1", "answer-2"), provider.received.toList())
        assertEquals(listOf<String?>("challenge-1", "challenge-2"), side.challenges.toList())

        // Сессия после обмена — обычная
        client.subscriptions.set("g", setOf("auth/#"), Qos.AT_LEAST_ONCE)
        withTimeout(10_000) { client.subscriptions.awaitApplied("g") }
        assertEquals(
            PublishResult.Delivered(connected.epoch),
            client.publish("auth/x", "1".encodeToByteArray(), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered),
        )
        BrokerTestKit.waitUntil(10_000, "сообщение") { inbox.messages.any { it.topic == "auth/x" } }

        // Обрыв → новый обмен со свежими начальными данными
        proxy.cut()
        BrokerTestKit.waitUntil(10_000, "обрыв замечен") { client.connection.value !is ConnectionState.Connected }
        proxy.restore()
        BrokerTestKit.waitUntil(15_000, "переподключение") {
            client.connection.value is ConnectionState.Connected && proxy.accepted >= 2
        }
        assertEquals(2, side.connects.get())
        assertEquals(listOf<String?>("hello-1", "answer-1", "answer-2", "hello-2", "answer-1", "answer-2"), provider.received.toList())
    }

    @Test
    fun singleStepWithoutChallenge() = timed {
        val provider = ScriptedProvider(steps = 0)
        val broker = broker(enhancedAuth = mapOf(METHOD to provider))
        val side = ClientSide()
        val client = trackClient(MqttClient(authOptions(broker.port, side.auth(), never = true)))
        assertIs<ConnectResult.Connected>(withTimeout(15_000) { client.connect() })
        assertEquals(listOf<String?>("hello-1"), provider.received.toList())
        assertTrue(side.challenges.isEmpty())
    }

    @Test
    fun brokerRefusalIsRefusedNotAuthorized() = timed {
        val broker = broker(enhancedAuth = mapOf(METHOD to ScriptedProvider(steps = 1, final = Result.ERROR)))
        val client = trackClient(MqttClient(authOptions(broker.port, ClientSide().auth(), never = true)))
        val failed = assertIs<ConnectResult.Failed>(withTimeout(15_000) { client.connect() })
        assertEquals(MqttError.Refused(ReasonCode.NOT_AUTHORIZED), failed.error)
    }

    @Test
    fun unknownMethodIsBadAuthenticationMethod() = timed {
        val broker = broker(enhancedAuth = mapOf(METHOD to ScriptedProvider()))
        val client = trackClient(MqttClient(authOptions(broker.port, ClientSide().auth("OTHER-METHOD"), never = true)))
        val failed = assertIs<ConnectResult.Failed>(withTimeout(15_000) { client.connect() })
        assertEquals(MqttError.Refused(ReasonCode.BAD_AUTHENTICATION_METHOD), failed.error)
    }

    @Test
    fun providerExceptionIsRefusedWithoutLeakingItsText() = timed {
        val broker = broker(enhancedAuth = mapOf(METHOD to ScriptedProvider()))
        val auth = MqttEnhancedAuth(METHOD, respond = { _, _ -> throw IllegalStateException(SECRET) })
        val client = trackClient(MqttClient(authOptions(broker.port, auth, never = true)))
        val failed = assertIs<ConnectResult.Failed>(withTimeout(15_000) { client.connect() })
        assertEquals(MqttError.Refused(ReasonCode.NOT_AUTHORIZED), failed.error)
        assertFalse(client.connection.value.toString().contains(SECRET))
        assertFalse(failed.toString().contains(SECRET))
    }

    @Test
    fun slowProviderIsRefusedAtTheConnackDeadline() = timed {
        val broker = broker(enhancedAuth = mapOf(METHOD to ScriptedProvider()))
        val auth = MqttEnhancedAuth(METHOD, respond = { _, _ ->
            delay(30_000)
            byteArrayOf()
        })
        val options = authOptions(broker.port, auth, never = true).copy(connackTimeout = 1.seconds)
        val client = trackClient(MqttClient(options))
        val started = System.currentTimeMillis()
        val failed = assertIs<ConnectResult.Failed>(withTimeout(15_000) { client.connect() })
        assertEquals(MqttError.Refused(ReasonCode.NOT_AUTHORIZED), failed.error)
        assertTrue(System.currentTimeMillis() - started < 10_000, "не ждали провайдера")
    }

    @Test
    fun sameExchangeOverWebSocket() = timed {
        val provider = ScriptedProvider()
        val broker = broker(webSocket = true, enhancedAuth = mapOf(METHOD to provider))
        val side = ClientSide()
        val client = trackClient(MqttClient(authOptions(broker.webSocketPort!!, side.auth(), never = true, ws = true)))
        assertEquals(MqttTransportKind.WebSocket, client.config.transport)
        assertIs<ConnectResult.Connected>(withTimeout(15_000) { client.connect() })
        assertEquals(listOf<String?>("hello-1", "answer-1", "answer-2"), provider.received.toList())
        assertEquals(listOf<String?>("challenge-1", "challenge-2"), side.challenges.toList())
    }

    @Test
    fun sameExchangeOverTls() = timed {
        val provider = ScriptedProvider()
        val broker = broker(tlsSettings = brokerTlsSettings(), enhancedAuth = mapOf(METHOD to provider))
        val side = ClientSide()
        val options = authOptions(broker.port, side.auth(), never = true)
            .copy(host = "localhost", tls = TLSClientSettings(serverCertificate = serverPem))
        val client = trackClient(MqttClient(options))
        assertEquals(MqttTransportKind.Tls, client.config.transport)
        assertIs<ConnectResult.Connected>(withTimeout(15_000) { client.connect() })
        assertEquals(listOf<String?>("hello-1", "answer-1", "answer-2"), provider.received.toList())
        assertEquals(listOf<String?>("challenge-1", "challenge-2"), side.challenges.toList())
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

    @Test
    fun optionsRejectMqtt311AndHideSecrets() {
        val auth = MqttEnhancedAuth(METHOD, initialData = { SECRET.encodeToByteArray() }, respond = { _, _ -> null })
        assertFailsWith<IllegalArgumentException> {
            MqttClientOptions("127.0.0.1", 1883, version = MQTTVersion.MQTT3_1_1, enhancedAuth = auth)
        }
        assertFailsWith<IllegalArgumentException> { MqttEnhancedAuth("", respond = { _, _ -> null }) }
        val text = MqttClientOptions("127.0.0.1", 1883, enhancedAuth = auth).toString()
        assertTrue(text.contains("enhancedAuth=MqttEnhancedAuth(method=$METHOD)"), text)
        assertFalse(text.contains(SECRET), text)
    }

    private companion object {
        const val METHOD = "TEST-SCRAM"
        const val SECRET = "top-secret-token"
    }
}
