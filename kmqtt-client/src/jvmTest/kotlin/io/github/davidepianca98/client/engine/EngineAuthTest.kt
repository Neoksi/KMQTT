package io.github.davidepianca98.client.engine

import io.github.davidepianca98.client.ConnectionState
import io.github.davidepianca98.client.MqttError
import io.github.davidepianca98.client.ReconnectPolicy
import io.github.davidepianca98.client.TransportEvent
import io.github.davidepianca98.client.core.BrokerScript
import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.core.SessionAuth
import io.github.davidepianca98.client.core.SessionSettings
import io.github.davidepianca98.mqtt.MQTTCurrentPacket
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.MQTTPacket
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Auth
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Connect
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Disconnect
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Properties
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Движок и расширенная аутентификация на виртуальном времени: вызов брокера → [EngineAction.AuthChallenge]
 * с номером подключения; ответ прошлому подключению не доходит до ядра нового.
 */
@OptIn(MqttCoreApi::class)
class EngineAuthTest {

    private val packets = BrokerScript(MQTTVersion.MQTT5) // только сборка пакетов брокера
    private var now = 0L
    private var epochs = 0L
    private var parser = MQTTCurrentPacket(1024u * 1024u, MQTTVersion.MQTT5)

    private val engine = MqttClientEngine(
        EngineOptions(
            host = "broker.example",
            port = 1883,
            session = SessionSettings(MQTTVersion.MQTT5, clientId = "c", keepAliveSec = 30, auth = SessionAuth(METHOD)),
            reconnect = ReconnectPolicy(initial = 1.seconds, jitter = 0.0),
        ),
        nextEpoch = { ++epochs },
    )

    private fun actions(): List<EngineAction> = engine.takeActions()

    private fun List<EngineAction>.written(): List<MQTTPacket> =
        filterIsInstance<EngineAction.Write>().flatMap { parser.addData(it.bytes.asUByteArray()) }

    /** Новый транспорт: Connect → Connected; возвращает отправленный CONNECT. */
    private fun connectTransport(): MQTT5Connect {
        parser = MQTTCurrentPacket(1024u * 1024u, MQTTVersion.MQTT5)
        engine.onTransport(TransportEvent.Connected, now)
        return assertIs<MQTT5Connect>(actions().written().single())
    }

    private fun brokerSends(bytes: ByteArray): List<EngineAction> {
        engine.onTransport(TransportEvent.Bytes(bytes), now)
        return actions()
    }

    private fun challenge(data: ByteArray? = null): EngineAction.AuthChallenge =
        assertIs<EngineAction.AuthChallenge>(
            brokerSends(packets.auth5(method = METHOD, data = data)).single { it is EngineAction.AuthChallenge },
        )

    private fun start() {
        engine.onCommand(EngineCommand.Start, now)
        assertTrue(actions().any { it is EngineAction.Connect })
    }

    @Test
    fun challengeResponseAndConnack() {
        start()
        assertEquals(METHOD, connectTransport().properties.authenticationMethod)
        val c = challenge(byteArrayOf(7))
        assertEquals(METHOD, c.method)
        assertContentEquals(byteArrayOf(7), c.data)
        engine.onCommand(EngineCommand.AuthResponse(c.connection, byteArrayOf(8)), now)
        val auth = assertIs<MQTT5Auth>(actions().written().single())
        assertContentEquals(byteArrayOf(8), auth.properties.authenticationData?.asByteArray())
        brokerSends(packets.connack(properties = MQTT5Properties(authenticationMethod = METHOD)))
        assertIs<ConnectionState.Connected>(engine.state)
    }

    @Test
    fun staleResponseNeverReachesTheNextConnection() {
        start()
        connectTransport()
        val old = challenge()
        // Обрыв, пока провайдер думает; переподключение
        engine.onTransport(TransportEvent.Eof, now)
        actions()
        assertIs<ConnectionState.Waiting>(engine.state)
        now += 1_000
        engine.onTick(now)
        assertTrue(actions().any { it is EngineAction.Connect })
        connectTransport()
        // Поздний ответ прошлому подключению — до вызова нового и после него
        engine.onCommand(EngineCommand.AuthResponse(old.connection, byteArrayOf(1)), now)
        assertTrue(actions().written().isEmpty())
        val fresh = challenge()
        assertTrue(fresh.connection != old.connection)
        engine.onCommand(EngineCommand.AuthResponse(old.connection, byteArrayOf(1)), now)
        assertTrue(actions().written().isEmpty(), "ответ прошлому подключению отброшен")
        engine.onCommand(EngineCommand.AuthResponse(fresh.connection, byteArrayOf(2)), now)
        val auth = assertIs<MQTT5Auth>(actions().written().single())
        assertContentEquals(byteArrayOf(2), auth.properties.authenticationData?.asByteArray())
    }

    @Test
    fun clientRefusalEndsTheAttemptWithRefused() {
        start()
        connectTransport()
        val c = challenge()
        engine.onCommand(EngineCommand.AuthResponse(c.connection, null, failed = true), now)
        val out = actions()
        assertEquals(ReasonCode.NOT_AUTHORIZED, assertIs<MQTT5Disconnect>(out.written().single()).reasonCode)
        assertTrue(out.any { it == EngineAction.CloseTransport })
        val waiting = assertIs<ConnectionState.Waiting>(engine.state)
        assertEquals(MqttError.Refused(ReasonCode.NOT_AUTHORIZED), waiting.lastError)
    }

    @Test
    fun actionsHideData() {
        val text = EngineAction.AuthChallenge(1, METHOD, "secret".encodeToByteArray()).toString() +
            EngineCommand.AuthResponse(1, "secret".encodeToByteArray()).toString()
        assertFalse(text.contains("secret"), text)
    }

    // ---- повторная аутентификация посреди сессии ----

    /** Подключение с успешным CONNACK (метод в свойствах обязателен для ядра с auth). */
    private fun connectedSession() {
        start()
        connectTransport()
        brokerSends(packets.connack(properties = MQTT5Properties(authenticationMethod = METHOD)))
        assertIs<ConnectionState.Connected>(engine.state)
    }

    @Test
    fun reauthenticationExchangeCompletes() {
        connectedSession()
        engine.onCommand(EngineCommand.Reauthenticate, now)
        val start = assertIs<MQTT5Auth>(actions().written().single())
        assertEquals(ReasonCode.RE_AUTHENTICATE, start.authenticateReasonCode)
        assertEquals(METHOD, start.properties.authenticationMethod)
        // Вызов брокера → AuthChallenge, ответ — в Connected (не только в Connecting, как при подключении)
        val c = challenge(byteArrayOf(3))
        engine.onCommand(EngineCommand.AuthResponse(c.connection, byteArrayOf(4)), now)
        val answer = assertIs<MQTT5Auth>(actions().written().single())
        assertEquals(ReasonCode.CONTINUE_AUTHENTICATION, answer.authenticateReasonCode)
        assertContentEquals(byteArrayOf(4), answer.properties.authenticationData?.asByteArray())
        val done = brokerSends(packets.auth5(code = ReasonCode.SUCCESS, method = METHOD))
        assertEquals(EngineAction.ReauthComplete(true), done.filterIsInstance<EngineAction.ReauthComplete>().single())
        assertIs<ConnectionState.Connected>(engine.state)
    }

    @Test
    fun reauthenticationWithoutSessionIsRefused() {
        engine.onCommand(EngineCommand.Reauthenticate, now)
        assertEquals(EngineAction.ReauthComplete(false), actions().single())
    }

    @Test
    fun secondReauthenticationIsRefusedWhileFirstRuns() {
        connectedSession()
        engine.onCommand(EngineCommand.Reauthenticate, now)
        actions()
        engine.onCommand(EngineCommand.Reauthenticate, now)
        assertEquals(EngineAction.ReauthComplete(false), actions().single())
        // Первый обмен завершается успехом — и снова можно
        val done = brokerSends(packets.auth5(code = ReasonCode.SUCCESS, method = METHOD))
        assertEquals(EngineAction.ReauthComplete(true), done.filterIsInstance<EngineAction.ReauthComplete>().single())
        engine.onCommand(EngineCommand.Reauthenticate, now)
        assertIs<MQTT5Auth>(actions().written().single())
    }

    @Test
    fun reauthenticationFailsWhenConnectionEnds() {
        connectedSession()
        engine.onCommand(EngineCommand.Reauthenticate, now)
        actions()
        engine.onTransport(TransportEvent.Eof, now)
        val out = actions()
        assertEquals(EngineAction.ReauthComplete(false), out.filterIsInstance<EngineAction.ReauthComplete>().single())
        assertIs<ConnectionState.Waiting>(engine.state)
    }

    private companion object {
        const val METHOD = "TEST-SCRAM"
    }
}
