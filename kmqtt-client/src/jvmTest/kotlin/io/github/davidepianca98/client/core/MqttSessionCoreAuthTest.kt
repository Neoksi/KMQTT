package io.github.davidepianca98.client.core

import io.github.davidepianca98.client.MqttError
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
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Расширенная аутентификация MQTT 5 в ядре (SEVEN-249): обмен AUTH на виртуальном времени [BrokerScript]. */
class MqttSessionCoreAuthTest {

    private val v5 = MQTTVersion.MQTT5
    private val secret = "client-first-secret".encodeToByteArray()

    private fun settings(
        auth: SessionAuth? = SessionAuth(METHOD) { secret },
        connackTimeoutMs: Long = 30_000,
    ) = SessionSettings(v5, clientId = "test-client", keepAliveSec = 10, connackTimeoutMs = connackTimeoutMs, auth = auth)

    private fun script(settings: SessionSettings = settings()) = BrokerScript(v5, settings, now = 0)

    private fun BrokerScript.connackOk(method: String? = METHOD, data: ByteArray? = null): ByteArray =
        connack(properties = MQTT5Properties(authenticationMethod = method, authenticationData = data?.asUByteArray()))

    private fun List<CoreOutput>.closed(): MqttError? = assertIs<CoreOutput.Closed>(single()).error

    private inline fun <reified T : MQTTPacket> List<MQTTPacket>.only(): T = assertIs<T>(single())

    private fun List<CoreOutput>.challenge(): CoreOutput.AuthChallenge = assertIs<CoreOutput.AuthChallenge>(single())

    @Test
    fun connectCarriesMethodAndInitialData() {
        val s = script()
        assertTrue(s.start().isEmpty())
        val connect = s.sent().only<MQTT5Connect>()
        assertEquals(METHOD, connect.properties.authenticationMethod)
        assertContentEquals(secret, connect.properties.authenticationData?.asByteArray())
    }

    @Test
    fun initialDataIsComputedForEveryConnection() {
        var calls = 0
        val auth = SessionAuth(METHOD) { byteArrayOf((++calls).toByte()) }
        repeat(2) { i ->
            val s = script(settings(auth))
            s.start()
            assertContentEquals(byteArrayOf((i + 1).toByte()), s.sent().only<MQTT5Connect>().properties.authenticationData?.asByteArray())
        }
        assertEquals(2, calls)
    }

    @Test
    fun noInitialDataWhenProviderReturnsNull() {
        val s = script(settings(SessionAuth(METHOD)))
        s.start()
        val connect = s.sent().only<MQTT5Connect>()
        assertEquals(METHOD, connect.properties.authenticationMethod)
        assertNull(connect.properties.authenticationData)
    }

    @Test
    fun initialDataExceptionRefusesWithoutConnect() {
        val s = script(settings(SessionAuth(METHOD) { throw IllegalStateException("secret text") }))
        assertEquals(MqttError.Refused(ReasonCode.NOT_AUTHORIZED), s.start().closed())
        assertTrue(s.sent().isEmpty(), "CONNECT не уходит")
        assertEquals(CoreState.Closed, s.core.state)
    }

    @Test
    fun singleStepConnackWithoutChallenge() {
        val s = script()
        s.start()
        s.sent()
        assertIs<CoreOutput.Connected>(s.send(s.connackOk()).single())
        assertEquals(CoreState.Connected, s.core.state)
    }

    @Test
    fun multiStepExchange() {
        val s = script()
        s.start()
        s.sent()
        // Шаг 1: вызов брокера → событие, ответ клиента → AUTH 0x18 с методом и данными
        val first = s.send(s.auth5(method = METHOD, data = byteArrayOf(1, 2))).challenge()
        assertEquals(METHOD, first.method)
        assertContentEquals(byteArrayOf(1, 2), first.data)
        assertTrue(s.sent().isEmpty(), "до ответа клиента ничего не уходит")
        assertTrue(s.core.authResponse(byteArrayOf(3), s.now).isEmpty())
        val answer1 = s.sent().only<MQTT5Auth>()
        assertEquals(ReasonCode.CONTINUE_AUTHENTICATION, answer1.authenticateReasonCode)
        assertEquals(METHOD, answer1.properties.authenticationMethod)
        assertContentEquals(byteArrayOf(3), answer1.properties.authenticationData?.asByteArray())
        // Шаг 2: вызов без данных, ответ без данных
        s.now = 100
        assertNull(s.send(s.auth5(method = METHOD)).challenge().data)
        s.core.authResponse(null, s.now)
        assertNull(s.sent().only<MQTT5Auth>().properties.authenticationData)
        // Завершение — CONNACK с тем же методом
        assertIs<CoreOutput.Connected>(s.send(s.connackOk(data = byteArrayOf(9))).single())
        assertEquals(CoreState.Connected, s.core.state)
    }

    @Test
    fun lateOrRepeatedResponseIsIgnored() {
        val s = script()
        s.start()
        s.sent()
        s.send(s.auth5(method = METHOD)).challenge()
        s.core.authResponse(byteArrayOf(1), s.now)
        s.sent().only<MQTT5Auth>()
        assertTrue(s.core.authResponse(byteArrayOf(2), s.now).isEmpty(), "повтор без вызова")
        assertTrue(s.core.authFailed(s.now).isEmpty(), "отказ без вызова")
        assertTrue(s.sent().isEmpty())
        s.send(s.connackOk())
        assertTrue(s.core.authResponse(byteArrayOf(3), s.now).isEmpty(), "после CONNACK")
        assertTrue(s.sent().isEmpty())
        assertEquals(CoreState.Connected, s.core.state)
    }

    @Test
    fun clientRefusalSendsDisconnectNotAuthorized() {
        val s = script()
        s.start()
        s.sent()
        s.send(s.auth5(method = METHOD)).challenge()
        assertEquals(MqttError.Refused(ReasonCode.NOT_AUTHORIZED), s.core.authFailed(s.now).closed())
        assertEquals(ReasonCode.NOT_AUTHORIZED, s.sent().only<MQTT5Disconnect>().reasonCode)
    }

    @Test
    fun brokerRefusalIsRefused() {
        for (code in listOf(ReasonCode.NOT_AUTHORIZED, ReasonCode.BAD_AUTHENTICATION_METHOD)) {
            val s = script()
            s.start()
            s.sent()
            s.send(s.auth5(method = METHOD)).challenge()
            s.core.authResponse(byteArrayOf(1), s.now)
            s.sent()
            assertEquals(MqttError.Refused(code), s.send(s.connack(code)).closed())
        }
    }

    @Test
    fun brokerSilenceIsConnackTimeout() {
        val s = script(settings(connackTimeoutMs = 5_000))
        s.start()
        s.sent()
        s.now = 1_000
        s.send(s.auth5(method = METHOD)).challenge()
        s.core.authResponse(byteArrayOf(1), s.now)
        s.sent()
        assertEquals(5_000, s.core.nextDeadline(), "срок всего обмена — от CONNECT")
        assertTrue(s.advanceTo(4_999).isEmpty())
        assertEquals(MqttError.ConnackTimeout, s.advanceTo(5_000).closed())
    }

    @Test
    fun clientSilenceIsRefusedNotAuthorized() {
        val s = script(settings(connackTimeoutMs = 5_000))
        s.start()
        s.sent()
        s.now = 1_000
        s.send(s.auth5(method = METHOD)).challenge()
        assertTrue(s.advanceTo(4_999).isEmpty())
        assertEquals(MqttError.Refused(ReasonCode.NOT_AUTHORIZED), s.advanceTo(5_000).closed())
        assertEquals(ReasonCode.NOT_AUTHORIZED, s.sent().only<MQTT5Disconnect>().reasonCode)
        assertTrue(s.core.authResponse(byteArrayOf(1), s.now).isEmpty(), "поздний ответ отброшен")
        assertTrue(s.sent().isEmpty())
    }

    @Test
    fun protocolViolations() {
        val cases: List<Pair<String, (BrokerScript) -> ByteArray>> = listOf(
            "другой метод" to { it.auth5(method = "OTHER") },
            "SUCCESS до CONNACK" to { it.auth5(code = ReasonCode.SUCCESS, method = METHOD) },
            "RE_AUTHENTICATE от брокера" to { it.auth5(code = ReasonCode.RE_AUTHENTICATE, method = METHOD) },
            "CONNACK с другим методом" to { it.connackOk(method = "OTHER") },
            "CONNACK без метода" to { it.connackOk(method = null) },
        )
        for ((name, packet) in cases) {
            val s = script()
            s.start()
            s.sent()
            assertEquals(MqttError.Protocol(ReasonCode.PROTOCOL_ERROR), s.send(packet(s)).closed(), name)
            assertEquals(ReasonCode.PROTOCOL_ERROR, s.sent().only<MQTT5Disconnect>().reasonCode, name)
        }
    }

    @Test
    fun secondChallengeBeforeAnswerIsProtocolError() {
        val s = script()
        s.start()
        s.sent()
        s.send(s.auth5(method = METHOD)).challenge()
        assertEquals(MqttError.Protocol(ReasonCode.PROTOCOL_ERROR), s.send(s.auth5(method = METHOD)).closed())
    }

    @Test
    fun authAfterConnackIsProtocolError() {
        val s = script()
        s.start()
        s.sent()
        s.send(s.connackOk())
        assertEquals(MqttError.Protocol(ReasonCode.PROTOCOL_ERROR), s.send(s.auth5(method = METHOD)).closed())
    }

    @Test
    fun authWithoutConfiguredMethodIsProtocolError() {
        val s = script(settings(auth = null))
        s.start()
        val connect = s.sent().only<MQTT5Connect>()
        assertNull(connect.properties.authenticationMethod)
        assertEquals(MqttError.Protocol(ReasonCode.PROTOCOL_ERROR), s.send(s.auth5(method = METHOD)).closed())
    }

    @Test
    fun mqtt311RejectsAuth() {
        assertFailsWith<IllegalArgumentException> {
            SessionSettings(MQTTVersion.MQTT3_1_1, clientId = "c", keepAliveSec = 10, auth = SessionAuth(METHOD))
        }
        // AUTH (тип 15) от брокера 3.1.1 — ошибка протокола, без DISCONNECT (он отменил бы will)
        val v4 = BrokerScript(MQTTVersion.MQTT3_1_1, now = 0)
        v4.start()
        v4.sent()
        assertEquals(MqttError.Protocol(ReasonCode.PROTOCOL_ERROR), v4.send(byteArrayOf(0xF0.toByte(), 0x00)).closed())
        assertTrue(v4.sent().isEmpty())
    }

    @Test
    fun toStringHidesData() {
        val challenge = CoreOutput.AuthChallenge(METHOD, secret)
        assertFalse(challenge.toString().contains("secret"))
        assertEquals("AuthChallenge(method=$METHOD, size=${secret.size})", challenge.toString())
        val text = settings().toString()
        assertTrue(text.contains("SessionAuth(method=$METHOD)"), text)
        assertFalse(text.contains("client-first"), text)
    }

    private companion object {
        const val METHOD = "TEST-SCRAM"
    }
}
