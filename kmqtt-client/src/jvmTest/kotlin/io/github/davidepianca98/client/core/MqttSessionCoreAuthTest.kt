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

/** Расширенная аутентификация MQTT 5 в ядре: обмен AUTH на виртуальном времени [BrokerScript]. */
class MqttSessionCoreAuthTest {

    private val v5 = MQTTVersion.MQTT5
    private val secret = "client-first-secret".encodeToByteArray()

    private fun settings(
        auth: SessionAuth? = SessionAuth(METHOD) { secret },
        connackTimeoutMs: Long = 30_000,
        keepAliveSec: Int = 10,
    ) = SessionSettings(v5, clientId = "test-client", keepAliveSec = keepAliveSec, connackTimeoutMs = connackTimeoutMs, auth = auth)

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

    /** данные успешного CONNACK проверяются до объявления подключения; не прошли - отказ клиента. */
    @Test
    fun connackDataIsVerifiedBeforeConnected() {
        val seen = ArrayList<List<Byte>?>()
        val auth = SessionAuth(METHOD, verifyConnack = { seen += it?.toList(); it?.contentEquals(byteArrayOf(9)) == true })
        val ok = script(settings(auth))
        ok.start()
        ok.sent()
        assertIs<CoreOutput.Connected>(ok.send(ok.connackOk(data = byteArrayOf(9))).single())
        assertEquals(listOf<List<Byte>?>(listOf(9.toByte())), seen)

        for (data in listOf(byteArrayOf(8), null)) {
            val s = script(settings(auth))
            s.start()
            s.sent()
            assertEquals(MqttError.Refused(ReasonCode.NOT_AUTHORIZED), s.send(s.connackOk(data = data)).closed())
            assertEquals(ReasonCode.NOT_AUTHORIZED, s.sent().only<MQTT5Disconnect>().reasonCode)
            assertEquals(CoreState.Closed, s.core.state)
        }
    }

    @Test
    fun connackVerifierExceptionRefusesWithoutLeakingItsText() {
        val s = script(settings(SessionAuth(METHOD, verifyConnack = { throw IllegalStateException("secret text") })))
        s.start()
        s.sent()
        val error = s.send(s.connackOk()).closed()
        assertEquals(MqttError.Refused(ReasonCode.NOT_AUTHORIZED), error)
        assertFalse("secret text" in error.toString())
        assertEquals(ReasonCode.NOT_AUTHORIZED, s.sent().only<MQTT5Disconnect>().reasonCode)
    }

    @Test
    fun connackWithoutVerifierIgnoresItsData() {
        val s = script()
        s.start()
        s.sent()
        assertIs<CoreOutput.Connected>(s.send(s.connackOk(data = byteArrayOf(1))).single())
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

    // ---- повторная аутентификация посреди сессии ----

    @Test
    fun reauthenticationExchange() {
        var initialCalls = 0
        val s = script(settings(SessionAuth(METHOD) { byteArrayOf((++initialCalls).toByte()) }))
        s.connect(s.connackOk())
        assertEquals(CoreState.Connected, s.core.state)
        // Начало — AUTH 0x19 с методом и свежими данными initialData (второй вызов: первый был для CONNECT)
        assertTrue(s.core.reauthenticate(s.now).isEmpty())
        val start = s.sent().only<MQTT5Auth>()
        assertEquals(ReasonCode.RE_AUTHENTICATE, start.authenticateReasonCode)
        assertEquals(METHOD, start.properties.authenticationMethod)
        assertContentEquals(byteArrayOf(2), start.properties.authenticationData?.asByteArray())
        // Вызов брокера → событие → ответ клиента → AUTH 0x18
        val challenge = s.send(s.auth5(method = METHOD, data = byteArrayOf(1, 2))).challenge()
        assertContentEquals(byteArrayOf(1, 2), challenge.data)
        assertTrue(s.core.authResponse(byteArrayOf(3), s.now).isEmpty())
        val answer = s.sent().only<MQTT5Auth>()
        assertEquals(ReasonCode.CONTINUE_AUTHENTICATION, answer.authenticateReasonCode)
        assertContentEquals(byteArrayOf(3), answer.properties.authenticationData?.asByteArray())
        // Конец — AUTH 0x00 с тем же методом; сессия продолжается
        assertIs<CoreOutput.Reauthenticated>(s.send(s.auth5(code = ReasonCode.SUCCESS, method = METHOD)).single())
        assertEquals(CoreState.Connected, s.core.state)
        assertEquals(2, initialCalls)
        // Обмен можно начать снова
        assertTrue(s.core.reauthenticate(s.now).isEmpty())
        assertEquals(ReasonCode.RE_AUTHENTICATE, s.sent().only<MQTT5Auth>().authenticateReasonCode)
        assertEquals(3, initialCalls)
    }

    @Test
    fun reauthenticationWithoutChallengeSucceedsAtOnce() {
        val s = script()
        s.connect(s.connackOk())
        s.core.reauthenticate(s.now)
        s.sent().only<MQTT5Auth>()
        assertIs<CoreOutput.Reauthenticated>(s.send(s.auth5(code = ReasonCode.SUCCESS, method = METHOD)).single())
        assertEquals(CoreState.Connected, s.core.state)
    }

    /** Данные успешного AUTH 0x00 проверяет та же verifyConnack, что данные CONNACK. */
    @Test
    fun reauthenticationSuccessDataIsVerified() {
        val seen = ArrayList<List<Byte>?>()
        val auth = SessionAuth(METHOD, verifyConnack = { seen += it?.toList(); it?.contentEquals(byteArrayOf(9)) == true })
        val s = script(settings(auth))
        s.connect(s.connackOk(data = byteArrayOf(9)))
        seen.clear()
        s.core.reauthenticate(s.now)
        s.sent()
        val success = s.send(s.auth5(code = ReasonCode.SUCCESS, method = METHOD, data = byteArrayOf(9))).single()
        assertIs<CoreOutput.Reauthenticated>(success)
        assertEquals(listOf<List<Byte>?>(listOf(9.toByte())), seen)
        // Неверные данные — отказ клиента с DISCONNECT 0x87, как на CONNACK
        val bad = script(settings(auth))
        bad.connect(bad.connackOk(data = byteArrayOf(9)))
        bad.core.reauthenticate(bad.now)
        bad.sent()
        val refused = bad.send(bad.auth5(code = ReasonCode.SUCCESS, method = METHOD, data = byteArrayOf(8))).closed()
        assertEquals(MqttError.Refused(ReasonCode.NOT_AUTHORIZED), refused)
        assertEquals(ReasonCode.NOT_AUTHORIZED, bad.sent().only<MQTT5Disconnect>().reasonCode)
        assertEquals(CoreState.Closed, bad.core.state)
    }

    @Test
    fun reauthenticationBrokerSilenceIsConnackTimeout() {
        val s = script(settings(connackTimeoutMs = 5_000, keepAliveSec = 0))
        s.connect(s.connackOk())
        s.now = 10_000
        s.core.reauthenticate(s.now)
        s.sent()
        assertEquals(15_000, s.core.nextDeadline(), "срок обмена — от вызова reauthenticate")
        assertTrue(s.advanceTo(14_999).isEmpty())
        assertEquals(MqttError.ConnackTimeout, s.advanceTo(15_000).closed())
    }

    @Test
    fun reauthenticationClientSilenceIsRefusedNotAuthorized() {
        val s = script(settings(connackTimeoutMs = 5_000, keepAliveSec = 0))
        s.connect(s.connackOk())
        s.now = 10_000
        s.core.reauthenticate(s.now)
        s.sent()
        s.send(s.auth5(method = METHOD)).challenge()
        assertTrue(s.advanceTo(14_999).isEmpty())
        assertEquals(MqttError.Refused(ReasonCode.NOT_AUTHORIZED), s.advanceTo(15_000).closed())
        assertEquals(ReasonCode.NOT_AUTHORIZED, s.sent().only<MQTT5Disconnect>().reasonCode)
        assertTrue(s.core.authResponse(byteArrayOf(1), s.now).isEmpty(), "поздний ответ отброшен")
        assertTrue(s.sent().isEmpty())
    }

    @Test
    fun reauthenticationNeedsLiveSessionAndNoActiveExchange() {
        // До CONNACK
        val early = script()
        early.start()
        early.sent()
        assertFalse(early.core.reauthAvailable)
        assertFailsWith<IllegalStateException> { early.core.reauthenticate(early.now) }
        assertTrue(early.sent().isEmpty())
        // Пока идёт обмен
        val s = script()
        s.connect(s.connackOk())
        assertTrue(s.core.reauthAvailable)
        s.core.reauthenticate(s.now)
        assertFalse(s.core.reauthAvailable)
        assertFailsWith<IllegalStateException> { s.core.reauthenticate(s.now) }
        assertEquals(1, s.sent().size)
        // Без настроенной аутентификации
        val plain = script(settings(auth = null))
        plain.start()
        plain.sent()
        assertIs<CoreOutput.Connected>(plain.send(plain.connack()).single())
        assertFalse(plain.core.reauthAvailable)
        assertFailsWith<IllegalStateException> { plain.core.reauthenticate(plain.now) }
        assertTrue(plain.sent().isEmpty())
    }

    @Test
    fun reauthenticationProtocolViolations() {
        // Вне обмена: 0x19 от брокера (её начинает только клиент) и 0x00 — нарушения протокола
        val cases: List<Pair<String, (BrokerScript) -> ByteArray>> = listOf(
            "AUTH 0x19 от брокера" to { it.auth5(code = ReasonCode.RE_AUTHENTICATE, method = METHOD) },
            "AUTH 0x00 вне обмена" to { it.auth5(code = ReasonCode.SUCCESS, method = METHOD) },
        )
        for ((name, packet) in cases) {
            val s = script()
            s.connect(s.connackOk())
            assertEquals(MqttError.Protocol(ReasonCode.PROTOCOL_ERROR), s.send(packet(s)).closed(), name)
        }
        // В активном обмене: 0x19 от брокера, второй вызов до ответа клиента, другой метод — нарушения
        val active = script()
        active.connect(active.connackOk())
        active.core.reauthenticate(active.now)
        active.sent()
        assertEquals(
            MqttError.Protocol(ReasonCode.PROTOCOL_ERROR),
            active.send(active.auth5(code = ReasonCode.RE_AUTHENTICATE, method = METHOD)).closed(),
        )
        val second = script()
        second.connect(second.connackOk())
        second.core.reauthenticate(second.now)
        second.sent()
        second.send(second.auth5(method = METHOD)).challenge()
        assertEquals(MqttError.Protocol(ReasonCode.PROTOCOL_ERROR), second.send(second.auth5(method = METHOD)).closed())
        val other = script()
        other.connect(other.connackOk())
        other.core.reauthenticate(other.now)
        other.sent()
        assertEquals(MqttError.Protocol(ReasonCode.PROTOCOL_ERROR), other.send(other.auth5(method = "OTHER")).closed())
    }

    @Test
    fun lateResponseAfterReauthenticationIsIgnored() {
        val s = script()
        s.connect(s.connackOk())
        s.core.reauthenticate(s.now)
        s.sent()
        s.send(s.auth5(method = METHOD)).challenge()
        s.core.authResponse(byteArrayOf(1), s.now)
        s.sent()
        assertIs<CoreOutput.Reauthenticated>(s.send(s.auth5(code = ReasonCode.SUCCESS, method = METHOD)).single())
        assertTrue(s.core.authResponse(byteArrayOf(2), s.now).isEmpty())
        assertTrue(s.core.authFailed(s.now).isEmpty())
        assertTrue(s.sent().isEmpty())
        assertEquals(CoreState.Connected, s.core.state)
    }

    private companion object {
        const val METHOD = "TEST-SCRAM"
    }
}
