package io.github.davidepianca98.client

import io.github.davidepianca98.client.internal.MqttClientImpl
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Auth
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Disconnect
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Properties
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Повторная аутентификация посреди сессии [MqttClient.reauthenticate] на виртуальном времени:
 * [VirtualHost] крутит фасад, [FakeBroker] отвечает пакетами AUTH. Провайдер `respond` работает на планировщике
 * теста (`authContext`), поэтому обмен идёт синхронно с `runCurrent`.
 */
class MqttClientReauthTest {

    private fun options(
        connackTimeout: Duration = 30.seconds,
        initialData: () -> ByteArray? = { byteArrayOf(1) },
        respond: suspend (method: String, data: ByteArray?) -> ByteArray? = { _, data -> data },
    ) = MqttClientOptions(
        host = "broker.test",
        port = 1883,
        clientId = "reauth",
        keepAlive = 0.seconds,
        connackTimeout = connackTimeout,
        reconnect = ReconnectPolicy.Never,
        enhancedAuth = MqttEnhancedAuth(METHOD, initialData, respond),
    )

    private class Rig(val client: MqttClient, val host: VirtualHost) {
        val broker: FakeBroker get() = host.broker
    }

    private fun TestScope.rig(options: MqttClientOptions): Rig {
        val host = VirtualHost(this)
        // Провайдер ответов — на планировщике теста, а не на Dispatchers.Default: обмен управляется runCurrent.
        // Job контекста теста убирается: иначе он заменил бы SupervisorJob области провайдера, и её отмена при
        // закрытии клиента отменяла бы весь тест
        return Rig(MqttClientImpl(options, host, host, authContext = coroutineContext.minusKey(Job)), host)
    }

    /** start + CONNECT (без авто-CONNACK: ядру с auth нужен Authentication Method в CONNACK) + CONNACK с методом. */
    private suspend fun TestScope.connected(rig: Rig): ConnectResult.Connected {
        rig.broker.connackCode = null
        val pending = async { rig.client.connect() }
        runCurrent()
        assertEquals(listOf("connect", "CONNECT"), rig.broker.log)
        rig.broker.send(rig.broker.script.connack(properties = MQTT5Properties(authenticationMethod = METHOD)))
        return assertIs<ConnectResult.Connected>(pending.await())
    }

    @Test
    fun challengeAnswerSuccess() = runTest {
        val rig = rig(options(respond = { _, data -> data?.plus(1) }))
        connected(rig)
        val result = async { rig.client.reauthenticate() }
        runCurrent()
        // Начало — AUTH 0x19 с методом и данными initialData
        val start = rig.broker.packets<MQTT5Auth>().single()
        assertEquals(ReasonCode.RE_AUTHENTICATE, start.authenticateReasonCode)
        assertEquals(METHOD, start.properties.authenticationMethod)
        assertContentEquals(byteArrayOf(1), start.properties.authenticationData?.asByteArray())
        // Вызов брокера → ответ провайдера ушёл AUTH 0x18
        rig.broker.send(rig.broker.script.auth5(method = METHOD, data = byteArrayOf(7)))
        runCurrent()
        val answer = rig.broker.packets<MQTT5Auth>().last()
        assertEquals(ReasonCode.CONTINUE_AUTHENTICATION, answer.authenticateReasonCode)
        assertContentEquals(byteArrayOf(7, 1), answer.properties.authenticationData?.asByteArray())
        // Успех — вызов завершается true, сессия жива
        rig.broker.send(rig.broker.script.auth5(code = ReasonCode.SUCCESS, method = METHOD, data = byteArrayOf(9)))
        assertTrue(result.await())
        assertIs<ConnectionState.Connected>(rig.client.connection.value)
        rig.client.close()
    }

    @Test
    fun successWithoutChallenge() = runTest {
        val rig = rig(options())
        connected(rig)
        val result = async { rig.client.reauthenticate() }
        runCurrent()
        rig.broker.send(rig.broker.script.auth5(code = ReasonCode.SUCCESS, method = METHOD))
        assertTrue(result.await())
        rig.client.close()
    }

    @Test
    fun withoutEnhancedAuthThrows() = runTest {
        val host = VirtualHost(this)
        val options = MqttClientOptions(host = "broker.test", port = 1883, keepAlive = 0.seconds)
        val client = MqttClient(options, host, host)
        assertFailsWith<IllegalStateException> { client.reauthenticate() }
        client.close()
    }

    @Test
    fun withoutSessionIsFalse() = runTest {
        val rig = rig(options())
        assertFalse(rig.client.reauthenticate())
        rig.client.close()
    }

    @Test
    fun secondCallWhileFirstRunsIsFalse() = runTest {
        val rig = rig(options())
        connected(rig)
        val first = async { rig.client.reauthenticate() }
        runCurrent()
        assertFalse(rig.client.reauthenticate(), "второй вызов, пока первый не отвечен")
        rig.broker.send(rig.broker.script.auth5(code = ReasonCode.SUCCESS, method = METHOD))
        assertTrue(first.await())
        // После завершения — снова можно
        val second = async { rig.client.reauthenticate() }
        runCurrent()
        rig.broker.send(rig.broker.script.auth5(code = ReasonCode.SUCCESS, method = METHOD))
        assertTrue(second.await())
        rig.client.close()
    }

    @Test
    fun brokerSilenceClosesWithConnackTimeout() = runTest {
        val rig = rig(options(connackTimeout = 5.seconds))
        connected(rig)
        val result = async { rig.client.reauthenticate() }
        runCurrent()
        assertEquals(1, rig.broker.packets<MQTT5Auth>().size, "AUTH 0x19 ушёл")
        advanceTimeBy(5_000)
        runCurrent()
        assertFalse(result.await())
        assertEquals(ConnectionState.Closed(MqttError.ConnackTimeout), rig.client.connection.value)
        rig.client.close()
    }

    @Test
    fun providerExceptionEndsWithRefusedNotAuthorized() = runTest {
        val rig = rig(options(respond = { _, _ -> throw IllegalStateException("secret text") }))
        connected(rig)
        val result = async { rig.client.reauthenticate() }
        runCurrent()
        rig.broker.send(rig.broker.script.auth5(method = METHOD, data = byteArrayOf(7)))
        assertFalse(result.await())
        val state = rig.client.connection.value
        assertEquals(ConnectionState.Closed(MqttError.Refused(ReasonCode.NOT_AUTHORIZED)), state)
        assertFalse(state.toString().contains("secret text"), "текст исключения провайдера нигде не сохраняется")
        assertEquals(ReasonCode.NOT_AUTHORIZED, rig.broker.packets<MQTT5Disconnect>().single().reasonCode)
        rig.client.close()
    }

    @Test
    fun connectionEndDuringExchangeIsFalse() = runTest {
        val rig = rig(options())
        connected(rig)
        val result = async { rig.client.reauthenticate() }
        runCurrent()
        rig.broker.eof()
        assertFalse(result.await())
        assertIs<ConnectionState.Closed>(rig.client.connection.value)
        rig.client.close()
    }

    private companion object {
        const val METHOD = "TEST-SCRAM"
    }
}
