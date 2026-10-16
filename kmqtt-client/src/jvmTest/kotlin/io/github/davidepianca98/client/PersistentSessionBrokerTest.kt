package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration.Companion.seconds

/**
 * Продолжение сессии (SEVEN-251, `persistentSession = true`) против встроенного брокера форка: обрыв до и после
 * PUBLISH на проводе, Session Present 1 (брокер помнит сессию) и 0 (брокер перезапущен), «не досылать». Клиент ходит
 * через [TcpProxy]: `holdUpstream` задерживает PUBLISH до брокера, `holdDownstream` — PUBACK до клиента, `cut` рвёт.
 */
internal class PersistentSessionBrokerTest : BrokerTestBase() {

    private fun <T> timed(block: suspend CoroutineScope.() -> T): T = runBlocking { withTimeout(30_000, block) }

    private suspend fun MqttClient.up(): ConnectResult.Connected =
        withTimeout(10_000) { connect() } as? ConnectResult.Connected ?: fail("клиент не подключился")

    private suspend fun MqttClient.subscribed(group: String, vararg filters: String) {
        subscriptions.set(group, filters.toSet(), Qos.AT_LEAST_ONCE)
        withTimeout(10_000) { subscriptions.awaitApplied(group) }
    }

    private fun persistentClient(port: Int, version: MQTTVersion) =
        client(port, version) { it.copy(persistentSession = true, sessionExpiry = 60.seconds) }

    private fun MqttClient.epochAfter(epoch: Long): ConnectionState.Connected? =
        (connection.value as? ConnectionState.Connected)?.takeIf { it.epoch > epoch }

    /**
     * PUBLISH дошёл до брокера, PUBACK задержан, соединение рвётся: после переподключения (Session Present = 1)
     * публикация досылается с DUP = 1 и прежним packetId, исход — `Delivered`. QoS 1 — «хотя бы раз»: подписчик может
     * получить её дважды.
     */
    private fun lostPubackIsResent(version: MQTTVersion) = timed {
        val broker = broker()
        val proxy = proxy(broker.port)
        val observer = client(broker.port, version)
        val inbox = collect(observer)
        observer.up()
        observer.subscribed("g", "cmd/#")
        val client = persistentClient(proxy.port, version)
        val first = client.up()
        assertFalse((client.connection.value as ConnectionState.Connected).sessionPresent, "первая сессия — новая")
        assertFalse(proxy.connections.last().connect!!.cleanStart, "CONNECT с Clean Start = 0")

        proxy.holdDownstream = true
        val result = async(Dispatchers.Default) {
            client.publish("cmd/x", byteArrayOf(7), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        }
        BrokerTestKit.waitUntil(5_000, "брокер получил PUBLISH") { inbox.messages.any { it.topic == "cmd/x" } }
        val original = proxy.connections.last().published.single()
        proxy.cut()
        proxy.holdDownstream = false
        proxy.restore()

        val outcome = withTimeout(15_000) { result.await() }
        val resumed = client.epochAfter(first.epoch) ?: fail("клиент не переподключился: ${client.connection.value}")
        assertTrue(resumed.sessionPresent, "брокер продолжил сессию")
        assertEquals(PublishResult.Delivered(resumed.epoch), outcome)
        val again = proxy.connections.last().published.single { it.topic == "cmd/x" }
        assertTrue(again.dup, "DUP = 1")
        assertEquals(original.packetId, again.packetId, "прежний packetId")
        BrokerTestKit.waitUntil(5_000, "досланная дошла") { inbox.messages.count { it.topic == "cmd/x" } >= 1 }
        assertTrue(inbox.messages.count { it.topic == "cmd/x" } in 1..2, "QoS 1: один или два раза")
    }

    @Test
    fun lostPubackIsResentMqtt5() = lostPubackIsResent(MQTTVersion.MQTT5)

    @Test
    fun lostPubackIsResentMqtt311() = lostPubackIsResent(MQTTVersion.MQTT3_1_1)

    /** PUBLISH не дошёл до брокера (застрял в прокси), обрыв: досылается и доходит ровно один раз. */
    @Test
    fun publicationLostOnTheWayIsResentAndArrivesOnce() = timed {
        val broker = broker()
        val proxy = proxy(broker.port)
        val observer = client(broker.port)
        val inbox = collect(observer)
        observer.up()
        observer.subscribed("g", "cmd/#")
        val client = persistentClient(proxy.port, MQTTVersion.MQTT5)
        val first = client.up()

        proxy.holdUpstream = true
        val result = async(Dispatchers.Default) {
            client.publish("cmd/once", byteArrayOf(1), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        }
        delay(300) // PUBLISH записан клиентом и лежит в прокси
        assertTrue(result.isActive)
        proxy.cut()
        proxy.holdUpstream = false
        proxy.restore()

        val outcome = withTimeout(15_000) { result.await() }
        val resumed = client.epochAfter(first.epoch) ?: fail("клиент не переподключился")
        assertEquals(PublishResult.Delivered(resumed.epoch), outcome)
        client.publish("cmd/end", ByteArray(0), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        BrokerTestKit.waitUntil(10_000, "метка") { inbox.messages.any { it.topic == "cmd/end" } }
        assertEquals(1, inbox.messages.count { it.topic == "cmd/once" }, "ровно один раз")
    }

    /** Брокер перезапущен (сессии нет, Session Present = 0): неподтверждённая — `Failed(SessionLost)`, не досылается. */
    @Test
    fun brokerRestartLosesTheSessionAndNothingIsResent() = timed {
        val first = TestBroker()
        val port = first.port
        val proxy = proxy(port)
        val client = persistentClient(proxy.port, MQTTVersion.MQTT5)
        val epoch = client.up().epoch

        proxy.holdDownstream = true
        val result = async(Dispatchers.Default) {
            client.publish("cmd/lost", byteArrayOf(1), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        }
        BrokerTestKit.waitUntil(5_000, "PUBLISH прошёл прокси") { proxy.connections.last().published.isNotEmpty() }
        proxy.cut()
        first.stop()
        proxy.holdDownstream = false
        track(TestBroker(port))
        proxy.restore()

        assertEquals(PublishResult.Failed(PublishFailure.SessionLost), withTimeout(15_000) { result.await() })
        val resumed = client.epochAfter(epoch) ?: fail("клиент не переподключился")
        assertFalse(resumed.sessionPresent)
        delay(200)
        assertTrue(proxy.connections.last().published.none { it.topic == "cmd/lost" }, "ничего не дослано")
    }

    /** «Не досылать» (опасная команда): обрыв до PUBACK — `ConnectionLost` сразу, после переподключения не пишется. */
    @Test
    fun noResendPublicationIsNeverWrittenAgain() = timed {
        val broker = broker()
        val proxy = proxy(broker.port)
        val client = persistentClient(proxy.port, MQTTVersion.MQTT5)
        val epoch = client.up().epoch

        proxy.holdDownstream = true
        val result = async(Dispatchers.Default) {
            client.publish("cmd/reboot", byteArrayOf(1), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered, resend = false)
        }
        BrokerTestKit.waitUntil(5_000, "PUBLISH прошёл прокси") { proxy.connections.last().published.isNotEmpty() }
        proxy.cut()
        assertEquals(PublishResult.Failed(PublishFailure.ConnectionLost), withTimeout(10_000) { result.await() })
        proxy.holdDownstream = false
        proxy.restore()
        BrokerTestKit.waitUntil(10_000, "клиент переподключился") { client.epochAfter(epoch)?.sessionPresent == true }
        client.publish("cmd/end", ByteArray(0), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        assertTrue(proxy.connections.last().published.none { it.topic == "cmd/reboot" }, "опасная команда не дослана")
    }

    @Test
    fun persistentSessionNeedsAnExplicitClientId() {
        assertFailsWith<IllegalArgumentException> {
            MqttClientOptions(host = TestBroker.HOST, port = 1883, persistentSession = true)
        }
        assertFailsWith<IllegalArgumentException> {
            MqttClientOptions(host = TestBroker.HOST, port = 1883, clientId = "c", persistentSession = true, sessionExpiry = 0.seconds)
        }
    }
}
