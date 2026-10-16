package io.github.davidepianca98.client.engine

import io.github.davidepianca98.client.ConnectionState
import io.github.davidepianca98.client.PublishFailure
import io.github.davidepianca98.client.PublishResult
import io.github.davidepianca98.client.PublishStage
import io.github.davidepianca98.client.ReconnectPolicy
import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.core.SessionSettings
import io.github.davidepianca98.client.engine.EngineReconnectTest.Companion.K1
import io.github.davidepianca98.client.engine.EngineReconnectTest.Rig
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTConnect
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPublish
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPubrel
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTSubscribe
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTUnsubscribe
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Connect
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Продолжение сессии (SEVEN-251, `cleanStart = false`) в движке на виртуальном времени: обрыв до и после PUBACK,
 * Session Present 0/1, TTL, признак «не досылать», предел хранилища, порядок досылки, PUBREL QoS 2, снятые за время
 * обрыва подписки. Стенд — [Rig] из [EngineReconnectTest] (брокер отвечает байтами `BrokerScript` синхронно).
 */
@OptIn(MqttCoreApi::class)
class EnginePersistentSessionTest {

    private fun rig(
        version: MQTTVersion = MQTTVersion.MQTT5,
        maxUnacknowledged: Int = 64,
        reconnect: ReconnectPolicy = ReconnectPolicy(random = { 0.0 }),
    ): Rig = Rig(
        version = version,
        reconnect = reconnect,
        session = SessionSettings(
            version,
            clientId = "c",
            keepAliveSec = 0,
            cleanStart = false,
            sessionExpiryIntervalSec = if (version == MQTTVersion.MQTT5) EXPIRY else null,
        ),
        maxUnacknowledged = maxUnacknowledged,
    ).also { it.sessionPresent = true }

    /** Подключён, подписка подтверждена; брокер не отвечает на PUBLISH (PUBACK шлёт тест). */
    private fun connected(rig: Rig): Rig {
        rig.start()
        rig.advance(100)
        rig.puback = null
        return rig
    }

    private fun Rig.publishesFrom(index: Int): List<MQTTPublish> = packets.drop(index).filterIsInstance<MQTTPublish>()

    private fun Rig.reconnect() {
        val sessions = started().size
        advanceUntil { started().size > sessions }
    }

    private fun Rig.lastEpoch(): Long = started().last()

    // ---- обрыв до / после PUBACK, Session Present = 1 ----

    private fun dropBeforePubackResendsWithDupAndTheSamePacketId(version: MQTTVersion) {
        val rig = connected(rig(version))
        val connect = rig.packets.filterIsInstance<MQTTConnect>().single()
        assertFalse(connect.connectFlags.cleanStart, "CONNECT с Clean Start = 0")
        if (version == MQTTVersion.MQTT5) {
            assertEquals(EXPIRY.toUInt(), (connect as MQTT5Connect).properties.sessionExpiryInterval)
        }

        val ticket = rig.publish("cmd/a", until = PublishStage.Delivered)
        val first = rig.packets.filterIsInstance<MQTTPublish>().single()
        assertFalse(first.dup)
        rig.drop()
        assertTrue(rig.results(ticket).isEmpty(), "ждёт следующей сессии, а не ConnectionLost")
        assertTrue(rig.engine.state is ConnectionState.Waiting)

        val mark = rig.packets.size
        rig.puback = io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode.SUCCESS
        rig.reconnect()
        val connected = rig.engine.state as ConnectionState.Connected
        assertTrue(connected.sessionPresent)
        val resent = rig.publishesFrom(mark).single()
        assertTrue(resent.dup, "DUP = 1")
        assertEquals(first.packetId, resent.packetId, "прежний packetId")
        assertEquals("cmd/a", resent.topicName)
        assertEquals(listOf<PublishResult>(PublishResult.Delivered(rig.lastEpoch())), rig.results(ticket))
        assertTrue(rig.logs().contains(EngineLogEvent.Resent(1)))
    }

    @Test
    fun dropBeforePubackResendsWithDupAndTheSamePacketIdMqtt5() =
        dropBeforePubackResendsWithDupAndTheSamePacketId(MQTTVersion.MQTT5)

    @Test
    fun dropBeforePubackResendsWithDupAndTheSamePacketIdMqtt311() =
        dropBeforePubackResendsWithDupAndTheSamePacketId(MQTTVersion.MQTT3_1_1)

    @Test
    fun dropAfterPubackResendsNothing() {
        val rig = rig()
        rig.start()
        rig.advance(100)
        val ticket = rig.publish("cmd/a", until = PublishStage.Delivered)
        assertEquals(listOf<PublishResult>(PublishResult.Delivered(rig.lastEpoch())), rig.results(ticket))
        rig.drop()
        val mark = rig.packets.size
        rig.reconnect()
        assertTrue(rig.publishesFrom(mark).isEmpty(), "подтверждённое не досылается")
        assertEquals(1, rig.results(ticket).size)
    }

    @Test
    fun publicationThatGotSentIsResentSilently() {
        val rig = connected(rig())
        val ticket = rig.publish("cmd/a")
        assertEquals(listOf<PublishResult>(PublishResult.Sent(rig.lastEpoch())), rig.results(ticket))
        rig.drop()
        val mark = rig.packets.size
        rig.reconnect()
        assertTrue(rig.publishesFrom(mark).single().dup)
        assertEquals(1, rig.results(ticket).size, "второго исхода нет")
    }

    // ---- Session Present = 0 ----

    @Test
    fun sessionLostFailsWithSessionLostAndResendsNothing() {
        val rig = connected(rig())
        val waiting = rig.publish("cmd/a", until = PublishStage.Delivered)
        val sent = rig.publish("cmd/b")
        rig.drop()
        rig.sessionPresent = false
        val mark = rig.packets.size
        rig.reconnect()

        assertFalse((rig.engine.state as ConnectionState.Connected).sessionPresent)
        assertEquals(listOf<PublishResult>(PublishResult.Failed(PublishFailure.SessionLost)), rig.results(waiting))
        assertEquals(1, rig.results(sent).size, "Sent: только прежний исход")
        assertTrue(rig.publishesFrom(mark).isEmpty(), "при потерянной сессии ничего не досылается")
        // Новые публикации идут как обычно
        val next = rig.publish("cmd/c")
        assertEquals(listOf<PublishResult>(PublishResult.Sent(rig.lastEpoch())), rig.results(next))
    }

    // ---- TTL ----

    @Test
    fun ttlRunningOutWhileOfflineGivesExpiredAtTheDeadlineAndNothingIsResent() {
        val rig = connected(rig())
        val publishedAt = rig.now
        val ticket = rig.publish("cmd/a", ttlMs = 2_000, until = PublishStage.Delivered)
        rig.advance(100)
        rig.online = false
        rig.drop()

        rig.advance(publishedAt + 1_999 - rig.now)
        assertTrue(rig.results(ticket).isEmpty())
        rig.advance(1)
        assertEquals(listOf<PublishResult>(PublishResult.Expired), rig.results(ticket), "Expired ровно в срок TTL")

        val mark = rig.packets.size
        rig.online = true
        rig.reconnect()
        assertTrue(rig.publishesFrom(mark).isEmpty(), "истёкшее не досылается")
    }

    @Test
    fun ttlIsCheckedAtTheResendItself() {
        val rig = connected(rig())
        val publishedAt = rig.now
        val alive = rig.publish("cmd/alive", ttlMs = 60_000, until = PublishStage.Delivered)
        val sentOnly = rig.publish("cmd/sent", ttlMs = 900)
        rig.drop() // переподключение через 1 с — у второй TTL уже кончился
        val mark = rig.packets.size
        rig.reconnect()
        assertTrue(rig.now >= publishedAt + 900)
        assertEquals(listOf("cmd/alive"), rig.publishesFrom(mark).map { it.topicName })
        assertEquals(1, rig.results(sentOnly).size, "получившая Sent забывается без второго исхода")
        assertTrue(rig.results(alive).isEmpty(), "ждёт PUBACK досланной")
    }

    // ---- «не досылать» ----

    @Test
    fun noResendPublicationFailsAtOnceAndIsNeverWrittenAgain() {
        val rig = connected(rig())
        val dangerous = rig.publish("cmd/reboot", until = PublishStage.Delivered, resend = false)
        val dangerousSent = rig.publish("cmd/reboot2", resend = false)
        rig.drop()
        assertEquals(listOf<PublishResult>(PublishResult.Failed(PublishFailure.ConnectionLost)), rig.results(dangerous))
        val mark = rig.packets.size
        rig.reconnect()
        assertTrue(rig.publishesFrom(mark).isEmpty(), "опасная команда не досылается")
        assertEquals(1, rig.results(dangerousSent).size)
    }

    @Test
    fun cancelledPublicationIsNotResent() {
        val rig = connected(rig())
        val ticket = rig.publish("cmd/a", until = PublishStage.Delivered)
        rig.command(EngineCommand.Cancel(ticket))
        rig.drop()
        val mark = rig.packets.size
        rig.reconnect()
        assertTrue(rig.publishesFrom(mark).isEmpty())
        assertTrue(rig.results(ticket).isEmpty())
    }

    // ---- порядок ----

    @Test
    fun resendGoesBeforePublicationsQueuedWhileOffline() {
        val rig = connected(rig())
        rig.publish("cmd/1", until = PublishStage.Delivered)
        rig.online = false
        rig.drop()
        val queued = rig.publish("cmd/2", ttlMs = 60_000)
        rig.advance(5_000)
        val mark = rig.packets.size
        rig.online = true
        rig.reconnect()
        val after = rig.publishesFrom(mark)
        assertEquals(listOf("cmd/1", "cmd/2"), after.map { it.topicName })
        assertEquals(listOf(true, false), after.map { it.dup })
        assertEquals(listOf<PublishResult>(PublishResult.Sent(rig.lastEpoch())), rig.results(queued))
        val subscribe = rig.packets.drop(mark).indexOfFirst { it is MQTTSubscribe }
        val resend = rig.packets.drop(mark).indexOfFirst { it is MQTTPublish }
        assertTrue(resend < subscribe, "досылка раньше подписки набора")
    }

    // ---- предел хранилища ----

    @Test
    fun unacknowledgedLimitHoldsNewPublicationsUntilAPuback() {
        val rig = connected(rig(maxUnacknowledged = 2))
        val tickets = List(3) { rig.publish("cmd/$it", until = PublishStage.Delivered) }
        val written = rig.packets.filterIsInstance<MQTTPublish>()
        assertEquals(listOf("cmd/0", "cmd/1"), written.map { it.topicName }, "третья ждёт в очереди")
        // QoS 0 за ней тоже ждёт (FIFO)
        val qos0 = rig.publish("cmd/q0", qos = Qos.AT_MOST_ONCE)
        assertTrue(rig.results(qos0).isEmpty())

        rig.brokerSends(rig.script.puback(written[0].packetId!!.toInt()))
        assertEquals(listOf<PublishResult>(PublishResult.Delivered(rig.lastEpoch())), rig.results(tickets[0]))
        assertEquals(
            listOf("cmd/0", "cmd/1", "cmd/2", "cmd/q0"),
            rig.packets.filterIsInstance<MQTTPublish>().map { it.topicName },
        )
    }

    @Test
    fun limitAlsoBoundsTheResend() {
        val rig = connected(rig(maxUnacknowledged = 2))
        rig.publish("cmd/0", until = PublishStage.Delivered)
        rig.publish("cmd/1", until = PublishStage.Delivered)
        rig.drop()
        val mark = rig.packets.size
        rig.reconnect()
        val resent = rig.publishesFrom(mark)
        assertEquals(listOf("cmd/0", "cmd/1"), resent.map { it.topicName })
        val next = rig.publish("cmd/2", until = PublishStage.Delivered)
        assertEquals(2, rig.publishesFrom(mark).size, "новая ждёт освобождения хранилища")
        rig.brokerSends(rig.script.puback(resent[0].packetId!!.toInt()))
        assertEquals("cmd/2", rig.publishesFrom(mark).last().topicName)
        assertTrue(rig.results(next).isEmpty())
    }

    // ---- закрытие и исчерпание попыток ----

    @Test
    fun closeFailsWaitingPublicationsWithClosed() {
        val rig = connected(rig())
        rig.online = false
        val ticket = rig.publish("cmd/a", until = PublishStage.Delivered)
        rig.drop()
        rig.command(EngineCommand.Close())
        assertEquals(listOf<PublishResult>(PublishResult.Failed(PublishFailure.Closed)), rig.results(ticket))
    }

    @Test
    fun exhaustedAttemptsFailWaitingPublicationsWithConnectionLost() {
        val rig = connected(rig(reconnect = ReconnectPolicy.Never))
        val ticket = rig.publish("cmd/a", until = PublishStage.Delivered)
        rig.drop()
        assertTrue(rig.engine.state is ConnectionState.Closed)
        assertEquals(listOf<PublishResult>(PublishResult.Failed(PublishFailure.ConnectionLost)), rig.results(ticket))
        assertNull(rig.engine.nextDeadline())
    }

    // ---- QoS 2 ----

    @Test
    fun qos2AfterPubrecResumesWithPubrel() {
        val rig = connected(rig())
        val ticket = rig.publish("cmd/q2", qos = Qos.EXACTLY_ONCE, until = PublishStage.Delivered)
        val id = rig.packets.filterIsInstance<MQTTPublish>().single().packetId!!.toInt()
        rig.brokerSends(rig.script.pubrec(id))
        rig.drop()
        val mark = rig.packets.size
        rig.reconnect()
        assertTrue(rig.publishesFrom(mark).isEmpty(), "PUBLISH после PUBREC не повторяется")
        assertEquals(id, rig.packets.drop(mark).filterIsInstance<MQTTPubrel>().single().packetId.toInt())
        rig.brokerSends(rig.script.pubcomp(id))
        assertEquals(listOf<PublishResult>(PublishResult.Delivered(rig.lastEpoch())), rig.results(ticket))
    }

    @Test
    fun incomingQos2StateSurvivesTheReconnect() {
        val rig = connected(rig())
        rig.brokerSends(rig.script.publish(K1, "x".encodeToByteArray(), Qos.EXACTLY_ONCE, packetId = 9))
        assertEquals(1, rig.emitted().count { it is io.github.davidepianca98.client.MqttIncoming.Message })
        rig.drop()
        rig.reconnect()
        val epoch = rig.lastEpoch()
        // Брокер досылает тот же PUBLISH (DUP) — второй раз не выдаётся; затем PUBREL — без ошибки протокола
        rig.brokerSends(rig.script.publish(K1, "x".encodeToByteArray(), Qos.EXACTLY_ONCE, packetId = 9))
        rig.brokerSends(rig.script.pubrel(9))
        rig.brokerSends(rig.script.pubrel(77)) // неизвестный — PUBCOMP, сессия жива
        assertEquals(1, rig.emitted().count { it is io.github.davidepianca98.client.MqttIncoming.Message })
        assertEquals(epoch, (rig.engine.state as ConnectionState.Connected).epoch)
    }

    // ---- подписки ----

    @Test
    fun filtersRemovedWhileOfflineAreUnsubscribedOnResume() {
        val rig = connected(rig())
        rig.online = false
        rig.drop()
        rig.command(EngineCommand.SetGroup("working", setOf(K2), Qos.AT_MOST_ONCE))
        val mark = rig.packets.size
        rig.online = true
        rig.reconnect()
        val after = rig.packets.drop(mark)
        val unsubscribe = after.filterIsInstance<MQTTUnsubscribe>().single()
        assertEquals(listOf(K1), unsubscribe.topicFilters)
        assertEquals(listOf(K2), after.filterIsInstance<MQTTSubscribe>().single().subscriptions.map { it.topicFilter })
        assertTrue(after.indexOf(unsubscribe) < after.indexOfFirst { it is MQTTSubscribe })
    }

    @Test
    fun withoutSessionPresentNothingIsUnsubscribed() {
        val rig = connected(rig())
        rig.online = false
        rig.drop()
        rig.command(EngineCommand.SetGroup("working", setOf(K2), Qos.AT_MOST_ONCE))
        rig.sessionPresent = false
        val mark = rig.packets.size
        rig.online = true
        rig.reconnect()
        assertTrue(rig.packets.drop(mark).none { it is MQTTUnsubscribe })
    }

    // ---- умолчание не изменилось ----

    @Test
    fun cleanStartByDefaultAndConnectionLostAsBefore() {
        val rig = Rig()
        rig.start()
        rig.advance(100)
        rig.puback = null
        val connect = rig.packets.filterIsInstance<MQTTConnect>().single() as MQTT5Connect
        assertTrue(connect.connectFlags.cleanStart)
        assertNull(connect.properties.sessionExpiryInterval)
        val ticket = rig.publish("cmd/a", until = PublishStage.Delivered)
        rig.drop()
        assertEquals(listOf<PublishResult>(PublishResult.Failed(PublishFailure.ConnectionLost)), rig.results(ticket))
        val mark = rig.packets.size
        rig.reconnect()
        assertTrue(rig.publishesFrom(mark).isEmpty())
    }

    private companion object {
        const val EXPIRY = 600L
        const val K2 = "/devices/wb-mr6c_1/controls/K2"
    }
}
