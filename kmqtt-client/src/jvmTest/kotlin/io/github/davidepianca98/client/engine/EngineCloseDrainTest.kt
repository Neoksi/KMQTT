package io.github.davidepianca98.client.engine

import io.github.davidepianca98.client.ConnectionState
import io.github.davidepianca98.client.PublishFailure
import io.github.davidepianca98.client.PublishResult
import io.github.davidepianca98.client.TransportEvent
import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.core.SessionSettings
import io.github.davidepianca98.client.engine.EngineReconnectTest.Rig
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTDisconnect
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPublish
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTSubscribe
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Properties
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * Закрытие с очередью сверх окна: [EngineCommand.Close] в живой сессии сначала досылает публикации, ждущие
 * окна (предел неподтверждённых продолжаемой сессии [EngineOptions.maxUnacknowledged] или Receive Maximum брокера), по
 * мере PUBACK и не дольше [EngineOptions.closeDrainMs], и только затем пишет DISCONNECT. Раньше DISCONNECT уходил сразу,
 * а хвост очереди получал `Failed(Closed)` — очистка retained большого устройства оставалась неполной.
 */
@OptIn(MqttCoreApi::class)
class EngineCloseDrainTest {

    private fun persistentRig(maxUnacknowledged: Int, closeDrainMs: Long = DRAIN_MS) = Rig(
        session = SessionSettings(MQTTVersion.MQTT5, clientId = "c", keepAliveSec = 0, cleanStart = false),
        maxUnacknowledged = maxUnacknowledged,
        closeDrainMs = closeDrainMs,
    )

    private fun Rig.publishes(): List<MQTTPublish> = packets.filterIsInstance<MQTTPublish>()

    private fun Rig.disconnectIndex(): Int = packets.indexOfFirst { it is MQTTDisconnect }

    /** Брокер подтверждает всё, что уже записано: остальное пойдёт само (Rig отвечает PUBACK на каждую новую). */
    private fun Rig.ackWritten() {
        puback = ReasonCode.SUCCESS
        for (publish in publishes()) brokerSends(script.puback(publish.packetId!!.toInt()))
    }

    @Test
    fun queueBeyondTheUnacknowledgedLimitIsSentBeforeDisconnect() {
        val rig = persistentRig(maxUnacknowledged = 2)
        rig.eofOnDisconnect = false
        rig.start()
        rig.puback = null
        val tickets = List(6) { rig.publish("dev/$it", retain = true) }
        assertEquals(listOf("dev/0", "dev/1"), rig.publishes().map { it.topicName }, "окно — две")

        rig.command(EngineCommand.Close())
        assertEquals(-1, rig.disconnectIndex(), "DISCONNECT ждёт, пока очередь не уйдёт: ${rig.packets}")
        assertIs<ConnectionState.Connected>(rig.engine.state)

        rig.ackWritten()
        assertEquals((0 until 6).map { "dev/$it" }, rig.publishes().map { it.topicName }, "все и по порядку")
        val disconnect = rig.disconnectIndex()
        assertTrue(disconnect > rig.packets.indexOfLast { it is MQTTPublish }, "DISCONNECT после последней: ${rig.packets}")
        for (ticket in tickets) assertEquals(listOf<PublishResult>(PublishResult.Sent(101)), rig.results(ticket))

        rig.deliver(TransportEvent.Eof)
        assertEquals(ConnectionState.Closed(null), rig.engine.state)
    }

    @Test
    fun queueBeyondTheBrokerReceiveWindowIsSentBeforeDisconnect() {
        val rig = Rig()
        rig.connackProps = { MQTT5Properties(receiveMaximum = 1u) }
        rig.start()
        rig.puback = null
        val tickets = List(4) { rig.publish("dev/$it", qos = if (it == 2) Qos.AT_MOST_ONCE else Qos.AT_LEAST_ONCE) }
        assertEquals(listOf("dev/0"), rig.publishes().map { it.topicName })

        rig.command(EngineCommand.Close())
        assertEquals(-1, rig.disconnectIndex())

        rig.ackWritten()
        assertEquals((0 until 4).map { "dev/$it" }, rig.publishes().map { it.topicName })
        assertTrue(rig.disconnectIndex() > rig.packets.indexOfLast { it is MQTTPublish })
        for (ticket in tickets) assertEquals(listOf<PublishResult>(PublishResult.Sent(101)), rig.results(ticket))
        assertEquals(ConnectionState.Closed(null), rig.engine.state, "брокер закрыл соединение после DISCONNECT")
    }

    @Test
    fun silentBrokerEndsTheDrainAtTheLimitAndTheRestFailsWithClosed() {
        val rig = persistentRig(maxUnacknowledged = 2)
        rig.start()
        rig.puback = null
        val tickets = List(5) { rig.publish("dev/$it", retain = true) }
        rig.command(EngineCommand.Close())

        rig.advance(DRAIN_MS - 1)
        assertEquals(-1, rig.disconnectIndex(), "предел ещё не вышел")
        assertTrue(tickets.drop(2).all { rig.results(it).isEmpty() })

        rig.advance(1)
        assertTrue(rig.packets.last() is MQTTDisconnect, "${rig.packets}")
        assertEquals(listOf("dev/0", "dev/1"), rig.publishes().map { it.topicName }, "без окна больше ничего не ушло")
        for (ticket in tickets.take(2)) assertEquals(listOf<PublishResult>(PublishResult.Sent(101)), rig.results(ticket))
        for (ticket in tickets.drop(2)) {
            assertEquals(listOf<PublishResult>(PublishResult.Failed(PublishFailure.Closed)), rig.results(ticket))
        }
        assertEquals(ConnectionState.Closed(null), rig.engine.state)
    }

    @Test
    fun publicationsAndGroupChangesDuringTheDrainAreRefused() {
        val rig = persistentRig(maxUnacknowledged = 1)
        rig.eofOnDisconnect = false
        rig.start()
        rig.puback = null
        rig.publish("dev/0")
        rig.publish("dev/1")
        rig.command(EngineCommand.Close())
        val subscribes = rig.packets.count { it is MQTTSubscribe }

        val late = rig.publish("dev/late")
        assertEquals(listOf<PublishResult>(PublishResult.Failed(PublishFailure.Closed)), rig.results(late))
        rig.command(EngineCommand.SetGroup("extra", setOf("x/1"), Qos.AT_MOST_ONCE))
        assertEquals(subscribes, rig.packets.count { it is MQTTSubscribe }, "SUBSCRIBE после close() не пишется")

        rig.ackWritten()
        assertEquals(listOf("dev/0", "dev/1"), rig.publishes().map { it.topicName })
        assertTrue(rig.packets.last() is MQTTDisconnect)
    }

    @Test
    fun emptyQueueDisconnectsAtOnce() {
        val rig = persistentRig(maxUnacknowledged = 2)
        rig.eofOnDisconnect = false
        rig.start()
        rig.puback = null
        val inFlight = rig.publish("dev/0") // записана, PUBACK нет: в очереди ничего не ждёт
        rig.command(EngineCommand.Close())
        assertTrue(rig.packets.last() is MQTTDisconnect, "${rig.packets}")
        assertEquals(listOf<PublishResult>(PublishResult.Sent(101)), rig.results(inFlight))
        assertFalse(rig.actions.contains(EngineAction.CloseTransport), "дальше — обычное ожидание EOF")
    }

    @Test
    fun customLimitIsHonouredAndZeroKeepsTheOldImmediateClose() {
        val custom = persistentRig(maxUnacknowledged = 1, closeDrainMs = 500)
        custom.start()
        custom.puback = null
        custom.publish("dev/0")
        val held = custom.publish("dev/1")
        custom.command(EngineCommand.Close())
        custom.advance(499)
        assertEquals(-1, custom.disconnectIndex())
        custom.advance(1)
        assertTrue(custom.packets.last() is MQTTDisconnect)
        assertEquals(listOf<PublishResult>(PublishResult.Failed(PublishFailure.Closed)), custom.results(held))

        val zero = persistentRig(maxUnacknowledged = 1, closeDrainMs = 0)
        zero.start()
        zero.puback = null
        zero.publish("dev/0")
        val dropped = zero.publish("dev/1")
        zero.command(EngineCommand.Close())
        assertTrue(zero.packets.last() is MQTTDisconnect, "без досылки DISCONNECT сразу")
        assertEquals(listOf<PublishResult>(PublishResult.Failed(PublishFailure.Closed)), zero.results(dropped))
    }

    @Test
    fun publicationExpiringDuringTheDrainEndsItEarly() {
        val rig = persistentRig(maxUnacknowledged = 1)
        rig.eofOnDisconnect = false
        rig.start()
        rig.puback = null
        rig.publish("dev/0")
        val shortLived = rig.publish("dev/1", ttlMs = 1_000)
        rig.command(EngineCommand.Close())
        rig.advance(1_000)
        assertEquals(listOf<PublishResult>(PublishResult.Expired), rig.results(shortLived))
        assertTrue(rig.packets.last() is MQTTDisconnect, "очередь пуста раньше предела: ${rig.packets}")
    }

    private companion object {
        /** Предел досылки по умолчанию ([EngineOptions.closeDrainMs]). */
        const val DRAIN_MS = EngineOptions.DEFAULT_CLOSE_DRAIN_MS
    }
}
