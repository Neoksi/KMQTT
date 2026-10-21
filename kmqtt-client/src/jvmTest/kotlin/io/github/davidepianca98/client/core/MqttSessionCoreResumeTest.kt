package io.github.davidepianca98.client.core

import io.github.davidepianca98.client.MqttError
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPubcomp
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPublish
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPubrec
import io.github.davidepianca98.mqtt.packets.mqttv4.ConnectReturnCode
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Connect
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubcomp
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** Ядро для продолжения сессии: резерв packetId, досылка с DUP, Session Present, неизвестный PUBREL. */
@OptIn(MqttCoreApi::class)
class MqttSessionCoreResumeTest {

    private fun persistent(version: MQTTVersion = MQTTVersion.MQTT5) = BrokerScript(
        version,
        SessionSettings(
            version,
            clientId = "c",
            keepAliveSec = 10,
            cleanStart = false,
            sessionExpiryIntervalSec = if (version == MQTTVersion.MQTT5) 3600 else null,
        ),
    )

    @Test
    fun connectCarriesCleanStartZeroAndSessionExpiry() {
        val script = persistent()
        script.start()
        val connect = script.sent().single() as MQTT5Connect
        assertEquals(false, connect.connectFlags.cleanStart)
        assertEquals(3600u, connect.properties.sessionExpiryInterval)
        val out = script.send(script.connack(sessionPresent = true))
        assertTrue(assertIs<CoreOutput.Connected>(out.single()).connack.sessionPresent)
    }

    @Test
    fun sessionPresentWithCleanStartIsStillAProtocolError() {
        val script = BrokerScript(MQTTVersion.MQTT5)
        val out = script.connect(script.connack(sessionPresent = true))
        assertEquals(CoreOutput.Closed(MqttError.Protocol(ReasonCode.PROTOCOL_ERROR)), out.single())
    }

    @Test
    fun reservedIdsAreSkippedAndResendUsesDupWithTheSameId() {
        val script = persistent(MQTTVersion.MQTT3_1_1)
        script.connect(script.connack4(io.github.davidepianca98.mqtt.packets.mqttv4.ConnectReturnCode.CONNECTION_ACCEPTED, true))
        script.core.reservePacketIds(listOf(1, 2))
        val fresh = script.core.publish(OutgoingPublish("t/new", byteArrayOf(1), Qos.AT_LEAST_ONCE), script.now)
        assertEquals(PublishAdmission.Accepted(3), fresh, "зарезервированные 1 и 2 не берутся")
        val resent = script.core.resendPublish(OutgoingPublish("t/old", byteArrayOf(2), Qos.AT_LEAST_ONCE), 1, script.now)
        assertEquals(PublishAdmission.Accepted(1), resent)
        val packets = script.sent().filterIsInstance<MQTTPublish>()
        assertEquals(listOf(false, true), packets.map { it.dup })
        assertEquals(listOf(3u, 1u), packets.map { it.packetId })
        assertFailsWith<IllegalStateException>("занятый id") {
            script.core.resendPublish(OutgoingPublish("t/old", byteArrayOf(2), Qos.AT_LEAST_ONCE), 1, script.now)
        }
        // PUBACK досланной — обычное событие
        assertEquals(listOf<CoreOutput>(CoreOutput.PubAck(1, ReasonCode.SUCCESS)), script.send(script.puback(1)))
        script.core.releasePacketId(2)
        val next = script.core.publish(OutgoingPublish("t/next", byteArrayOf(3), Qos.AT_LEAST_ONCE), script.now)
        assertEquals(PublishAdmission.Accepted(4), next)
    }

    @Test
    fun resendRespectsTheReceiveWindowOfTheBroker() {
        val script = persistent()
        script.connect(
            script.connack(
                sessionPresent = true,
                properties = io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Properties(receiveMaximum = 1u),
            ),
        )
        script.core.reservePacketIds(listOf(5, 6))
        val a = script.core.resendPublish(OutgoingPublish("t/a", byteArrayOf(1), Qos.AT_LEAST_ONCE), 5, script.now)
        val b = script.core.resendPublish(OutgoingPublish("t/b", byteArrayOf(1), Qos.AT_LEAST_ONCE), 6, script.now)
        assertEquals(PublishAdmission.Accepted(5), a)
        assertEquals(PublishAdmission.FlowControlled, b)
    }

    @Test
    fun unknownPubrelInAResumedSessionIsCompletedWithoutClosing() {
        val script = persistent()
        script.connect(script.connack(sessionPresent = true))
        val out = script.send(script.pubrel(42))
        assertTrue(out.isEmpty(), "без Closed")
        val pubcomp = script.sent().single() as MQTT5Pubcomp
        assertEquals(42u, pubcomp.packetId)
        assertEquals(ReasonCode.PACKET_IDENTIFIER_NOT_FOUND, pubcomp.reasonCode)
    }

    @Test
    fun incomingQos2StateIsCarriedToTheNextCore() {
        val first = persistent()
        first.connect(first.connack())
        first.send(first.publish("t/q2", byteArrayOf(1), Qos.EXACTLY_ONCE, packetId = 7))
        val carried = first.core.incomingQos2()
        assertEquals(setOf(7), carried)

        val second = persistent()
        second.connect(second.connack(sessionPresent = true))
        second.core.restoreIncomingQos2(carried)
        val repeat = second.send(second.publish("t/q2", byteArrayOf(1), Qos.EXACTLY_ONCE, packetId = 7))
        assertTrue(repeat.none { it is CoreOutput.Message }, "повтор не выдаётся второй раз")
        second.sent()
        second.send(second.pubrel(7))
        assertIs<MQTTPubcomp>(second.sent().single())
        assertEquals(CoreState.Connected, second.core.state)
    }

    /**
     * брокер (Mosquitto) досылает PUBLISH (DUP = 1) и PUBREL продолжаемой сессии сразу за CONNACK, и они
     * приходят ОДНИМ чтением с ним. Состояние входящих QoS 2, переданное ядру до подключения ([carryIncomingQos2]),
     * действует уже для пакетов того же буфера: дубликат не выдаётся, PUBREL известного id — PUBCOMP с успехом.
     */
    private fun carriedQos2StateCoversPacketsInTheSameReadAsConnack(version: MQTTVersion) {
        val script = persistent(version)
        script.core.carryIncomingQos2(setOf(7, 8))
        script.start()
        script.sent()
        val connack = if (version == MQTTVersion.MQTT5) {
            script.connack(sessionPresent = true)
        } else {
            script.connack4(ConnectReturnCode.CONNECTION_ACCEPTED, true)
        }
        val out = script.send(
            connack +
                script.publish("t/q2", byteArrayOf(1), Qos.EXACTLY_ONCE, packetId = 7, dup = true) +
                script.pubrel(7) +
                script.pubrel(8),
        )
        assertIs<CoreOutput.Connected>(out.first())
        assertTrue(out.none { it is CoreOutput.Message }, "дубликат не выдаётся: $out")
        assertEquals(CoreState.Connected, script.core.state)
        val sent = script.sent()
        assertEquals(listOf(7u), sent.filterIsInstance<MQTTPubrec>().map { it.packetId })
        val pubcomps = sent.filterIsInstance<MQTTPubcomp>()
        assertEquals(listOf(7u, 8u), pubcomps.map { it.packetId })
        if (version == MQTTVersion.MQTT5) {
            assertEquals(listOf(ReasonCode.SUCCESS, ReasonCode.SUCCESS), pubcomps.map { (it as MQTT5Pubcomp).reasonCode })
        }
        assertTrue(script.core.incomingQos2().isEmpty(), "потоки завершены")
        // Новый PUBLISH с тем же id после PUBREL — новое сообщение
        val fresh = script.send(script.publish("t/q2", byteArrayOf(2), Qos.EXACTLY_ONCE, packetId = 7))
        assertEquals(1, fresh.count { it is CoreOutput.Message })
    }

    @Test
    fun carriedQos2StateCoversPacketsInTheSameReadAsConnackMqtt5() =
        carriedQos2StateCoversPacketsInTheSameReadAsConnack(MQTTVersion.MQTT5)

    @Test
    fun carriedQos2StateCoversPacketsInTheSameReadAsConnackMqtt311() =
        carriedQos2StateCoversPacketsInTheSameReadAsConnack(MQTTVersion.MQTT3_1_1)

    @Test
    fun carriedQos2StateIsDiscardedWhenTheSessionIsNotPresent() {
        val script = persistent()
        script.core.carryIncomingQos2(setOf(7))
        script.start()
        script.sent()
        val out = script.send(
            script.connack(sessionPresent = false) +
                script.publish("t/q2", byteArrayOf(1), Qos.EXACTLY_ONCE, packetId = 7),
        )
        assertEquals(1, out.count { it is CoreOutput.Message }, "сессия новая: id 7 — новое сообщение")
        assertEquals(setOf(7), script.core.incomingQos2())
    }

    @Test
    fun carryIncomingQos2OnlyBeforeConnack() {
        val script = persistent()
        script.connect(script.connack(sessionPresent = true))
        assertFailsWith<IllegalStateException> { script.core.carryIncomingQos2(setOf(1)) }
    }
}
