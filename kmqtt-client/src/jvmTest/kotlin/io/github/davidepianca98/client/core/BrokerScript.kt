package io.github.davidepianca98.client.core

import io.github.davidepianca98.mqtt.MQTTCurrentPacket
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.ConnectAcknowledgeFlags
import io.github.davidepianca98.mqtt.packets.MQTTPacket
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqttv4.ConnectReturnCode
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Connack
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pingresp
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Puback
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pubcomp
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Publish
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pubrec
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pubrel
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Suback
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Unsuback
import io.github.davidepianca98.mqtt.packets.mqttv4.toSubackReturnCodes
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Auth
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Connack
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Disconnect
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pingresp
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Properties
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Puback
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubcomp
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Publish
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubrec
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubrel
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Suback
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Unsuback
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode

/**
 * Сторона брокера для тестов ядра: байты пакетов брокера собираются сериализаторами `kmqtt-common`, исходящее ядра
 * разбирается `MQTTCurrentPacket` (как это делает брокер). Плюс виртуальные часы [now].
 */
internal class BrokerScript(
    val version: MQTTVersion,
    settings: SessionSettings = SessionSettings(version, clientId = "test-client", keepAliveSec = 10),
    var now: Long = 1_000,
) {
    val core = MqttSessionCore(settings, now)
    private val parser = MQTTCurrentPacket(16u * 1024u * 1024u, version)
    private val mqtt5 = version == MQTTVersion.MQTT5

    /** Всё, что ядро выдало событиями за время сценария. */
    val events = ArrayList<CoreOutput>()

    fun start(): List<CoreOutput> = core.start(now).also { events += it }

    /** Скормить ядру байты брокера (целиком). */
    fun send(bytes: ByteArray): List<CoreOutput> = core.onBytes(bytes, 0, bytes.size, now).also { events += it }

    fun tick(): List<CoreOutput> = core.onTick(now).also { events += it }

    fun advanceTo(time: Long): List<CoreOutput> {
        now = time
        return tick()
    }

    /** Исходящие пакеты ядра с прошлого вызова. */
    fun sent(): List<MQTTPacket> {
        val bytes = core.takeOutbound() ?: return emptyList()
        return parser.addData(bytes.asUByteArray())
    }

    /** start + CONNECT забран + CONNACK с успехом. */
    fun connect(connack: ByteArray = connack()): List<CoreOutput> {
        start()
        sent()
        return send(connack)
    }

    // ---- пакеты брокера ----

    fun connack(
        code: ReasonCode = ReasonCode.SUCCESS,
        sessionPresent: Boolean = false,
        properties: MQTT5Properties = MQTT5Properties(),
    ): ByteArray = if (mqtt5) {
        MQTT5Connack(ConnectAcknowledgeFlags(sessionPresent), code, properties).bytes()
    } else {
        connack4(if (code == ReasonCode.SUCCESS) ConnectReturnCode.CONNECTION_ACCEPTED else error("use connack4"))
    }

    fun connack4(code: ConnectReturnCode, sessionPresent: Boolean = false): ByteArray =
        MQTT4Connack(ConnectAcknowledgeFlags(sessionPresent), code).bytes()

    fun publish(
        topic: String,
        payload: ByteArray,
        qos: Qos = Qos.AT_MOST_ONCE,
        packetId: Int? = null,
        retain: Boolean = false,
        alias: Int? = null,
        dup: Boolean = false,
    ): ByteArray = if (mqtt5) {
        MQTT5Publish(retain, qos, dup, topic, packetId?.toUInt(), MQTT5Properties(topicAlias = alias?.toUInt()), payload.asUByteArray(), 0L).bytes()
    } else {
        MQTT4Publish(retain, qos, dup, topic, packetId?.toUInt(), payload.asUByteArray(), 0L).bytes()
    }

    fun puback(id: Int, code: ReasonCode = ReasonCode.SUCCESS): ByteArray =
        if (mqtt5) MQTT5Puback(id.toUInt(), code).bytes() else MQTT4Puback(id.toUInt()).bytes()

    fun pubrec(id: Int, code: ReasonCode = ReasonCode.SUCCESS): ByteArray =
        if (mqtt5) MQTT5Pubrec(id.toUInt(), code).bytes() else MQTT4Pubrec(id.toUInt()).bytes()

    fun pubrel(id: Int): ByteArray = if (mqtt5) MQTT5Pubrel(id.toUInt()).bytes() else MQTT4Pubrel(id.toUInt()).bytes()

    fun pubcomp(id: Int): ByteArray = if (mqtt5) MQTT5Pubcomp(id.toUInt()).bytes() else MQTT4Pubcomp(id.toUInt()).bytes()

    fun suback(id: Int, codes: List<ReasonCode>): ByteArray = if (mqtt5) {
        MQTT5Suback(id.toUInt(), codes).bytes()
    } else {
        MQTT4Suback(id.toUInt(), codes.toSubackReturnCodes()).bytes()
    }

    fun unsuback(id: Int, codes: List<ReasonCode> = listOf(ReasonCode.SUCCESS)): ByteArray =
        if (mqtt5) MQTT5Unsuback(id.toUInt(), codes).bytes() else MQTT4Unsuback(id.toUInt()).bytes()

    fun pingresp(): ByteArray = if (mqtt5) MQTT5Pingresp().bytes() else MQTT4Pingresp().bytes()

    fun disconnect5(code: ReasonCode, serverReference: String? = null): ByteArray =
        MQTT5Disconnect(code, MQTT5Properties(serverReference = serverReference)).bytes()

    fun auth5(
        code: ReasonCode = ReasonCode.CONTINUE_AUTHENTICATION,
        method: String = "m",
        data: ByteArray? = null,
    ): ByteArray =
        MQTT5Auth(code, MQTT5Properties(authenticationMethod = method, authenticationData = data?.asUByteArray())).bytes()

    private fun MQTTPacket.bytes(): ByteArray = toByteArray().asByteArray()
}
