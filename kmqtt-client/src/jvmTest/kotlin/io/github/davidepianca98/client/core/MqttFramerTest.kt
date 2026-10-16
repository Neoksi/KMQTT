package io.github.davidepianca98.client.core

import io.github.davidepianca98.client.DropReason
import io.github.davidepianca98.client.MqttError
import io.github.davidepianca98.mqtt.MQTTException
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.ConnectAcknowledgeFlags
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqttv4.ConnectReturnCode
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Connack
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Publish
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Suback
import io.github.davidepianca98.mqtt.packets.mqttv4.SubackReturnCode
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Properties
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Puback
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Publish
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

class MqttFramerTest {

    private fun publish4(
        topic: String,
        payload: ByteArray,
        qos: Qos = Qos.AT_MOST_ONCE,
        packetId: Int? = null,
        retain: Boolean = false,
    ): ByteArray = MQTT4Publish(retain, qos, false, topic, packetId?.toUInt(), payload.asUByteArray())
        .toByteArray().asByteArray()

    private fun framer4(max: Int = 64 * 1024, maxTopic: Int = 1024, initial: Int = 16 * 1024) =
        MqttFramer(MQTTVersion.MQTT3_1_1, max, maxTopic, initial)

    private fun MqttFramer.feedAll(bytes: ByteArray): List<FramerOutput> = feed(bytes, 0, bytes.size)

    private fun MqttFramer.feedByByte(bytes: ByteArray): List<FramerOutput> {
        val out = ArrayList<FramerOutput>()
        for (i in bytes.indices) out += feed(bytes, i, 1)
        return out
    }

    private fun MqttFramer.feedChunks(bytes: ByteArray, chunk: Int): List<FramerOutput> {
        val out = ArrayList<FramerOutput>()
        var off = 0
        while (off < bytes.size) {
            val n = minOf(chunk, bytes.size - off)
            out += feed(bytes, off, n)
            off += n
        }
        return out
    }

    private fun FramerOutput.asPublish(): IncomingPublish = assertIs<FramerOutput.Publish>(this).publish

    @Test
    fun publish311ByteByByte() {
        val payload = "21.5".encodeToByteArray()
        val bytes = publish4("/devices/wb/controls/t", payload, Qos.AT_LEAST_ONCE, packetId = 7, retain = true)
        val framer = framer4()
        val out = framer.feedByByte(bytes)
        assertEquals(1, out.size)
        val p = out.single().asPublish()
        assertEquals("/devices/wb/controls/t", p.topic)
        assertEquals(7, p.packetId)
        assertEquals(Qos.AT_LEAST_ONCE, p.qos)
        assertTrue(p.retain)
        assertFalse(p.dup)
        assertNull(p.topicAlias)
        assertContentEquals(payload, p.payload)
        assertFalse(p.toString().contains("devices"), "toString без топика (H11)")
    }

    @Test
    fun threePublishesInOneChunk() {
        val bytes = publish4("a/b", byteArrayOf(1)) +
            publish4("c/d", byteArrayOf(2, 3), Qos.AT_LEAST_ONCE, packetId = 1) +
            publish4("e", ByteArray(0), Qos.EXACTLY_ONCE, packetId = 65535)
        val out = framer4().feedAll(bytes).map { it.asPublish() }
        assertEquals(listOf("a/b", "c/d", "e"), out.map { it.topic })
        assertEquals(listOf(null, 1, 65535), out.map { it.packetId })
        assertEquals(listOf(Qos.AT_MOST_ONCE, Qos.AT_LEAST_ONCE, Qos.EXACTLY_ONCE), out.map { it.qos })
        assertContentEquals(byteArrayOf(2, 3), out[1].payload)
        assertEquals(0, out[2].payload.size)
    }

    @Test
    fun nonPublishPacketsGoThroughCommonDeserializers() {
        val connack = MQTT4Connack(ConnectAcknowledgeFlags(true), ConnectReturnCode.CONNECTION_ACCEPTED)
            .toByteArray().asByteArray()
        val suback = MQTT4Suback(5u, listOf(SubackReturnCode.MAXIMUM_QOS0, SubackReturnCode.FAILURE))
            .toByteArray().asByteArray()
        val out = framer4().feedByByte(connack + suback)
        assertEquals(2, out.size)
        val c = assertIs<MQTT4Connack>(assertIs<FramerOutput.Packet>(out[0]).packet)
        assertTrue(c.connectAcknowledgeFlags.sessionPresentFlag)
        val s = assertIs<MQTT4Suback>(assertIs<FramerOutput.Packet>(out[1]).packet)
        assertEquals(5u, s.packetIdentifier)
        assertEquals(listOf(SubackReturnCode.MAXIMUM_QOS0, SubackReturnCode.FAILURE), s.reasonCodes)

        val puback = MQTT5Puback(9u, ReasonCode.NOT_AUTHORIZED).toByteArray().asByteArray()
        val out5 = MqttFramer(MQTTVersion.MQTT5, 1024, 64).feedAll(puback)
        val pa = assertIs<MQTT5Puback>(assertIs<FramerOutput.Packet>(out5.single()).packet)
        assertEquals(9u, pa.packetId)
        assertEquals(ReasonCode.NOT_AUTHORIZED, pa.reasonCode)
    }

    @Test
    fun publish5WithTopicAlias() {
        val withTopic = MQTT5Publish(
            retain = false,
            qos = Qos.AT_LEAST_ONCE,
            topicName = "zigbee2mqtt/lamp",
            packetId = 3u,
            properties = MQTT5Properties(
                payloadFormatIndicator = 1u,
                messageExpiryInterval = 60u,
                contentType = "text/plain",
                responseTopic = "r/t",
                correlationData = ubyteArrayOf(1u, 2u),
                topicAlias = 4u,
            ).apply {
                subscriptionIdentifier += 300u
                addUserProperty("k" to "v")
            },
            payload = "{\"state\":\"ON\"}".encodeToByteArray().asUByteArray(),
        ).toByteArray().asByteArray()
        val aliasOnly = MQTT5Publish(
            retain = true,
            topicName = "",
            packetId = null,
            properties = MQTT5Properties(topicAlias = 4u),
            payload = byteArrayOf(9).asUByteArray(),
        ).toByteArray().asByteArray()

        val framer = MqttFramer(MQTTVersion.MQTT5, 64 * 1024, 1024)
        val out = framer.feedByByte(withTopic) + framer.feedAll(aliasOnly)
        val first = out[0].asPublish()
        assertEquals("zigbee2mqtt/lamp", first.topic)
        assertEquals(4, first.topicAlias)
        assertEquals(3, first.packetId)
        assertContentEquals("{\"state\":\"ON\"}".encodeToByteArray(), first.payload)
        val second = out[1].asPublish()
        assertEquals("", second.topic)
        assertEquals(4, second.topicAlias)
        assertTrue(second.retain)
        assertNull(second.packetId)
        assertContentEquals(byteArrayOf(9), second.payload)
    }

    @Test
    fun tooLargePacketIsSkippedStreamingOneChunk() = checkTooLarge { framer, bytes -> framer.feedAll(bytes) }

    @Test
    fun tooLargePacketIsSkippedStreamingByByte() = checkTooLarge { framer, bytes -> framer.feedByByte(bytes) }

    @Test
    fun tooLargePacketIsSkippedStreamingOddChunks() = checkTooLarge { framer, bytes -> framer.feedChunks(bytes, 1013) }

    private fun checkTooLarge(feed: (MqttFramer, ByteArray) -> List<FramerOutput>) {
        val limit = 64 * 1024
        val big = publish4("big/topic", ByteArray(70_000) { it.toByte() }, Qos.AT_LEAST_ONCE, packetId = 42)
        val next = publish4("next", byteArrayOf(5), Qos.AT_LEAST_ONCE, packetId = 43)
        val framer = framer4(max = limit)
        val out = feed(framer, big + next)
        assertEquals(2, out.size)
        val dropped = assertIs<FramerOutput.Dropped>(out[0])
        assertEquals(DropReason.TooLarge(big.size, limit), dropped.reason)
        assertEquals(42, dropped.packetId)
        assertEquals(Qos.AT_LEAST_ONCE, dropped.qos)
        val p = out[1].asPublish()
        assertEquals("next", p.topic)
        assertEquals(43, p.packetId)
        assertTrue(framer.capacity <= limit + 5, "capacity=${framer.capacity}")
        assertTrue(framer.capacity <= 16 * 1024, "большой пакет не буферизуется: capacity=${framer.capacity}")
    }

    @Test
    fun tooLargeQos0HasNoPacketId() {
        val big = publish4("t", ByteArray(2_000))
        val out = framer4(max = 1024, initial = 256).feedChunks(big + publish4("u", byteArrayOf(1)), 100)
        val dropped = assertIs<FramerOutput.Dropped>(out[0])
        assertNull(dropped.packetId)
        assertEquals(Qos.AT_MOST_ONCE, dropped.qos)
        assertEquals("u", out[1].asPublish().topic)
    }

    @Test
    fun bufferStaysBoundedAfterManyPackets() {
        val limit = 64 * 1024
        val one = publish4("devices/x/controls/y", ByteArray(100 - 2 - 2 - 20) { 1 })
        assertEquals(100, one.size)
        val all = ByteArray(one.size * 10_000)
        for (i in 0 until 10_000) one.copyInto(all, i * one.size)

        for (feed in listOf<(MqttFramer) -> List<FramerOutput>>(
            { it.feedAll(all) },
            { it.feedChunks(all, 333) },
            { it.feedByByte(all) },
        )) {
            val framer = framer4(max = limit)
            val out = feed(framer)
            assertEquals(10_000, out.size)
            assertTrue(out.all { it.asPublish().topic == "devices/x/controls/y" })
            assertTrue(framer.capacity <= limit + 5, "capacity=${framer.capacity}")
        }
    }

    @Test
    fun bufferGrowsForPacketUnderLimitButNotBeyond() {
        val limit = 50_000
        val payload = ByteArray(40_000) { (it % 7).toByte() }
        val framer = framer4(max = limit, initial = 1024)
        val out = framer.feedChunks(publish4("g", payload), 4096)
        assertContentEquals(payload, out.single().asPublish().payload)
        assertTrue(framer.capacity in 40_000..(limit + 5), "capacity=${framer.capacity}")
    }

    @Test
    fun topicLongerThanLimitIsDropped() {
        val framer = framer4(maxTopic = 16)
        val long = publish4("a".repeat(17), byteArrayOf(1), Qos.AT_LEAST_ONCE, packetId = 11)
        val ok = publish4("a".repeat(16), byteArrayOf(2))
        val out = framer.feedAll(long + ok)
        val dropped = assertIs<FramerOutput.Dropped>(out[0])
        assertEquals(DropReason.TopicTooLong(17, 16), dropped.reason)
        assertEquals(11, dropped.packetId)
        assertEquals("a".repeat(16), out[1].asPublish().topic)
    }

    @Test
    fun garbageThrows() {
        // Длина из 5 байтов с битом продолжения.
        assertProtocolError(ReasonCode.MALFORMED_PACKET, byteArrayOf(0x30, -1, -1, -1, -1, 0x01))
        // Тип 0 (Reserved).
        assertProtocolError(ReasonCode.MALFORMED_PACKET, byteArrayOf(0x00, 0x00))
        // SUBSCRIBE от брокера.
        assertProtocolError(ReasonCode.PROTOCOL_ERROR, byteArrayOf(0x82.toByte(), 0x00))
        // AUTH в 3.1.1.
        assertProtocolError(ReasonCode.PROTOCOL_ERROR, byteArrayOf(0xF0.toByte(), 0x00))
        // PUBLISH с QoS 3.
        assertProtocolError(ReasonCode.MALFORMED_PACKET, byteArrayOf(0x36, 0x03, 0x00, 0x01, 0x61))
        // PUBLISH: длина топика больше пакета.
        assertProtocolError(ReasonCode.MALFORMED_PACKET, byteArrayOf(0x30, 0x03, 0x00, 0x05, 0x61))
        // Битый CONNACK (лишний флаг) — ошибка десериализатора kmqtt-common.
        assertProtocolError(ReasonCode.MALFORMED_PACKET, byteArrayOf(0x20, 0x02, 0x07, 0x00))
        // Топик с wildcard.
        assertProtocolError(ReasonCode.TOPIC_NAME_INVALID, publish4Raw("a/#"))
    }

    private fun publish4Raw(topic: String): ByteArray {
        val t = topic.encodeToByteArray()
        return byteArrayOf(0x30, (2 + t.size).toByte(), 0, t.size.toByte()) + t
    }

    private fun assertProtocolError(code: ReasonCode, bytes: ByteArray) {
        val e = assertFailsWith<MQTTException> { framer4().feedAll(bytes) }
        assertEquals(code, e.reasonCode)
        val e1 = assertFailsWith<MQTTException> { framer4().feedByByte(bytes) }
        assertEquals(code, e1.reasonCode)
    }

    @Test
    fun errorsDoNotLeakAddresses() {
        val io = MqttError.Io("connect", RuntimeException("failed to connect to 192.168.1.10:1883"))
        assertFalse(io.toString().contains("192.168"), io.toString())
        assertTrue(io.toString().contains("RuntimeException"))
        assertEquals("TopicTooLong(length=5, limit=4)", DropReason.TopicTooLong(5, 4).toString())
    }
}
