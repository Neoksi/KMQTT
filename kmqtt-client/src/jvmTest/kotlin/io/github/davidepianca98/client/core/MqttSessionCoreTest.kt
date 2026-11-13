package io.github.davidepianca98.client.core

import io.github.davidepianca98.client.DropReason
import io.github.davidepianca98.client.MqttError
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.MQTTPacket
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqttv4.ConnectReturnCode
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Connect
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Disconnect
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pingreq
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Puback
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Publish
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Subscribe
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Connect
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Disconnect
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pingreq
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Properties
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Puback
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubcomp
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Publish
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubrec
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubrel
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Subscribe
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Unsubscribe
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class MqttSessionCoreTest {

    private val v4 = MQTTVersion.MQTT3_1_1
    private val v5 = MQTTVersion.MQTT5

    private fun settings(
        version: MQTTVersion,
        keepAliveSec: Int = 10,
        will: WillSpec? = null,
        credentials: SessionCredentials? = null,
        maxIncomingPacket: Int = 64 * 1024,
        maxTopicLength: Int = 1024,
        topicAliasMaximum: Int = 0,
        closeLingerMs: Long = 3_000,
        connackTimeoutMs: Long = 30_000,
        cleanStart: Boolean = true,
    ) = SessionSettings(
        version = version,
        clientId = "test-client",
        credentials = credentials,
        keepAliveSec = keepAliveSec,
        cleanStart = cleanStart,
        will = will,
        connackTimeoutMs = connackTimeoutMs,
        closeLingerMs = closeLingerMs,
        maxIncomingPacket = maxIncomingPacket,
        maxTopicLength = maxTopicLength,
        topicAliasMaximum = topicAliasMaximum,
    )

    private fun script(version: MQTTVersion, settings: SessionSettings = settings(version), now: Long = 0) =
        BrokerScript(version, settings, now)

    private fun BrokerScript.connected(): BrokerScript {
        val out = connect()
        assertIs<CoreOutput.Connected>(out.single())
        assertEquals(CoreState.Connected, core.state)
        return this
    }

    private fun List<CoreOutput>.closed(): MqttError? = assertIs<CoreOutput.Closed>(single()).error

    private inline fun <reified T : MQTTPacket> List<MQTTPacket>.only(): T = assertIs<T>(single())

    // ---- CONNECT / CONNACK ----

    @Test
    fun connect311WithoutWill() {
        val s = script(v4)
        assertNull(s.core.nextDeadline(), "до start() просыпаться незачем")
        assertTrue(s.start().isEmpty())
        val connect = s.sent().only<MQTT4Connect>()
        assertEquals("test-client", connect.clientID)
        assertEquals(10, connect.keepAlive)
        assertFalse(connect.connectFlags.willFlag)
        assertFalse(connect.connectFlags.userNameFlag)
        assertTrue(connect.connectFlags.cleanStart)
        assertEquals(CoreState.Connecting, s.core.state)

        val info = assertIs<CoreOutput.Connected>(s.send(s.connack()).single()).connack
        assertFalse(info.sessionPresent)
        assertNull(info.serverKeepAliveSec)
        assertEquals(65_535, info.receiveMaximum)
        assertEquals(CoreState.Connected, s.core.state)
    }

    @Test
    fun connect311WithWillAndCredentials() {
        val will = WillSpec("/devices/dev/meta/error", "r".encodeToByteArray(), retain = true, qos = Qos.AT_LEAST_ONCE)
        val creds = SessionCredentials("user", "secret".encodeToByteArray())
        val s = script(v4, settings(v4, will = will, credentials = creds))
        s.start()
        val connect = s.sent().only<MQTT4Connect>()
        assertTrue(connect.connectFlags.willFlag)
        assertTrue(connect.connectFlags.willRetain)
        assertEquals(Qos.AT_LEAST_ONCE, connect.connectFlags.willQos)
        assertEquals("/devices/dev/meta/error", connect.willTopic)
        assertContentEquals("r".encodeToByteArray(), connect.willPayload!!.asByteArray())
        assertEquals("user", connect.userName)
        assertContentEquals("secret".encodeToByteArray(), connect.password!!.asByteArray())
    }

    @Test
    fun connect5WithWillHasWillPropertiesAndMaximumPacketSize() {
        val will = WillSpec("/devices/dev/meta/error", "r".encodeToByteArray(), retain = true)
        val s = script(v5, settings(v5, will = will, maxIncomingPacket = 200_000))
        s.start()
        val connect = s.sent().only<MQTT5Connect>()
        assertEquals(200_000u, connect.properties.maximumPacketSize)
        assertNull(connect.properties.receiveMaximum, "65535 не объявляется")
        assertNull(connect.properties.topicAliasMaximum, "алиасы по умолчанию не объявляются")
        assertNotNull(connect.willProperties, "MQTT 5: пустые willProperties")
        assertEquals("/devices/dev/meta/error", connect.willTopic)
        assertTrue(connect.connectFlags.willRetain)
    }

    @Test
    fun connect5WithoutWill() {
        val s = script(v5)
        s.start()
        val connect = s.sent().only<MQTT5Connect>()
        assertFalse(connect.connectFlags.willFlag)
        assertEquals(64u * 1024u, connect.properties.maximumPacketSize)
        assertIs<CoreOutput.Connected>(s.send(s.connack()).single())
    }

    @Test
    fun connack5RefusedClosesWithRefused() {
        val s = script(v5)
        s.start()
        s.sent()
        assertEquals(MqttError.Refused(ReasonCode.NOT_AUTHORIZED), s.send(s.connack(ReasonCode.NOT_AUTHORIZED)).closed())
        assertEquals(CoreState.Closed, s.core.state)
        assertNull(s.core.takeOutbound())
        assertNull(s.core.nextDeadline())
    }

    @Test
    fun connack5ServerMovedWithReferenceIsRefusedWithoutRedirect() {
        val s = script(v5)
        s.start()
        s.sent()
        val connack = s.connack(ReasonCode.SERVER_MOVED, properties = MQTT5Properties(serverReference = "other:1883"))
        val error = s.send(connack).closed()
        assertEquals(MqttError.Refused(ReasonCode.SERVER_MOVED), error)
        assertFalse(error.toString().contains("other"), "без адреса (H11)")
    }

    @Test
    fun connack311RefusedMapsReturnCode() {
        val s = script(v4)
        s.start()
        s.sent()
        val error = s.send(s.connack4(ConnectReturnCode.BAD_USERNAME_PASSWORD)).closed()
        assertEquals(MqttError.Refused(ReasonCode.BAD_USER_NAME_OR_PASSWORD), error)
    }

    @Test
    fun sessionPresentWithCleanStartIsProtocolError() {
        val s = script(v5)
        s.start()
        s.sent()
        assertEquals(MqttError.Protocol(ReasonCode.PROTOCOL_ERROR), s.send(s.connack(sessionPresent = true)).closed())
        val disconnect = s.sent().only<MQTT5Disconnect>()
        assertEquals(ReasonCode.PROTOCOL_ERROR, disconnect.reasonCode)
    }

    @Test
    fun packetBeforeConnackIsProtocolError() {
        val s = script(v4)
        s.start()
        s.sent()
        assertEquals(MqttError.Protocol(ReasonCode.PROTOCOL_ERROR), s.send(s.publish("/a", byteArrayOf(1))).closed())
        assertTrue(s.sent().isEmpty(), "3.1.1: DISCONNECT при ошибке не шлётся (отменил бы will)")
    }

    @Test
    fun connackTimeout() {
        val s = script(v5, settings(v5, connackTimeoutMs = 15_000), now = 100)
        s.start()
        assertEquals(15_100, s.core.nextDeadline())
        assertTrue(s.advanceTo(15_099).isEmpty())
        assertEquals(MqttError.ConnackTimeout, s.advanceTo(15_100).closed())
    }

    @Test
    fun serverKeepAliveIsApplied() {
        val s = script(v5, settings(v5, keepAliveSec = 60))
        val out = s.connect(s.connack(properties = MQTT5Properties(serverKeepAlive = 10u)))
        assertEquals(10, assertIs<CoreOutput.Connected>(out.single()).connack.serverKeepAliveSec)
        assertEquals(9_000, s.core.nextDeadline(), "PINGREQ на 0,9 × keepAlive брокера")
    }

    // ---- публикации ----

    @Test
    fun publishQos0AndQos1WithPubackReasonCode() {
        val s = script(v5).connected()
        assertEquals(PublishAdmission.Accepted(null), s.core.publish(OutgoingPublish("/a", byteArrayOf(1)), s.now))
        val p0 = s.sent().only<MQTT5Publish>()
        assertEquals("/a", p0.topicName)
        assertEquals(Qos.AT_MOST_ONCE, p0.qos)

        val admission = s.core.publish(OutgoingPublish("/b", byteArrayOf(2), Qos.AT_LEAST_ONCE, retain = true), s.now)
        val id = assertNotNull(assertIs<PublishAdmission.Accepted>(admission).packetId)
        val p1 = s.sent().only<MQTT5Publish>()
        assertEquals(id.toUInt(), p1.packetId)
        assertTrue(p1.retain)

        assertEquals(CoreOutput.PubAck(id, ReasonCode.NOT_AUTHORIZED), s.send(s.puback(id, ReasonCode.NOT_AUTHORIZED)).single())
        assertTrue(s.send(s.puback(id)).isEmpty(), "повторный PUBACK по уже подтверждённому id — без события")
    }

    @Test
    fun publish311Qos1() {
        val s = script(v4).connected()
        val id = assertNotNull(
            assertIs<PublishAdmission.Accepted>(s.core.publish(OutgoingPublish("/x", byteArrayOf(), Qos.AT_LEAST_ONCE), s.now)).packetId,
        )
        s.sent().only<MQTT4Publish>()
        assertEquals(CoreOutput.PubAck(id, ReasonCode.SUCCESS), s.send(s.puback(id)).single())
    }

    @Test
    fun publishQos2Flow() {
        val s = script(v5).connected()
        val id = assertNotNull(
            assertIs<PublishAdmission.Accepted>(s.core.publish(OutgoingPublish("/q2", byteArrayOf(1), Qos.EXACTLY_ONCE), s.now)).packetId,
        )
        s.sent()
        assertTrue(s.send(s.pubrec(id)).isEmpty())
        assertEquals(id.toUInt(), s.sent().only<MQTT5Pubrel>().packetId)
        assertEquals(CoreOutput.PubAck(id, ReasonCode.SUCCESS), s.send(s.pubcomp(id)).single())
    }

    @Test
    fun flowControlledAtServerReceiveMaximum() {
        val s = script(v5)
        s.connect(s.connack(properties = MQTT5Properties(receiveMaximum = 2u)))
        val a = s.core.publish(OutgoingPublish("/a", byteArrayOf(), Qos.AT_LEAST_ONCE), s.now)
        val b = s.core.publish(OutgoingPublish("/b", byteArrayOf(), Qos.AT_LEAST_ONCE), s.now)
        assertIs<PublishAdmission.Accepted>(a)
        assertIs<PublishAdmission.Accepted>(b)
        assertEquals(PublishAdmission.FlowControlled, s.core.publish(OutgoingPublish("/c", byteArrayOf(), Qos.AT_LEAST_ONCE), s.now))
        assertIs<PublishAdmission.Accepted>(s.core.publish(OutgoingPublish("/d", byteArrayOf()), s.now), "QoS 0 окном не ограничен")
        assertEquals(3, s.sent().size)

        s.send(s.puback(a.packetId!!))
        assertIs<PublishAdmission.Accepted>(s.core.publish(OutgoingPublish("/c", byteArrayOf(), Qos.AT_LEAST_ONCE), s.now))
    }

    @Test
    fun publishRejections() {
        val before = script(v5)
        before.start()
        assertEquals(
            PublishAdmission.Rejected(PublishRejection.NotConnected),
            before.core.publish(OutgoingPublish("/a", byteArrayOf()), before.now),
        )

        val s = script(v5)
        s.connect(s.connack(properties = MQTT5Properties(maximumQos = 0u, retainAvailable = 0u, maximumPacketSize = 64u)))
        s.sent()
        assertEquals(
            PublishAdmission.Rejected(PublishRejection.QosNotSupported),
            s.core.publish(OutgoingPublish("/a", byteArrayOf(), Qos.AT_LEAST_ONCE), s.now),
        )
        assertEquals(
            PublishAdmission.Rejected(PublishRejection.RetainNotSupported),
            s.core.publish(OutgoingPublish("/a", byteArrayOf(), retain = true), s.now),
        )
        val tooLarge = s.core.publish(OutgoingPublish("/a", ByteArray(100)), s.now)
        assertEquals(64, assertIs<PublishRejection.TooLarge>(assertIs<PublishAdmission.Rejected>(tooLarge).reason).limit)
        assertEquals(
            PublishAdmission.Rejected(PublishRejection.InvalidTopic),
            s.core.publish(OutgoingPublish("/a/+", byteArrayOf()), s.now),
        )
        assertEquals(
            PublishAdmission.Rejected(PublishRejection.InvalidTopic),
            s.core.publish(OutgoingPublish("", byteArrayOf()), s.now),
        )
        assertTrue(s.sent().isEmpty(), "ничего не ушло")
        assertEquals(CoreState.Connected, s.core.state)
    }

    // ---- подписки ----

    @Test
    fun subackWithPartialRefusal311() {
        val s = script(v4).connected()
        val id = s.core.subscribe(
            listOf(SubscriptionRequest("/a/#"), SubscriptionRequest("/secret/#"), SubscriptionRequest("/b", Qos.AT_LEAST_ONCE)),
            s.now,
        )
        val subscribe = s.sent().only<MQTT4Subscribe>()
        assertEquals(id.toUInt(), subscribe.packetIdentifier)
        assertEquals(listOf("/a/#", "/secret/#", "/b"), subscribe.subscriptions.map { it.topicFilter })
        assertEquals(Qos.AT_LEAST_ONCE, subscribe.subscriptions[2].options.qos)

        val out = s.send(s.suback(id, listOf(ReasonCode.SUCCESS, ReasonCode.UNSPECIFIED_ERROR, ReasonCode.GRANTED_QOS1)))
        assertEquals(
            CoreOutput.SubAck(id, listOf(ReasonCode.SUCCESS, ReasonCode.UNSPECIFIED_ERROR, ReasonCode.GRANTED_QOS1)),
            out.single(),
        )
        assertEquals(CoreState.Connected, s.core.state, "отказ SUBACK сессию не рвёт")
    }

    @Test
    fun subackWithPartialRefusal5AndUnsuback() {
        val s = script(v5).connected()
        val id = s.core.subscribe(listOf(SubscriptionRequest("/a"), SubscriptionRequest("/b")), s.now)
        s.sent().only<MQTT5Subscribe>()
        assertEquals(
            CoreOutput.SubAck(id, listOf(ReasonCode.SUCCESS, ReasonCode.NOT_AUTHORIZED)),
            s.send(s.suback(id, listOf(ReasonCode.SUCCESS, ReasonCode.NOT_AUTHORIZED))).single(),
        )
        val unsubId = s.core.unsubscribe(listOf("/a"), s.now)
        assertEquals(listOf("/a"), s.sent().only<MQTT5Unsubscribe>().topicFilters)
        assertEquals(CoreOutput.UnsubAck(unsubId, listOf(ReasonCode.SUCCESS)), s.send(s.unsuback(unsubId)).single())
    }

    @Test
    fun subscribeBeforeConnackThrows() {
        val s = script(v5)
        s.start()
        assertFailsWith<IllegalStateException> { s.core.subscribe(listOf(SubscriptionRequest("/a")), s.now) }
    }

    @Test
    fun packetIdsSkipThoseInUse() {
        val s = script(v5).connected()
        val ids = (1..5).map { s.core.subscribe(listOf(SubscriptionRequest("/t$it")), s.now) }
        assertEquals(ids.size, ids.toSet().size)
        val pub = assertIs<PublishAdmission.Accepted>(s.core.publish(OutgoingPublish("/p", byteArrayOf(), Qos.AT_LEAST_ONCE), s.now))
        assertFalse(pub.packetId in ids)
    }

    // ---- входящие ----

    @Test
    fun incomingQos1IsAcknowledgedWhenMessageIsEmitted() {
        val s = script(v4).connected()
        val msg = assertIs<CoreOutput.Message>(
            s.send(s.publish("/devices/dev/controls/t", "21.5".encodeToByteArray(), Qos.AT_LEAST_ONCE, packetId = 7, retain = true)).single(),
        )
        assertEquals("/devices/dev/controls/t", msg.topic)
        assertContentEquals("21.5".encodeToByteArray(), msg.payload)
        assertTrue(msg.retain)
        assertEquals(7u, s.sent().only<MQTT4Puback>().packetId)
        assertFalse(msg.toString().contains("devices"), "toString без топика (H11)")
    }

    @Test
    fun incomingQos2Flow() {
        val s = script(v5).connected()
        assertIs<CoreOutput.Message>(s.send(s.publish("/q", byteArrayOf(1), Qos.EXACTLY_ONCE, packetId = 4)).single())
        assertEquals(4u, s.sent().only<MQTT5Pubrec>().packetId)
        assertTrue(s.send(s.publish("/q", byteArrayOf(1), Qos.EXACTLY_ONCE, packetId = 4)).isEmpty(), "повтор не выдаётся")
        s.sent()
        assertTrue(s.send(s.pubrel(4)).isEmpty())
        assertEquals(4u, s.sent().only<MQTT5Pubcomp>().packetId)
    }

    @Test
    fun topicAliases5() {
        val s = script(v5, settings(v5, topicAliasMaximum = 10))
        s.start()
        assertEquals(10u, s.sent().only<MQTT5Connect>().properties.topicAliasMaximum)
        s.send(s.connack())

        val first = assertIs<CoreOutput.Message>(s.send(s.publish("/devices/a", byteArrayOf(1), alias = 3)).single())
        assertEquals("/devices/a", first.topic)
        val second = assertIs<CoreOutput.Message>(s.send(s.publish("", byteArrayOf(2), alias = 3)).single())
        assertEquals("/devices/a", second.topic)
        assertContentEquals(byteArrayOf(2), second.payload)

        // перепривязка алиаса
        s.send(s.publish("/devices/b", byteArrayOf(3), alias = 3))
        assertEquals("/devices/b", assertIs<CoreOutput.Message>(s.send(s.publish("", byteArrayOf(4), alias = 3)).single()).topic)

        // неизвестный алиас без топика — нарушение протокола
        assertEquals(MqttError.Protocol(ReasonCode.PROTOCOL_ERROR), s.send(s.publish("", byteArrayOf(5), alias = 4)).closed())
    }

    @Test
    fun topicAliasAboveMaximumIsInvalid() {
        val s = script(v5, settings(v5, topicAliasMaximum = 2)).connected()
        assertEquals(MqttError.Protocol(ReasonCode.TOPIC_ALIAS_INVALID), s.send(s.publish("/a", byteArrayOf(), alias = 3)).closed())
        assertEquals(ReasonCode.TOPIC_ALIAS_INVALID, s.sent().only<MQTT5Disconnect>().reasonCode)

        val noAliases = script(v5).connected()
        assertEquals(
            MqttError.Protocol(ReasonCode.TOPIC_ALIAS_INVALID),
            noAliases.send(noAliases.publish("/a", byteArrayOf(), alias = 1)).closed(),
            "алиасы не объявлены (максимум 0) — брокер не вправе их слать",
        )
    }

    @Test
    fun topicTooLongWithAliasesClearsAliasTable() {
        val s = script(v5, settings(v5, topicAliasMaximum = 10, maxTopicLength = 16)).connected()
        s.send(s.publish("/short", byteArrayOf(1), alias = 2))

        // Длинный топик с алиасом 2 (перепривязка) отброшен: старая привязка 2 → /short недостоверна
        val dropped = s.send(s.publish("/a/very/long/topic/name", byteArrayOf(2), Qos.AT_LEAST_ONCE, packetId = 5, alias = 2))
        assertIs<DropReason.TopicTooLong>(assertIs<CoreOutput.Dropped>(dropped.single()).reason)
        assertEquals(5u, s.sent().only<MQTT5Puback>().packetId, "отброшенный QoS 1 подтверждён")

        val next = s.send(s.publish("", byteArrayOf(3), Qos.AT_LEAST_ONCE, packetId = 6, alias = 2))
        val reason = assertIs<DropReason.TopicTooLong>(assertIs<CoreOutput.Dropped>(next.single()).reason)
        assertEquals(-1, reason.length)
        assertEquals(6u, s.sent().only<MQTT5Puback>().packetId)
        assertEquals(CoreState.Connected, s.core.state)

        // брокер снова привязал алиас к допустимому топику
        s.send(s.publish("/short2", byteArrayOf(4), alias = 2))
        assertEquals("/short2", assertIs<CoreOutput.Message>(s.send(s.publish("", byteArrayOf(5), alias = 2)).single()).topic)
    }

    @Test
    fun packetLargerThanLimitIsDroppedAndAcknowledged() {
        val s = script(v4, settings(v4, maxIncomingPacket = 1024)).connected()
        val out = s.send(s.publish("/big", ByteArray(2000), Qos.AT_LEAST_ONCE, packetId = 9))
        val reason = assertIs<DropReason.TooLarge>(assertIs<CoreOutput.Dropped>(out.single()).reason)
        assertEquals(1024, reason.limit)
        assertEquals(9u, s.sent().only<MQTT4Puback>().packetId)
        assertEquals(CoreState.Connected, s.core.state, "сессия жива")
        assertIs<CoreOutput.Message>(s.send(s.publish("/small", byteArrayOf(1))).single())
    }

    @Test
    fun bytesInPiecesAndSeveralPacketsAtOnce() {
        val s = script(v4).connected()
        val all = s.publish("/a", byteArrayOf(1)) + s.publish("/b", byteArrayOf(2)) + s.pingresp()
        val out = ArrayList<CoreOutput>()
        for (i in all.indices) out += s.core.onBytes(all, i, 1, s.now)
        assertEquals(listOf("/a", "/b"), out.map { assertIs<CoreOutput.Message>(it).topic })
    }

    @Test
    fun garbageIsProtocolErrorWithDisconnectIn5() {
        val s = script(v5).connected()
        assertEquals(MqttError.Protocol(ReasonCode.MALFORMED_PACKET), s.send(byteArrayOf(0x00, 0x00)).closed())
        assertEquals(ReasonCode.MALFORMED_PACKET, s.sent().only<MQTT5Disconnect>().reasonCode)
    }

    @Test
    fun incomingAuthIsProtocolError() {
        val s = script(v5).connected()
        assertEquals(MqttError.Protocol(ReasonCode.PROTOCOL_ERROR), s.send(s.auth5()).closed())
    }

    // ---- keepAlive ----

    @Test
    fun keepAliveHalfOpen() {
        val s = script(v5, settings(v5, keepAliveSec = 10)).connected()
        assertEquals(9_000, s.core.nextDeadline(), "PINGREQ на 0,9 × keepAlive от последней отправки (CONNECT)")
        assertTrue(s.advanceTo(8_999).isEmpty())
        assertTrue(s.sent().isEmpty())
        assertTrue(s.advanceTo(9_000).isEmpty())
        s.sent().only<MQTT5Pingreq>()
        assertEquals(14_000, s.core.nextDeadline(), "ответ ждём ½ keepAlive")
        assertTrue(s.advanceTo(13_999).isEmpty())
        assertTrue(s.sent().isEmpty(), "пока ждём ответа, второй PINGREQ не шлётся")
        assertEquals(MqttError.KeepAliveTimeout, s.advanceTo(14_000).closed())
        assertNull(s.core.nextDeadline())
    }

    @Test
    fun keepAlivePingrespKeepsSessionAlive() {
        val s = script(v4, settings(v4, keepAliveSec = 10)).connected()
        s.advanceTo(9_000)
        s.sent().only<MQTT4Pingreq>()
        s.now = 9_100
        assertTrue(s.send(s.pingresp()).isEmpty())
        // следующий PINGREQ: 0,9 × keepAlive от отправки PINGREQ
        assertEquals(18_000, s.core.nextDeadline())
        assertTrue(s.advanceTo(18_000).isEmpty())
        s.sent().only<MQTT4Pingreq>()
        assertEquals(CoreState.Connected, s.core.state)
    }

    @Test
    fun keepAlivePingOnBrokerSilence() {
        val s = script(v5, settings(v5, keepAliveSec = 10)).connected()
        // клиент шлёт сам, брокер молчит: PINGREQ по молчанию брокера (keepAlive от последнего приёма)
        s.now = 5_000
        s.core.publish(OutgoingPublish("/a", byteArrayOf()), s.now)
        s.sent()
        assertEquals(10_000, s.core.nextDeadline())
        s.advanceTo(10_000)
        s.sent().only<MQTT5Pingreq>()
    }

    @Test
    fun keepAliveWhileReadPaused() {
        val s = script(v5, settings(v5, keepAliveSec = 10)).connected()
        s.now = 1_000
        s.core.setReadPaused(true, s.now)
        assertEquals(9_000, s.core.nextDeadline())
        s.advanceTo(9_000)
        s.sent().only<MQTT5Pingreq>()
        assertEquals(18_000, s.core.nextDeadline(), "на паузе: следующий PINGREQ, таймаута ответа нет")
        s.advanceTo(14_000)
        assertEquals(CoreState.Connected, s.core.state)
        s.advanceTo(18_000)
        s.sent().only<MQTT5Pingreq>()
        s.advanceTo(27_000)
        s.sent().only<MQTT5Pingreq>()
        s.advanceTo(30_000)
        assertEquals(CoreState.Connected, s.core.state, "3 × keepAlive на паузе — сессия жива")

        s.core.setReadPaused(false, s.now)
        assertEquals(35_000, s.core.nextDeadline(), "после паузы отсчёт ответа заново")
        assertTrue(s.advanceTo(34_999).isEmpty())
        assertEquals(MqttError.KeepAliveTimeout, s.advanceTo(35_000).closed())
    }

    @Test
    fun keepAliveAfterPauseAnswered() {
        val s = script(v5, settings(v5, keepAliveSec = 10)).connected()
        s.core.setReadPaused(true, s.now)
        s.advanceTo(9_000)
        s.sent()
        s.now = 20_000
        s.core.setReadPaused(false, s.now)
        s.now = 20_010
        s.send(s.publish("/a", byteArrayOf()) + s.pingresp())
        assertEquals(CoreState.Connected, s.advanceTo(25_000).let { s.core.state })
    }

    @Test
    fun keepAliveZeroDisablesPing() {
        val s = script(v5, settings(v5, keepAliveSec = 0)).connected()
        assertNull(s.core.nextDeadline())
        s.advanceTo(10_000_000)
        assertTrue(s.sent().isEmpty())
    }

    // ---- закрытие ----

    @Test
    fun disconnectThenEofClosesGracefully() {
        val s = script(v5).connected()
        s.now = 2_000
        assertTrue(s.core.disconnect(ReasonCode.SUCCESS, s.now).isEmpty())
        assertEquals(CoreState.Closing, s.core.state)
        assertEquals(ReasonCode.SUCCESS, s.sent().only<MQTT5Disconnect>().reasonCode)
        assertEquals(5_000, s.core.nextDeadline(), "linger 3 с")

        // в Closing входящие читаются и отбрасываются: ни событий, ни PUBACK
        assertTrue(s.send(s.publish("/a", byteArrayOf(1), Qos.AT_LEAST_ONCE, packetId = 3) + s.puback(1)).isEmpty())
        assertTrue(s.sent().isEmpty())
        assertEquals(PublishAdmission.Rejected(PublishRejection.NotConnected), s.core.publish(OutgoingPublish("/a", byteArrayOf()), s.now))
        assertTrue(s.core.disconnect(ReasonCode.SUCCESS, s.now).isEmpty(), "повторный disconnect — ничего")

        assertNull(s.core.onEof(s.now).closed())
        assertEquals(CoreState.Closed, s.core.state)
    }

    @Test
    fun closingEndsByLingerDeadline() {
        val s = script(v4).connected()
        s.now = 2_000
        s.core.disconnect(ReasonCode.SUCCESS, s.now)
        s.sent().only<MQTT4Disconnect>()
        assertTrue(s.advanceTo(4_999).isEmpty())
        assertNull(s.advanceTo(5_000).closed())
        assertNull(s.core.nextDeadline())
    }

    @Test
    fun disconnectWithZeroLingerClosesAtOnceKeepingDisconnect() {
        val s = script(v5, settings(v5, closeLingerMs = 0)).connected()
        assertNull(s.core.disconnect(ReasonCode.SUCCESS, s.now).closed())
        s.sent().only<MQTT5Disconnect>()
    }

    @Test
    fun serverDisconnectServerMovedIsServerDisconnect() {
        val s = script(v5).connected()
        val error = s.send(s.disconnect5(ReasonCode.SERVER_MOVED, serverReference = "elsewhere:1883")).closed()
        assertEquals(MqttError.ServerDisconnect(ReasonCode.SERVER_MOVED), error)
        assertNull(s.core.takeOutbound())
    }

    @Test
    fun eofWhileConnectedIsIoError() {
        val s = script(v5).connected()
        assertEquals(MqttError.Io("eof", null), s.core.onEof(s.now).closed())
    }

    @Test
    fun closedIsEmittedOnceAndCoreIsInert() {
        val s = script(v5).connected()
        s.core.onEof(s.now).closed()
        assertTrue(s.core.onEof(s.now).isEmpty())
        assertTrue(s.advanceTo(1_000_000).isEmpty())
        assertTrue(s.send(s.publish("/a", byteArrayOf())).isEmpty())
        assertTrue(s.core.disconnect(ReasonCode.SUCCESS, s.now).isEmpty())
        assertNull(s.core.nextDeadline())
        assertNull(s.core.takeOutbound())
    }

    @Test
    fun nextDeadlineIsAlwaysTheNearest() {
        val s = script(v5, settings(v5, keepAliveSec = 10, connackTimeoutMs = 5_000, closeLingerMs = 3_000))
        s.start()
        assertEquals(5_000, s.core.nextDeadline(), "Connecting: CONNACK")
        s.now = 1_000
        s.send(s.connack())
        assertEquals(9_000, s.core.nextDeadline(), "Connected: keepAlive")
        s.now = 4_000
        s.core.publish(OutgoingPublish("/a", byteArrayOf()), s.now)
        assertEquals(11_000, s.core.nextDeadline(), "молчание брокера раньше 0,9 от отправки (13 000)")
        s.advanceTo(11_000)
        assertEquals(16_000, s.core.nextDeadline(), "ожидание ответа на PINGREQ")
        s.now = 12_000
        s.core.disconnect(ReasonCode.SUCCESS, s.now)
        assertEquals(15_000, s.core.nextDeadline(), "Closing: linger, keepAlive не считается")
    }

    // ---- безопасность ----

    @Test
    fun secretsAreNotInStringsAndErrors() {
        val password = "p4ssw0rd-SECRET"
        val will = WillSpec("/devices/dev/meta/error", "will-payload-SECRET".encodeToByteArray())
        val settings = settings(v5, will = will, credentials = SessionCredentials("login-SECRET", password.encodeToByteArray()))
        val text = settings.toString()
        for (secret in listOf("p4ssw0rd", "login-SECRET", "will-payload", "/devices/dev")) {
            assertFalse(text.contains(secret), "SessionSettings.toString не содержит $secret")
        }
        assertFalse(OutgoingPublish("/devices/x", "payload".encodeToByteArray()).toString().contains("/devices"))
        assertFalse(SubscriptionRequest("/devices/#").toString().contains("/devices"))

        val e = assertFailsWith<IllegalArgumentException> {
            SessionSettings(v5, "c", SessionCredentials(null, password.encodeToByteArray()), keepAliveSec = 10)
        }
        assertFalse(e.message.orEmpty().contains("p4ssw0rd"))

        // ошибки ядра не несут топиков и нагрузок
        val s = script(v5, settings(v5, topicAliasMaximum = 1)).connected()
        val error = s.send(s.publish("/devices/secret-topic", "secret-payload".encodeToByteArray(), alias = 5)).closed()
        assertFalse(error.toString().contains("secret"))
        assertFalse(s.events.joinToString().contains("secret"))
    }

    @Test
    fun settingsValidation() {
        assertFailsWith<IllegalArgumentException> { SessionSettings(v4, null, keepAliveSec = 65_536) }
        assertFailsWith<IllegalArgumentException> { SessionSettings(v4, null, keepAliveSec = -1) }
        assertFailsWith<IllegalArgumentException> {
            SessionSettings(v4, null, SessionCredentials(null, byteArrayOf(1)), keepAliveSec = 10)
        }
        assertFailsWith<IllegalArgumentException> {
            SessionSettings(v4, null, keepAliveSec = 10, will = WillSpec("", byteArrayOf()))
        }
        assertFailsWith<IllegalArgumentException> {
            SessionSettings(v4, null, keepAliveSec = 10, receiveMaximum = 0)
        }
        // логин без пароля — допустимо
        SessionSettings(v4, null, SessionCredentials("user", null), keepAliveSec = 10)
    }

    @Test
    fun randomClientIdWhenNull() {
        val s = BrokerScript(v4, SessionSettings(v4, null, keepAliveSec = 10), 0)
        s.start()
        assertTrue(s.sent().only<MQTT4Connect>().clientID.isNotEmpty())
    }
}
