package io.github.davidepianca98.client

import io.github.davidepianca98.client.internal.MqttClientImpl
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTDisconnect
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPublish
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTSubscribe
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTUnsubscribe
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Connect
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Disconnect
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import io.github.davidepianca98.socket.tls.TLSClientSettings
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Фасад [MqttClient] на виртуальном времени (S06b-T05b, W6 §7.1): [VirtualHost] крутит `runOnce` в `runTest`,
 * [FakeBroker]/[FakeTransport] отвечают пакетами. Сроки (TTL, linger, дописывание DISCONNECT, предел паузы потребителя,
 * переподключение) проверяются точно по виртуальным часам.
 */
class MqttClientVirtualTest {

    private fun options(
        reconnect: ReconnectPolicy = ReconnectPolicy.Never,
        closeLinger: Duration = 3.seconds,
        limits: MqttLimits = MqttLimits(),
        offlineQueue: OfflineQueue = OfflineQueue(),
        will: MqttWill? = null,
        credentials: MqttCredentials? = null,
        logger: MqttLogger = MqttLogger.None,
    ) = MqttClientOptions(
        host = "broker.test",
        port = 1883,
        clientId = "virtual",
        keepAlive = 0.seconds,
        closeLinger = closeLinger,
        reconnect = reconnect,
        limits = limits,
        offlineQueue = offlineQueue,
        will = will,
        credentials = credentials,
        logger = logger,
    )

    private class Rig(val client: MqttClient, val host: VirtualHost) {
        val broker: FakeBroker get() = host.broker
    }

    private fun TestScope.rig(options: MqttClientOptions = options(), broker: FakeBroker = FakeBroker()): Rig {
        val host = VirtualHost(this, broker)
        return Rig(MqttClient(options, host, host), host)
    }

    private suspend fun Rig.connected(): ConnectResult.Connected = assertIs(client.connect())

    /** Собирать `incoming` в список (фоном, до конца теста). */
    private fun TestScope.collect(client: MqttClient): MutableList<MqttIncoming> {
        val items = ArrayList<MqttIncoming>()
        backgroundScope.launch(start = CoroutineStart.UNDISPATCHED) { client.incoming.collect { items += it } }
        return items
    }

    private fun payloads(items: List<MqttIncoming>): List<Int> =
        items.filterIsInstance<MqttIncoming.Message>().map { it.payload.single().toInt() }

    private fun FakeBroker.burst(range: IntRange): ByteArray =
        range.map { script.publish("t/$it", byteArrayOf(it.toByte())) }.reduce { a, b -> a + b }

    // ---- подключение ----

    @Test
    fun connectReturnsConnectedAndCloseEndsWithDisconnectAndEof() = runTest {
        val rig = rig()
        val connected = rig.connected()
        val state = assertIs<ConnectionState.Connected>(rig.client.connection.value)
        assertEquals(connected.epoch, state.epoch)
        assertEquals(listOf("connect", "CONNECT"), rig.broker.log)

        val before = currentTime
        rig.client.close()
        assertEquals(listOf("connect", "CONNECT", "DISCONNECT", "close"), rig.broker.log)
        assertEquals(before, currentTime, "закрытие по EOF брокера, без ожидания linger")
        assertEquals(ConnectionState.Closed(null), rig.client.connection.value)
        assertFalse(rig.host.isRunning)
    }

    @Test
    fun connackRefusalWithNeverIsRefusedAndClosesClient() = runTest {
        val rig = rig(broker = FakeBroker().apply { connackCode = ReasonCode.NOT_AUTHORIZED })
        assertEquals(ConnectResult.Failed(MqttError.Refused(ReasonCode.NOT_AUTHORIZED)), rig.client.connect())
        assertEquals(ConnectionState.Closed(MqttError.Refused(ReasonCode.NOT_AUTHORIZED)), rig.client.connection.value)
        assertTrue(rig.broker.current.closed)
        rig.client.close()
    }

    @Test
    fun connectTimeoutWhenTransportStaysSilent() = runTest {
        val rig = rig(broker = FakeBroker().apply { connect = FakeBroker.ConnectBehavior.Silent })
        val result = rig.client.connect()
        assertEquals(ConnectResult.Failed(MqttError.ConnectTimeout), result)
        assertEquals(30_000L, currentTime, "срок connectTimeout по виртуальным часам")
        rig.client.close()
    }

    // ---- публикации ----

    @Test
    fun publishResultsSentDeliveredAndQos0DeliveredIsSent() = runTest {
        val rig = rig()
        val epoch = rig.connected().epoch
        assertEquals(PublishResult.Sent(epoch), rig.client.publish("a", byteArrayOf(1)))
        assertEquals(PublishResult.Delivered(epoch), rig.client.publish("a", byteArrayOf(2), until = PublishStage.Delivered))
        assertEquals(
            PublishResult.Sent(epoch),
            rig.client.publish("a", byteArrayOf(3), qos = Qos.AT_MOST_ONCE, until = PublishStage.Delivered),
        )
        assertEquals(listOf(1, 2, 3), rig.broker.packets<MQTTPublish>().map { it.payload!!.single().toInt() })
        rig.client.close()
    }

    @Test
    fun brokerRefusalInPubackIsRejected0x87AndSessionSurvives() = runTest {
        val refusedCodes = ArrayList<ReasonCode>()
        val logger = object : MqttLogger {
            override fun publishRefused(code: ReasonCode) {
                refusedCodes += code
            }
        }
        val rig = rig(options(logger = logger), FakeBroker().apply { pubackCode = ReasonCode.NOT_AUTHORIZED })
        rig.connected()
        assertEquals(
            PublishResult.Failed(PublishFailure.Rejected(ReasonCode.NOT_AUTHORIZED)),
            rig.client.publish("a", byteArrayOf(1), until = PublishStage.Delivered),
        )
        assertEquals(listOf(ReasonCode.NOT_AUTHORIZED), refusedCodes)
        assertIs<ConnectionState.Connected>(rig.client.connection.value)
        rig.client.close()
    }

    @Test
    fun unacknowledgedPublishFailsWithConnectionLostWhenTransportDrops() = runTest {
        val rig = rig(options(reconnect = ReconnectPolicy(jitter = 0.0)), FakeBroker().apply { pubackCode = null })
        rig.connected()
        val pending = async { rig.client.publish("a", byteArrayOf(1), until = PublishStage.Delivered) }
        runCurrent()
        assertEquals(1, rig.broker.packets<MQTTPublish>().size)
        rig.broker.fail()
        assertEquals(PublishResult.Failed(PublishFailure.ConnectionLost), pending.await())
        assertIs<ConnectionState.Waiting>(rig.client.connection.value)
        rig.client.close()
    }

    @Test
    fun unacknowledgedPublishFailsWithClosedOnClose() = runTest {
        val rig = rig(broker = FakeBroker().apply { pubackCode = null })
        rig.connected()
        val pending = async { rig.client.publish("a", byteArrayOf(1), until = PublishStage.Delivered) }
        runCurrent()
        rig.client.close()
        assertEquals(PublishResult.Failed(PublishFailure.Closed), pending.await())
        assertEquals(PublishResult.Failed(PublishFailure.Closed), rig.client.publish("a", byteArrayOf(2)))
    }

    @Test
    fun offlinePublishExpiresExactlyAtTtl() = runTest {
        val rig = rig()
        val result = rig.client.publish("a", byteArrayOf(1), ttl = 5.seconds)
        assertEquals(PublishResult.Expired, result)
        assertEquals(5_000L, currentTime)
        assertTrue(rig.broker.transports.isEmpty(), "клиент не запускался — публикация брокеру не уходит")
        rig.client.connect()
        runCurrent()
        assertTrue(rig.broker.packets<MQTTPublish>().isEmpty())
        rig.client.close()
    }

    @Test
    fun cancelledPublishBeforeConnectionLeavesQueue() = runTest {
        val rig = rig(options(offlineQueue = OfflineQueue(ttl = 10.seconds, capacity = 1)))
        val cancelled = launch { rig.client.publish("a", byteArrayOf(1)) }
        runCurrent()
        cancelled.cancel()
        runCurrent()
        // место в очереди освободилось: вторая не QueueFull
        val second = async { rig.client.publish("b", byteArrayOf(2)) }
        runCurrent()
        val epoch = rig.connected().epoch
        assertEquals(PublishResult.Sent(epoch), second.await())
        assertEquals(listOf("b"), rig.broker.packets<MQTTPublish>().map { it.topicName })
        rig.client.close()
    }

    // ---- подписки ----

    @Test
    fun setAwaitAppliedAndRefusalsWithoutBreakingSession() = runTest {
        val broker = FakeBroker().apply { subackCode = { if (it == "deny/#") ReasonCode.NOT_AUTHORIZED else null } }
        val rig = rig(broker = broker)
        val refusals = ArrayList<SubscribeRefused>()
        backgroundScope.launch(start = CoroutineStart.UNDISPATCHED) { rig.client.subscriptions.refusals.collect { refusals += it } }
        rig.connected()
        rig.client.subscriptions.set("g", setOf("ok/#", "deny/#"), Qos.AT_LEAST_ONCE)
        val applied = rig.client.subscriptions.awaitApplied("g")
        val expected = mapOf("ok/#" to TopicState.Granted(Qos.AT_LEAST_ONCE), "deny/#" to TopicState.Refused(ReasonCode.NOT_AUTHORIZED))
        assertEquals(expected, applied)
        assertEquals(expected, rig.client.subscriptions.state.value)
        runCurrent()
        assertEquals(listOf(SubscribeRefused("g", "deny/#", ReasonCode.NOT_AUTHORIZED)), refusals)
        assertIs<ConnectionState.Connected>(rig.client.connection.value)
        rig.client.close()
    }

    @Test
    fun withTemporaryReportsRefusalAndRemovesGroupEvenOnException() = runTest {
        val broker = FakeBroker().apply { subackCode = { if (it == "deny/#") ReasonCode.NOT_AUTHORIZED else null } }
        val rig = rig(broker = broker)
        rig.connected()
        val subs = rig.client.subscriptions

        val refusal = subs.withTemporary(setOf("deny/#")) { it.refused.await() }
        assertEquals(ReasonCode.NOT_AUTHORIZED, refusal.code)
        assertTrue(refusal.group.startsWith("temporary-"))

        assertFailsWith<IllegalStateException> {
            subs.withTemporary(setOf("disc/#")) {
                subs.state.first { it["disc/#"] is TopicState.Granted }
                error("block failed")
            }
        }
        runCurrent()
        // Снятие группы: UNSUBSCRIBE уходит и для отказанного фильтра (поведение SubscriptionSet T03b, безвредно)
        assertEquals(listOf("deny/#", "disc/#"), broker.packets<MQTTUnsubscribe>().flatMap { it.topicFilters })
        assertFalse("disc/#" in subs.state.value)
        rig.client.close()
    }

    @Test
    fun bindFollowsFlowAndRemovesGroupWhenJobEnds() = runTest {
        val rig = rig()
        rig.connected()
        val filters = MutableStateFlow(setOf("a/#"))
        val job = rig.client.subscriptions.bind("b", filters, Qos.AT_MOST_ONCE, backgroundScope)
        runCurrent()
        filters.value = setOf("b/#")
        runCurrent()
        job.cancel()
        runCurrent()
        val subscribed = rig.broker.packets<MQTTSubscribe>().flatMap { p -> p.subscriptions.map { it.topicFilter } }
        val unsubscribed = rig.broker.packets<MQTTUnsubscribe>().flatMap { it.topicFilters }
        assertEquals(listOf("a/#", "b/#"), subscribed)
        assertEquals(listOf("a/#", "b/#"), unsubscribed)
        assertTrue(rig.client.subscriptions.state.value.isEmpty())
        rig.client.close()
    }

    // ---- входящие ----

    @Test
    fun incomingKeepsOrderWithSessionBoundariesAndEpochsGrowAcrossReconnect() = runTest {
        val rig = rig(options(reconnect = ReconnectPolicy(initial = 1.seconds, jitter = 0.0)))
        val items = collect(rig.client)
        val first = rig.connected().epoch
        rig.broker.send(rig.broker.script.publish("t", byteArrayOf(1)))
        runCurrent()
        rig.broker.eof()
        runCurrent()
        assertIs<ConnectionState.Waiting>(rig.client.connection.value)
        advanceTimeBy(1_000)
        runCurrent()
        val second = assertIs<ConnectionState.Connected>(rig.client.connection.value).epoch
        assertTrue(second > first)
        rig.broker.send(rig.broker.script.publish("t", byteArrayOf(2)))
        runCurrent()
        rig.client.close()
        runCurrent()

        assertEquals(6, items.size, "$items")
        assertEquals(MqttIncoming.SessionStarted(first), items[0])
        assertEquals(first, (items[1] as MqttIncoming.Message).epoch)
        val ended = assertIs<MqttIncoming.SessionEnded>(items[2])
        assertEquals(first, ended.epoch)
        assertNotNull(ended.error)
        assertEquals(MqttIncoming.SessionStarted(second), items[3])
        assertEquals(second, (items[4] as MqttIncoming.Message).epoch)
        assertEquals(MqttIncoming.SessionEnded(second, null), items[5])
        assertEquals(listOf(1, 2), payloads(items))
    }

    @Test
    fun partialReadsAreReassembled() = runTest {
        val rig = rig(broker = FakeBroker().apply { chunk = 1 })
        val items = collect(rig.client)
        rig.connected()
        rig.broker.send(rig.broker.burst(0..4))
        runCurrent()
        assertEquals(listOf(0, 1, 2, 3, 4), payloads(items))
        rig.client.close()
    }

    @Test
    fun fullBufferPausesReadingWithholdsBytesAndResumesAfterReceive() = runTest {
        val rig = rig(options(limits = MqttLimits(incomingBuffer = 4)))
        rig.connected()
        rig.broker.send(rig.broker.burst(0..5))
        runCurrent()
        val transport = rig.broker.current
        assertFalse(transport.readEnabled, "буфер 4 полон — чтение приостановлено")
        rig.broker.send(rig.broker.burst(6..8))
        runCurrent()
        assertTrue(transport.withheldBytes > 0, "байты ждут в сокете, а не в памяти клиента")

        val items = rig.client.incoming.take(10).toList()
        assertIs<MqttIncoming.SessionStarted>(items.first())
        assertEquals((0..8).toList(), payloads(items))
        runCurrent()
        assertTrue(transport.readEnabled)
        assertEquals(0, transport.withheldBytes)
        assertTrue(HostAction.PauseRead(true) in rig.host.actions && HostAction.PauseRead(false) in rig.host.actions)
        rig.client.close()
    }

    @Test
    fun consumerStallClosesConnectionAfterLimitAndNothingIsLost() = runTest {
        val rig = rig(options(limits = MqttLimits(incomingBuffer = 2, consumerStallLimit = 5.seconds)))
        val epoch = rig.connected().epoch
        val pausedAt = currentTime
        rig.broker.send(rig.broker.burst(0..4))
        runCurrent()
        assertFalse(rig.broker.current.readEnabled)
        advanceTimeBy(4_999)
        runCurrent()
        assertIs<ConnectionState.Connected>(rig.client.connection.value)
        advanceTimeBy(1)
        runCurrent()
        assertEquals(pausedAt + 5_000, currentTime)
        assertEquals(ConnectionState.Closed(MqttError.ConsumerStalled), rig.client.connection.value)
        assertTrue(rig.broker.current.closed)

        val items = rig.client.incoming.take(7).toList()
        assertEquals((0..4).toList(), payloads(items))
        assertEquals(MqttIncoming.SessionEnded(epoch, MqttError.ConsumerStalled), items.last())
        rig.client.close()
    }

    @Test
    fun afterConsumerStallReconnectWaitsForConsumerThenResumes() = runTest {
        val rig = rig(
            options(
                reconnect = ReconnectPolicy(initial = 1.seconds, jitter = 0.0),
                limits = MqttLimits(incomingBuffer = 2, consumerStallLimit = 5.seconds),
            ),
        )
        val first = rig.connected().epoch
        rig.broker.send(rig.broker.burst(0..4))
        runCurrent()
        advanceTimeBy(5_000)
        runCurrent()
        assertEquals(MqttError.ConsumerStalled, assertIs<ConnectionState.Waiting>(rig.client.connection.value).lastError)
        advanceTimeBy(1_000)
        runCurrent()
        // Новое подключение начинается с паузой чтения: потребитель всё ещё не забрал старое — CONNACK ждёт в сокете
        val second = rig.broker.current
        assertEquals(2, rig.broker.transports.size)
        assertFalse(second.readEnabled)
        assertIs<ConnectionState.Connecting>(rig.client.connection.value)

        val items = collect(rig.client)
        runCurrent()
        assertTrue(second.readEnabled)
        val connected = assertIs<ConnectionState.Connected>(rig.client.connection.value)
        assertTrue(connected.epoch > first)
        assertEquals((0..4).toList(), payloads(items))
        assertEquals(MqttIncoming.SessionEnded(first, MqttError.ConsumerStalled), items[6])
        assertEquals(MqttIncoming.SessionStarted(connected.epoch), items[7])
        rig.client.close()
    }

    // ---- закрытие ----

    @Test
    fun closeAfterPublishWritesPublishThenDisconnectThenClosesOnEof() = runTest {
        val rig = rig()
        val epoch = rig.connected().epoch
        val publish = async(start = CoroutineStart.UNDISPATCHED) { rig.client.publish("bye", byteArrayOf(1), retain = true) }
        rig.client.close()
        assertEquals(PublishResult.Sent(epoch), publish.await())
        assertEquals(listOf("connect", "CONNECT", "PUBLISH", "DISCONNECT", "close"), rig.broker.log)
    }

    @Test
    fun closeWaitsLingerWhenBrokerKeepsConnectionOpen() = runTest {
        val rig = rig(options(closeLinger = 2.seconds), FakeBroker().apply { eofOnDisconnect = false })
        rig.connected()
        val start = currentTime
        rig.client.close()
        assertEquals(start + 2_000, currentTime)
        assertEquals(listOf("connect", "CONNECT", "DISCONNECT", "close"), rig.broker.log)
    }

    @Test
    fun protocolErrorSendsDisconnectWithCodeAndClosesTransportOnEof() = runTest {
        val rig = rig()
        val epoch = rig.connected().epoch
        val items = collect(rig.client)
        rig.broker.send(byteArrayOf(0x00, 0x00)) // тип 0 — некорректный пакет
        runCurrent()
        val disconnect = assertIs<MQTT5Disconnect>(rig.broker.packets<MQTTDisconnect>().single())
        assertEquals(ReasonCode.MALFORMED_PACKET, disconnect.reasonCode)
        assertEquals(listOf("connect", "CONNECT", "DISCONNECT", "close"), rig.broker.log)
        assertEquals(ConnectionState.Closed(MqttError.Protocol(ReasonCode.MALFORMED_PACKET)), rig.client.connection.value)
        assertEquals(MqttIncoming.SessionEnded(epoch, MqttError.Protocol(ReasonCode.MALFORMED_PACKET)), items.last())
        rig.client.close()
    }

    @Test
    fun protocolErrorClosesTransportRightAfterDisconnectIsWrittenWithoutEof() = runTest {
        // S06b-T05f: придержания 500 мс больше нет — остаток пуст, FlushAndClose закрывает сразу
        val rig = rig(broker = FakeBroker().apply { eofOnDisconnect = false })
        rig.connected()
        val start = currentTime
        rig.broker.send(byteArrayOf(0x00, 0x00))
        runCurrent()
        assertEquals(1, rig.broker.packets<MQTTDisconnect>().size)
        assertTrue(rig.broker.current.closed)
        assertEquals(start, currentTime)
        assertEquals(listOf("connect", "CONNECT", "DISCONNECT", "close"), rig.broker.log)
        assertEquals(HostAction.FlushAndClose(MqttClientImpl.CLOSE_FLUSH_MS), rig.host.actions.last())
        rig.client.close()
    }

    @Test
    fun protocolErrorDisconnectTailIsFlushedUnderSlowWritesBeforeClose() = runTest {
        val rig = rig(broker = FakeBroker().apply { eofOnDisconnect = false })
        rig.connected()
        val transport = rig.broker.current
        transport.drainPerMs = 1 // DISCONNECT(code) уходит по байту в мс
        val start = currentTime
        rig.broker.send(byteArrayOf(0x00, 0x00))
        runCurrent()
        assertTrue(transport.pendingWriteBytes > 0, "остаток DISCONNECT ещё в очереди")
        assertFalse(transport.closed, "закрытие ждёт остаток")
        assertIs<ConnectionState.Closed>(rig.client.connection.value)
        advanceTimeBy(10) // DISCONNECT(code) MQTT 5 — несколько байт
        runCurrent()
        assertTrue(transport.closed, "закрыт сразу после дописывания, задолго до срока")
        val disconnect = assertIs<MQTT5Disconnect>(rig.broker.packets<MQTTDisconnect>().single())
        assertEquals(ReasonCode.MALFORMED_PACKET, disconnect.reasonCode)
        assertEquals(listOf("connect", "CONNECT", "DISCONNECT", "close"), rig.broker.log)
        assertEquals(start + 10, currentTime)
        rig.client.close()
    }

    // ---- закрытие: дописывание остатка (S06b-T05f) ----

    @Test
    fun closeWithZeroLingerAndEmptyBufferIsFasterThan100ms() = runTest {
        val rig = rig(options(closeLinger = Duration.ZERO), FakeBroker().apply { eofOnDisconnect = false })
        rig.connected()
        val start = currentTime
        rig.client.close()
        assertTrue(currentTime - start < 100, "close() занял ${currentTime - start} мс виртуального времени")
        assertEquals(listOf("connect", "CONNECT", "DISCONNECT", "close"), rig.broker.log)
        assertEquals(ConnectionState.Closed(null), rig.client.connection.value)
        assertFalse(rig.host.isRunning)
    }

    @Test
    fun zeroLingerCloseFlushesPublishAndDisconnectUnderSlowWritesBeforeClosing() = runTest {
        // H2 + дописывание: публикация и close() подряд, брокер читает 64 Б/мс — PUBLISH и DISCONNECT целиком до close
        val rig = rig(options(closeLinger = Duration.ZERO), FakeBroker().apply { eofOnDisconnect = false })
        val epoch = rig.connected().epoch
        val transport = rig.broker.current
        transport.drainPerMs = 64
        val payload = ByteArray(32 * 1024) { it.toByte() }
        val start = currentTime
        val publish = async(start = CoroutineStart.UNDISPATCHED) { rig.client.publish("big", payload, qos = Qos.AT_MOST_ONCE) }
        rig.client.close()
        assertEquals(PublishResult.Sent(epoch), publish.await())
        assertEquals(listOf("connect", "CONNECT", "PUBLISH", "DISCONNECT", "close"), rig.broker.log)
        assertContentEquals(payload, rig.broker.packets<MQTTPublish>().single().payload!!.toByteArray())
        val elapsed = currentTime - start
        assertTrue(elapsed in 500..MqttClientImpl.CLOSE_FLUSH_MS, "дописывание ≈ 512 мс, факт $elapsed")
        assertTrue(transport.closed)
        assertFalse(rig.host.isRunning)
    }

    @Test
    fun flushDeadlineClosesWithoutHangingWhenBrokerNeverReads() = runTest {
        val rig = rig(options(closeLinger = Duration.ZERO), FakeBroker().apply { eofOnDisconnect = false })
        rig.connected()
        val transport = rig.broker.current
        transport.drainPerMs = 0
        val start = currentTime
        rig.client.close()
        assertEquals(start + MqttClientImpl.CLOSE_FLUSH_MS, currentTime, "закрытие по сроку дописывания")
        assertTrue(transport.closed)
        assertEquals(listOf("connect", "CONNECT", "close"), rig.broker.log, "DISCONNECT не дошёл — остаток отброшен по сроку")
        assertEquals(ConnectionState.Closed(null), rig.client.connection.value)
        assertFalse(rig.host.isRunning)
    }

    @Test
    fun lingerCloseUnderSlowWritesEndsOnBrokerEofAfterWholeDisconnect() = runTest {
        // H1: linger 3 с, брокер закрывает соединение, только получив DISCONNECT целиком (EOF по разбору)
        val rig = rig(options(closeLinger = 3.seconds))
        val epoch = rig.connected().epoch
        val transport = rig.broker.current
        transport.drainPerMs = 16
        val start = currentTime
        val publish = async(start = CoroutineStart.UNDISPATCHED) {
            rig.client.publish("retained/clear", ByteArray(8 * 1024), retain = true, qos = Qos.AT_MOST_ONCE)
        }
        rig.client.close()
        assertEquals(PublishResult.Sent(epoch), publish.await())
        assertEquals(listOf("connect", "CONNECT", "PUBLISH", "DISCONNECT", "close"), rig.broker.log)
        val elapsed = currentTime - start
        assertTrue(elapsed in 500..1_000, "закрытие по EOF после дописывания ≈ 513 мс, факт $elapsed")
        assertEquals(ConnectionState.Closed(null), rig.client.connection.value)
    }

    @Test
    fun reconnectDoesNotCutTheFlushOfThePreviousTransport() = runTest {
        // Ошибка протокола → DISCONNECT(code) дописывается на старом транспорте, пока движок переподключается
        val rig = rig(options(reconnect = ReconnectPolicy(initial = 10.milliseconds, jitter = 0.0)), FakeBroker().apply { eofOnDisconnect = false })
        rig.connected()
        val first = rig.broker.current
        first.drainPerMs = 0
        rig.broker.send(byteArrayOf(0x00, 0x00))
        runCurrent()
        advanceTimeBy(50)
        runCurrent()
        assertEquals(2, rig.broker.transports.size, "переподключение идёт")
        assertFalse(first.closed, "старый транспорт ещё дописывает")
        first.drainPerMs = 1_000
        rig.host.wakeup()
        advanceTimeBy(5)
        runCurrent()
        assertTrue(first.closed)
        assertEquals(ReasonCode.MALFORMED_PACKET, assertIs<MQTT5Disconnect>(rig.broker.packets<MQTTDisconnect>().first()).reasonCode)
        val closeIndex = rig.broker.log.indexOf("close")
        assertTrue(rig.broker.log.indexOf("DISCONNECT") in 0 until closeIndex, rig.broker.log.toString())
        rig.client.close()
    }

    @Test
    fun closeBeforeTransportConnectedAbortsWithoutDisconnect() = runTest {
        val rig = rig(broker = FakeBroker().apply { connect = FakeBroker.ConnectBehavior.Silent })
        rig.client.start()
        runCurrent()
        val start = currentTime
        rig.client.close()
        assertEquals(start, currentTime, "H5: закрытие во время подключения — сразу")
        assertEquals(listOf("connect", "close"), rig.broker.log)
        assertEquals(ConnectionState.Closed(null), rig.client.connection.value)
    }

    @Test
    fun closeWhileAwaitingConnackAbortsWithoutDisconnect() = runTest {
        val rig = rig(broker = FakeBroker().apply { connackCode = null })
        val connecting = async { rig.client.connect() }
        runCurrent()
        assertEquals(listOf("connect", "CONNECT"), rig.broker.log)
        val start = currentTime
        rig.client.close()
        assertEquals(start, currentTime)
        assertEquals(listOf("connect", "CONNECT", "close"), rig.broker.log)
        assertIs<ConnectResult.Failed>(connecting.await())
    }

    // ---- опции ----

    @Test
    fun willAndCredentialsFromOptionsGoIntoConnect() = runTest {
        val will = MqttWill("dev/lwt", "offline".encodeToByteArray(), retain = true, qos = Qos.AT_LEAST_ONCE)
        val rig = rig(options(will = will, credentials = MqttCredentials("user", "pw".encodeToByteArray())))
        rig.connected()
        val connect = rig.broker.packets<MQTT5Connect>().single()
        assertTrue(connect.connectFlags.willFlag)
        assertTrue(connect.connectFlags.willRetain)
        assertEquals(Qos.AT_LEAST_ONCE, connect.connectFlags.willQos)
        assertEquals("dev/lwt", connect.willTopic)
        assertContentEquals("offline".encodeToByteArray(), connect.willPayload!!.asByteArray())
        assertNotNull(connect.willProperties)
        assertEquals("user", connect.userName)
        assertContentEquals("pw".encodeToByteArray(), connect.password!!.asByteArray())
        rig.client.close()
    }

    @Test
    fun optionsToStringHasNoPasswordOrAddress() {
        val text = options(credentials = MqttCredentials("login", "secret-pw".encodeToByteArray())).toString()
        for (secret in listOf("secret-pw", "login", "broker.test", "1883")) assertFalse(secret in text, text)
    }

    @Test
    fun tlsOptionsWithForeignHostAreRefusedAtCreation() = runTest {
        val host = VirtualHost(this)
        val error = assertFailsWith<IllegalArgumentException> {
            MqttClient(options().copy(tls = TLSClientSettings(clientCertificatePassword = "tls-pass")), host, host)
        }
        assertFalse("tls-pass" in error.message.orEmpty() || "broker.test" in error.message.orEmpty())
        assertFalse(host.isRunning)
        assertEquals(0, host.steps)
    }

    // ---- нагрузка ----

    @Test
    fun thousandStartCloseCyclesLeaveNoRunningHostsOrOpenTransports() = runTest {
        val hosts = ArrayList<VirtualHost>()
        repeat(1_000) { i ->
            val rig = rig()
            hosts += rig.host
            when (i % 3) {
                0 -> Unit // закрытие до запуска
                1 -> {
                    rig.connected()
                }
                else -> rig.client.start() // закрытие во время подключения
            }
            rig.client.close()
            assertEquals(ConnectionState.Closed(null), rig.client.connection.value)
        }
        runCurrent()
        assertTrue(hosts.none { it.isRunning })
        assertTrue(hosts.flatMap { it.broker.transports }.all { it.closed })
        assertEquals(333, hosts.count { h -> "DISCONNECT" in h.broker.log })
    }
}
