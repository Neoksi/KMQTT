package io.github.davidepianca98.client.engine

import io.github.davidepianca98.client.ConnectionState
import io.github.davidepianca98.client.MqttError
import io.github.davidepianca98.client.MqttIncoming
import io.github.davidepianca98.client.PublishFailure
import io.github.davidepianca98.client.PublishResult
import io.github.davidepianca98.client.PublishStage
import io.github.davidepianca98.client.ReconnectPolicy
import io.github.davidepianca98.client.TransportEvent
import io.github.davidepianca98.client.core.BrokerScript
import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.core.SessionSettings
import io.github.davidepianca98.mqtt.MQTTCurrentPacket
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.MQTTPacket
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTConnect
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTDisconnect
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPingreq
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPublish
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTSubscribe
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTUnsubscribe
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Properties
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Движок [MqttClientEngine] на виртуальном времени (S06b-T03a): перенесённые сценарии `MqttReconnectTest` приложения
 * (задержки точно, без шага цикла), оффлайн-очередь, закрытие (H1/H2), эпохи, `maxAttempts`, H11.
 * Брокер — [Rig]: отвечает на пакеты движка байтами [BrokerScript] синхронно, время двигает только тест.
 */
@OptIn(MqttCoreApi::class)
class EngineReconnectTest {

    // ---- перенос MqttReconnectTest (10 сценариев) ----

    @Test
    fun backoffDoublesUpTo10sAndResetsAfterTheWholeSetIsSubscribed() {
        val rig = Rig()
        rig.online = false
        rig.start()
        rig.advance(50_000)
        assertEquals(listOf(1_000L, 2_000, 4_000, 8_000, 10_000, 10_000, 10_000), rig.gaps())

        rig.online = true
        rig.advance(10_000)
        assertEquals(1, rig.started().size, "connected")
        rig.markConnectTime()

        rig.drop()
        rig.advance(1_100)
        assertEquals(listOf(1_000L), rig.gaps(), "after SUBACK of the set the delay starts from 1 s again")
        assertEquals(2, rig.started().size)
    }

    @Test
    fun refusedSubscriptionKeepsTheSessionButOnlyTimeResetsTheBackoff() {
        val rig = Rig()
        rig.suback = Suback.REJECTED
        rig.online = false
        rig.start()
        rig.advance(7_500) // попытки в 0, 1, 3, 7 с: следующая задержка — 8 с
        rig.online = true
        rig.advanceUntil { rig.started().isNotEmpty() }

        rig.advance(5_000)
        assertEquals(1, rig.started().size)
        assertTrue(rig.ended().isEmpty(), "the refusal does not end the session")
        assertTrue(rig.logs().any { it is EngineLogEvent.Refused }, "refusal logged")

        // Обрыв до 30 с — задержка растёт дальше (10 с)
        rig.markConnectTime()
        rig.drop()
        rig.advanceUntil { rig.started().size >= 2 }
        assertEquals(listOf(10_000L), rig.gaps(), "not reset")

        // 31 с с отказом — стабильна по времени: после обрыва снова 1 с
        rig.advance(31_000)
        rig.markConnectTime()
        rig.drop()
        rig.advanceUntil { rig.started().size >= 3 }
        assertEquals(listOf(1_000L), rig.gaps(), "reset after 30 s")
    }

    /**
     * Замена `withFailSessionOnSubackErrorTheRefusalEndsTheSession`: флаг `failSessionOnSubackError` в новом клиенте
     * не переносится (W6 §5, отказ SUBACK сессию не рвёт — H7). Та же проверка «каждая попытка кончается сразу —
     * задержка растёт» — на отказе брокера в CONNACK.
     */
    @Test
    fun connackRefusedOnEveryAttemptKeepsTheDelayGrowingWithoutSessions() {
        val rig = Rig()
        rig.connackCode = ReasonCode.NOT_AUTHORIZED
        rig.start()
        rig.advance(26_000)

        assertEquals(listOf(1_000L, 2_000, 4_000, 8_000, 10_000), rig.gaps())
        assertTrue(rig.started().isEmpty() && rig.ended().isEmpty(), "no session without CONNACK success")
        val waiting = rig.states().filterIsInstance<ConnectionState.Waiting>().last()
        assertEquals(MqttError.Refused(ReasonCode.NOT_AUTHORIZED), waiting.lastError)
        assertEquals(10_000.milliseconds, waiting.delay)
    }

    @Test
    fun shortSessionsWithoutSubackDoNotResetTheBackoff() {
        val rig = Rig()
        rig.suback = Suback.SILENT
        rig.start()
        repeat(5) { i ->
            rig.advanceUntil { rig.started().size > i }
            rig.advance(100)
            rig.drop()
        }
        rig.advanceUntil { rig.started().size >= 6 }

        val nominal = listOf(1_000L, 2_000, 4_000, 8_000, 10_000)
        assertEquals(nominal.map { it + 100 }, rig.gaps(), "the delay keeps growing, counted from the end of the session")
    }

    @Test
    fun onlyASessionThatLivedLongEnoughResetsTheBackoffWithoutSuback() {
        val rig = Rig()
        rig.suback = Suback.SILENT
        rig.online = false
        rig.start()
        rig.advance(7_500)
        rig.online = true
        rig.advanceUntil { rig.started().isNotEmpty() }

        // 29 с без SUBACK — не стабильна: после обрыва задержка растёт дальше (10 с)
        rig.advance(29_000)
        rig.markConnectTime()
        rig.drop()
        rig.advanceUntil { rig.started().size >= 2 }
        assertEquals(listOf(10_000L), rig.gaps(), "not reset")

        // 31 с — стабильна: после обрыва снова 1 с
        rig.advance(31_000)
        rig.markConnectTime()
        rig.drop()
        rig.advanceUntil { rig.started().size >= 3 }
        assertEquals(listOf(1_000L), rig.gaps(), "reset after 30 s")
    }

    @Test
    fun jitterOnlyShortensTheDelay() {
        val policy = ReconnectPolicy(random = { 0.999 })
        val delays = List(7) { policy.delayMs(it) }
        val nominal = listOf(1_000L, 2_000, 4_000, 8_000, 10_000, 10_000, 10_000)
        delays.zip(nominal).forEach { (d, n) -> assertTrue(d in (n * 0.8).toLong()..n, "$d of $n") }
        assertTrue(policy.delayMs(0) <= 1_000)
        // MAX_SHIFT: огромный номер попытки не переполняет сдвиг
        assertEquals(10_000L, ReconnectPolicy(random = { 0.0 }).delayMs(1_000))
        assertEquals(10_000L, ReconnectPolicy(random = { 0.0 }).delayMs(19))
    }

    @Test
    fun brokerOutageIsRecoveredWithin15sAndTheSetIsSubscribedAgain() {
        val rig = Rig(reconnect = ReconnectPolicy(random = { 0.5 }))
        rig.start()
        rig.advance(100)
        val droppedAt = rig.now
        rig.online = false
        rig.drop()

        rig.advance(6_000)
        rig.online = true
        rig.advanceUntil { rig.started().size >= 2 }

        val recovery = rig.now - droppedAt
        assertTrue(recovery <= 15_000, "recovered in $recovery ms")
        val lastSubscribe = rig.packets.filterIsInstance<MQTTSubscribe>().last()
        assertEquals(listOf(K1), lastSubscribe.subscriptions.map { it.topicFilter })
        assertEquals(rig.started().take(1), rig.ended().take(1), "the broken session was ended")
    }

    @Test
    fun serverMovedDisconnectEndsTheSessionAndReconnectsToTheSameBroker() {
        val rig = Rig()
        rig.start()
        rig.advance(100)

        rig.brokerSends(rig.script.disconnect5(ReasonCode.SERVER_MOVED, serverReference = "other:1883"))
        assertEquals(rig.started().take(1), rig.ended(), "session ended")
        val end = rig.emitted().filterIsInstance<MqttIncoming.SessionEnded>().single()
        assertEquals(MqttError.ServerDisconnect(ReasonCode.SERVER_MOVED), end.error)

        rig.advance(1_100)
        assertEquals(2, rig.started().size, "reconnected after the first backoff step")
        val connects = rig.actions.filterIsInstance<EngineAction.Connect>()
        assertTrue(connects.all { it.host == HOST && it.port == PORT }, "no redirect (H10)")
    }

    @Test
    fun closeReleasesTheConnectionAndNoAttemptsFollow() {
        val rig = Rig()
        rig.start()
        rig.advance(100)
        rig.command(EngineCommand.Close())

        assertTrue(rig.packets.last() is MQTTDisconnect)
        assertEquals(ConnectionState.Closed(null), rig.engine.state)
        assertEquals(rig.started(), rig.ended())
        val attempts = rig.connectTimes.size
        rig.advance(120_000)
        assertEquals(attempts, rig.connectTimes.size, "no reconnects after close")
        assertEquals(null, rig.engine.nextDeadline())
    }

    @Test
    fun closeDuringBackoffCancelsTheReconnect() {
        val rig = Rig()
        rig.online = false
        rig.start()
        rig.advance(1_500)
        rig.command(EngineCommand.Close())

        assertEquals(ConnectionState.Closed(null), rig.engine.state)
        val attempts = rig.connectTimes.size
        rig.advance(120_000)
        assertEquals(attempts, rig.connectTimes.size)
    }

    // ---- эпохи, maxAttempts ----

    @Test
    fun sessionsStartAndEndWithGrowingEpochsInStreamOrder() {
        val rig = Rig()
        rig.start()
        repeat(2) {
            rig.advance(100)
            rig.drop()
            rig.advanceUntil { rig.started().size > it + 1 }
        }
        assertEquals(listOf(101L, 102, 103), rig.started())
        assertEquals(listOf(101L, 102), rig.ended())
        val markers = rig.emitted().map {
            when (it) {
                is MqttIncoming.SessionStarted -> "S${it.epoch}"
                is MqttIncoming.SessionEnded -> "E${it.epoch}"
                else -> "M"
            }
        }
        assertEquals(listOf("S101", "E101", "S102", "E102", "S103"), markers)
        val connected = rig.states().filterIsInstance<ConnectionState.Connected>().map { it.epoch }
        assertEquals(listOf(101L, 102, 103), connected)
    }

    @Test
    fun maxAttemptsClosesTheClientWithTheLastError() {
        val rig = Rig(reconnect = ReconnectPolicy(maxAttempts = 2, random = { 0.0 }))
        rig.online = false
        val queued = rig.publish("a/b", ttlMs = 60_000)
        rig.start()
        rig.advance(10_000)

        assertEquals(listOf(0L, 1_000, 3_000), rig.connectTimes, "first attempt + 2 reconnects")
        val closed = rig.engine.state
        assertTrue(closed is ConnectionState.Closed && closed.error is MqttError.Io, "$closed")
        assertEquals(listOf(PublishResult.Failed(PublishFailure.Closed)), rig.results(queued))
        assertEquals(null, rig.engine.nextDeadline())
    }

    @Test
    fun neverPolicyClosesAfterTheFirstConnectionEnds() {
        val rig = Rig(reconnect = ReconnectPolicy.Never)
        rig.start()
        rig.advance(100)
        rig.drop()
        rig.advance(60_000)
        assertEquals(1, rig.connectTimes.size)
        assertEquals(ConnectionState.Closed(MqttError.Io("eof", null)), rig.engine.state)
        assertEquals(rig.started(), rig.ended())
    }

    @Test
    fun aStableSessionAlsoResetsTheAttemptLimit() {
        val rig = Rig(reconnect = ReconnectPolicy(maxAttempts = 1, random = { 0.0 }))
        rig.start()
        repeat(3) {
            rig.advance(100)
            rig.drop() // SUBACK пришёл — сессия стабильна, попытки сброшены
            rig.advanceUntil { rig.started().size > it + 1 }
        }
        assertEquals(4, rig.started().size)
        assertTrue(rig.engine.state is ConnectionState.Connected)
    }

    // ---- оффлайн-очередь ----

    @Test
    fun queuedPublishIsSentAfterTheSetIsSubscribed() {
        val rig = Rig()
        rig.online = false
        rig.start()
        val ticket = rig.publish("a/b", ttlMs = 10_000)
        assertTrue(rig.results(ticket).isEmpty(), "waits for the connection")
        rig.online = true
        rig.advance(1_000)

        val types = rig.packets.map { it::class.simpleName }
        assertEquals(listOf("MQTT5Connect", "MQTT5Subscribe", "MQTT5Publish"), types)
        assertEquals(listOf(PublishResult.Sent(101)), rig.results(ticket))
        val writeOfPublish = rig.actions.indexOfLast { it is EngineAction.Write }
        val completion = rig.actions.indexOfFirst { it is EngineAction.Complete }
        assertTrue(writeOfPublish < completion, "Sent after the write")
    }

    @Test
    fun publishExpiresByTtlAndNeverReachesTheBroker() {
        val rig = Rig()
        rig.online = false
        rig.start()
        val ticket = rig.publish("a/b", ttlMs = 1_500)
        rig.advance(1_499)
        assertTrue(rig.results(ticket).isEmpty())
        rig.advance(1)
        assertEquals(listOf<PublishResult>(PublishResult.Expired), rig.results(ticket))

        rig.online = true
        rig.advance(5_000)
        assertEquals(1, rig.started().size)
        assertTrue(rig.packets.none { it is MQTTPublish }, "expired publish is not sent")
    }

    @Test
    fun fullQueueRejectsTheNextPublish() {
        val rig = Rig(offlineCapacity = 2)
        val first = rig.publish("a/1")
        val second = rig.publish("a/2")
        val third = rig.publish("a/3")
        assertEquals(listOf(PublishResult.Failed(PublishFailure.QueueFull)), rig.results(third))
        rig.start()
        assertEquals(listOf("a/1", "a/2"), rig.packets.filterIsInstance<MQTTPublish>().map { it.topicName })
        assertEquals(listOf(PublishResult.Sent(101)), rig.results(first))
        assertEquals(listOf(PublishResult.Sent(101)), rig.results(second))
    }

    @Test
    fun cancelledPublishLeavesTheQueueWithoutAResult() {
        val rig = Rig()
        rig.online = false
        rig.start()
        val cancelled = rig.publish("a/1")
        val kept = rig.publish("a/2")
        rig.command(EngineCommand.Cancel(cancelled))
        rig.online = true
        rig.advance(1_000)

        assertEquals(listOf("a/2"), rig.packets.filterIsInstance<MQTTPublish>().map { it.topicName })
        assertTrue(rig.results(cancelled).isEmpty())
        assertEquals(listOf(PublishResult.Sent(101)), rig.results(kept))
    }

    @Test
    fun publishInALiveSessionGoesOutAtOnce() {
        val rig = Rig()
        rig.start()
        val ticket = rig.publish("a/b")
        assertTrue(rig.actions.last() is EngineAction.Complete)
        assertEquals(listOf(PublishResult.Sent(101)), rig.results(ticket))
        assertEquals(Qos.AT_LEAST_ONCE, rig.packets.filterIsInstance<MQTTPublish>().single().qos)
    }

    // ---- Delivered, PUBACK, окно брокера ----

    @Test
    fun deliveredFailsWithConnectionLostOnDropAndIsNotResent() {
        val rig = Rig()
        rig.puback = null
        rig.start()
        val ticket = rig.publish("a/b", until = PublishStage.Delivered)
        assertTrue(rig.results(ticket).isEmpty(), "waits for PUBACK")
        rig.drop()
        assertEquals(listOf(PublishResult.Failed(PublishFailure.ConnectionLost)), rig.results(ticket))

        rig.advanceUntil { rig.started().size >= 2 }
        rig.advance(1_000)
        assertEquals(1, rig.packets.count { it is MQTTPublish }, "not resent after reconnect (cleanStart)")
    }

    @Test
    fun pubackCompletesDeliveredAndRefusalIsRejected() {
        val rig = Rig()
        rig.start()
        val ok = rig.publish("a/1", until = PublishStage.Delivered)
        assertEquals(listOf(PublishResult.Delivered(101)), rig.results(ok))

        rig.puback = ReasonCode.NOT_AUTHORIZED
        val refused = rig.publish("a/2", until = PublishStage.Delivered)
        assertEquals(listOf(PublishResult.Failed(PublishFailure.Rejected(ReasonCode.NOT_AUTHORIZED))), rig.results(refused))
        assertTrue(rig.engine.state is ConnectionState.Connected, "refusal keeps the session")

        val qos0 = rig.publish("a/3", qos = Qos.AT_MOST_ONCE, until = PublishStage.Delivered)
        assertEquals(listOf(PublishResult.Sent(101)), rig.results(qos0), "QoS 0 has no PUBACK")
    }

    @Test
    fun clientSideRejectionCarriesTheReasonCode() {
        val rig = Rig()
        rig.connackProps = { MQTT5Properties(maximumQos = 0u) }
        rig.start()
        val ticket = rig.publish("a/b", qos = Qos.AT_LEAST_ONCE)
        assertEquals(listOf(PublishResult.Failed(PublishFailure.Rejected(ReasonCode.QOS_NOT_SUPPORTED))), rig.results(ticket))
        assertTrue(rig.packets.none { it is MQTTPublish })
    }

    @Test
    fun flowControlHoldsPublishesUntilPuback() {
        val rig = Rig()
        rig.connackProps = { MQTT5Properties(receiveMaximum = 1u) }
        rig.puback = null
        rig.start()
        val first = rig.publish("a/1", until = PublishStage.Delivered)
        val second = rig.publish("a/2", until = PublishStage.Delivered)
        val publishes = rig.packets.filterIsInstance<MQTTPublish>()
        assertEquals(listOf("a/1"), publishes.map { it.topicName })

        rig.brokerSends(rig.script.puback(publishes.single().packetId!!.toInt()))
        assertEquals(listOf(PublishResult.Delivered(101)), rig.results(first))
        assertEquals(listOf("a/1", "a/2"), rig.packets.filterIsInstance<MQTTPublish>().map { it.topicName })
        assertTrue(rig.results(second).isEmpty())
    }

    // ---- закрытие (H1, H2) ----

    @Test
    fun closeAfterPublishWritesPublishThenDisconnectAndClosesTransportOnlyAfterEof() {
        val rig = Rig()
        rig.eofOnDisconnect = false
        rig.start()
        rig.actions.clear()
        val ticket = rig.publish("a/retained", retain = true)
        rig.command(EngineCommand.Close())

        assertTrue(rig.packets.takeLast(2).let { it[0] is MQTTPublish && it[1] is MQTTDisconnect }, "${rig.packets}")
        assertEquals(listOf(PublishResult.Sent(101)), rig.results(ticket))
        assertFalse(rig.actions.contains(EngineAction.CloseTransport), "waits for EOF")

        rig.deliver(TransportEvent.Eof)
        val close = rig.actions.indexOf(EngineAction.CloseTransport)
        assertTrue(close > rig.actions.indexOfLast { it is EngineAction.Write })
        assertEquals(EngineAction.State(ConnectionState.Closed(null)), rig.actions.last { it is EngineAction.State })
        assertEquals(listOf(MqttIncoming.SessionEnded(101, null)), rig.emitted().filterIsInstance<MqttIncoming.SessionEnded>())
    }

    @Test
    fun publishAndCloseBackToBackGiveWritePublishCompleteWriteDisconnect() {
        val rig = Rig()
        rig.eofOnDisconnect = false
        rig.start()
        val ticket = PublishTicket(99)
        rig.engine.onCommand(
            EngineCommand.Publish(ticket, "a/b", byteArrayOf(1), Qos.AT_LEAST_ONCE, true, 1_000, PublishStage.Sent),
            rig.now,
        )
        rig.engine.onCommand(EngineCommand.Close(), rig.now)
        val batch = rig.engine.takeActions()
        assertEquals(3, batch.size, "$batch")
        val parser = MQTTCurrentPacket(1024u * 1024u, MQTTVersion.MQTT5)
        assertTrue(parser.addData((batch[0] as EngineAction.Write).bytes.asUByteArray()).single() is MQTTPublish)
        assertEquals(EngineAction.Complete(ticket, PublishResult.Sent(101)), batch[1])
        assertTrue(parser.addData((batch[2] as EngineAction.Write).bytes.asUByteArray()).single() is MQTTDisconnect)
    }

    @Test
    fun closeWithoutEofClosesTheTransportAfterTheLinger() {
        val rig = Rig()
        rig.eofOnDisconnect = false
        rig.start()
        rig.command(EngineCommand.Close())
        rig.advance(2_999)
        assertFalse(rig.actions.contains(EngineAction.CloseTransport))
        rig.advance(1)
        assertTrue(rig.actions.contains(EngineAction.CloseTransport))
        assertEquals(ConnectionState.Closed(null), rig.engine.state)
    }

    @Test
    fun closeWithoutAConnectionFailsQueuedPublishes() {
        val rig = Rig()
        rig.connectHangs = true
        rig.start()
        val queued = rig.publish("a/b")
        rig.command(EngineCommand.Close())

        assertTrue(rig.actions.contains(EngineAction.CloseTransport), "connect in progress is cancelled")
        assertEquals(listOf(PublishResult.Failed(PublishFailure.Closed)), rig.results(queued))
        val after = rig.publish("a/c")
        assertEquals(listOf(PublishResult.Failed(PublishFailure.Closed)), rig.results(after))
        assertEquals(ConnectionState.Closed(null), rig.engine.state)
    }

    @Test
    fun closeBeforeStartIsImmediate() {
        val rig = Rig()
        val queued = rig.publish("a/b")
        rig.command(EngineCommand.Close())
        rig.start()
        assertTrue(rig.connectTimes.isEmpty())
        assertEquals(listOf(PublishResult.Failed(PublishFailure.Closed)), rig.results(queued))
    }

    // ---- подписки (минимум T03a), обратное давление ----

    @Test
    fun liveGroupChangeSendsTheDifference() {
        val rig = Rig()
        rig.start()
        rig.command(EngineCommand.SetGroup("extra", setOf("x/1"), Qos.AT_MOST_ONCE))
        rig.command(EngineCommand.SetGroup("working", emptySet(), Qos.AT_MOST_ONCE))
        val tail = rig.packets.takeLast(2)
        assertEquals(listOf("x/1"), (tail[0] as MQTTSubscribe).subscriptions.map { it.topicFilter })
        assertEquals(listOf(K1), (tail[1] as MQTTUnsubscribe).topicFilters)
        rig.command(EngineCommand.Resubscribe(null))
        assertEquals(listOf("x/1"), (rig.packets.last() as MQTTSubscribe).subscriptions.map { it.topicFilter })
    }

    @Test
    fun theSetIsSubscribedInBatches() {
        val rig = Rig()
        rig.command(EngineCommand.SetGroup("big", (1..120).map { "t/$it" }.toSet(), Qos.AT_MOST_ONCE))
        rig.start()
        val sizes = rig.packets.filterIsInstance<MQTTSubscribe>().map { it.subscriptions.size }
        assertEquals(listOf(50, 50, 21), sizes) // 120 + K1 рабочей группы
    }

    @Test
    fun consumerStallEndsTheSessionAndReconnectWaitsForTheConsumer() {
        // S06b-T05g: пока чтение приостановлено, переподключения нет — ни Connect, ни срока
        val rig = Rig()
        rig.start()
        rig.command(EngineCommand.SetReadPaused(true))
        assertEquals(EngineAction.PauseRead(true), rig.actions.last())
        rig.advance(60_000)
        assertEquals(MqttError.ConsumerStalled, rig.emitted().filterIsInstance<MqttIncoming.SessionEnded>().single().error)
        val waiting = assertIs<ConnectionState.Waiting>(rig.engine.state)
        assertEquals(MqttError.ConsumerStalled, waiting.lastError)

        rig.actions.clear()
        rig.advance(600_000)
        assertTrue(rig.actions.none { it is EngineAction.Connect || it is EngineAction.State }, "${rig.actions}")
        assertEquals(null, rig.engine.nextDeadline())
        assertEquals(waiting, rig.engine.state)

        val resumedAt = rig.now
        rig.command(EngineCommand.SetReadPaused(false))
        assertTrue(rig.actions.none { it is EngineAction.Connect }, "подключение — на ближайшем тике, не из команды")
        val deadline = assertNotNull(rig.engine.nextDeadline())
        assertTrue(deadline <= resumedAt, "срок задержки давно прошёл — попытка сразу")
        rig.advance(0)
        val connect = rig.actions.indexOfFirst { it is EngineAction.Connect }
        assertTrue(connect >= 0)
        assertTrue(rig.actions.drop(connect).none { it == EngineAction.PauseRead(true) })
        assertEquals(listOf(102L), rig.started(), "новая эпоха (действия до ожидания очищены)")
        assertIs<ConnectionState.Connected>(rig.engine.state)
    }

    @Test
    fun resumeBeforeTheBackoffDelayWaitsForTheDelay() {
        val rig = Rig()
        rig.start()
        rig.command(EngineCommand.SetReadPaused(true))
        rig.advance(60_000)
        val endedAt = rig.now
        assertEquals(1.seconds, assertIs<ConnectionState.Waiting>(rig.engine.state).delay)
        rig.advance(300)
        rig.command(EngineCommand.SetReadPaused(false))
        assertEquals(endedAt + 1_000, rig.engine.nextDeadline())
        rig.actions.clear()
        rig.advance(699)
        assertTrue(rig.actions.none { it is EngineAction.Connect })
        rig.advance(1)
        assertTrue(rig.actions.any { it is EngineAction.Connect })
    }

    @Test
    fun pauseBeforeEofAlsoHoldsReconnectAndKeepsTheOriginalError() {
        // Пауза из-за всплеска, затем брокер закрыл соединение: причина — EOF, переподключение ждёт потребителя
        val rig = Rig()
        rig.start()
        rig.command(EngineCommand.SetReadPaused(true))
        rig.drop()
        val waiting = assertIs<ConnectionState.Waiting>(rig.engine.state)
        assertTrue(waiting.lastError !is MqttError.ConsumerStalled)
        rig.actions.clear()
        rig.advance(600_000)
        assertTrue(rig.actions.none { it is EngineAction.Connect })
        assertEquals(waiting, rig.engine.state)
        rig.command(EngineCommand.SetReadPaused(false))
        rig.advance(0)
        assertIs<ConnectionState.Connected>(rig.engine.state)
    }

    @Test
    fun closeWhileWaitingForTheConsumerIsImmediate() {
        val rig = Rig()
        rig.start()
        rig.command(EngineCommand.SetReadPaused(true))
        rig.advance(60_000)
        assertIs<ConnectionState.Waiting>(rig.engine.state)
        rig.actions.clear()
        rig.command(EngineCommand.Close())
        assertEquals(ConnectionState.Closed(null), rig.engine.state)
        assertTrue(rig.actions.none { it is EngineAction.Connect || it is EngineAction.Write })
        assertEquals(null, rig.engine.nextDeadline())
    }

    @Test
    fun keepAliveRunsThroughTheEngine() {
        val rig = Rig(session = SessionSettings(MQTTVersion.MQTT5, clientId = "c", keepAliveSec = 10))
        rig.start()
        rig.advance(9_000)
        assertTrue(rig.packets.last() is MQTTPingreq)
        rig.advance(5_000) // ответа нет ½ keepAlive
        assertEquals(MqttError.KeepAliveTimeout, rig.emitted().filterIsInstance<MqttIncoming.SessionEnded>().single().error)
    }

    @Test
    fun mqtt311SessionWorksTheSameWay() {
        val rig = Rig(version = MQTTVersion.MQTT3_1_1)
        rig.start()
        val ticket = rig.publish("a/b", until = PublishStage.Delivered)
        assertEquals(listOf(PublishResult.Delivered(101)), rig.results(ticket))
        rig.brokerSends(rig.script.publish(K1, "1".encodeToByteArray(), retain = true))
        val message = rig.emitted().filterIsInstance<MqttIncoming.Message>().single()
        assertEquals(K1, message.topic)
        assertEquals(101, message.epoch)
        assertTrue(message.retain)
    }

    // ---- H11 ----

    @Test
    fun actionsAndCommandsDoNotPrintTopicsPayloadsOrTheHost() {
        val rig = Rig()
        rig.command(EngineCommand.SetGroup("g", setOf("secret/filter"), Qos.AT_MOST_ONCE))
        rig.start()
        val command = EngineCommand.Publish(
            PublishTicket(1), "secret/topic", "secret-payload".encodeToByteArray(), Qos.AT_LEAST_ONCE, true, 1_000,
            PublishStage.Sent,
        )
        rig.command(command)
        rig.brokerSends(rig.script.publish("secret/topic", "secret-payload".encodeToByteArray()))
        rig.drop()
        val printed = (rig.actions + command + EngineCommand.SetGroup("g", setOf("secret/filter"), Qos.AT_MOST_ONCE))
            .joinToString { it.toString() } + rig.engine.state + EngineOptions(HOST, PORT, rig.session)
        for (secret in listOf("secret", HOST)) assertFalse(printed.contains(secret), "leaked '$secret': $printed")
    }

    // ---- стенд ----

    internal enum class Suback { GRANTED, REJECTED, SILENT }

    internal class Rig(
        val version: MQTTVersion = MQTTVersion.MQTT5,
        reconnect: ReconnectPolicy = ReconnectPolicy(random = { 0.0 }),
        offlineCapacity: Int = 64,
        val session: SessionSettings = SessionSettings(version, clientId = "c", keepAliveSec = 0),
        maxFilters: Int = 4096,
    ) {
        var now = 0L
        private var epochs = 100L
        val engine = MqttClientEngine(
            EngineOptions(HOST, PORT, session, reconnect = reconnect, offlineCapacity = offlineCapacity, maxFilters = maxFilters),
            nextEpoch = { ++epochs },
        )
        val script = BrokerScript(version)
        val actions = ArrayList<EngineAction>()
        val packets = ArrayList<MQTTPacket>()
        val connectTimes = ArrayList<Long>()
        private var gapsFrom = 0

        var online = true
        var connectHangs = false
        var connackCode = ReasonCode.SUCCESS
        var connackProps: () -> MQTT5Properties = { MQTT5Properties() }
        var suback = Suback.GRANTED

        /** Код SUBACK по фильтру (T03b); `null` — по [suback]. */
        var subackCode: ((String) -> ReasonCode)? = null
        var puback: ReasonCode? = ReasonCode.SUCCESS
        var eofOnDisconnect = true

        private var linkUp = false
        private var parser = MQTTCurrentPacket(16u * 1024u * 1024u, version)
        private val inbound = ArrayDeque<TransportEvent>()
        private var tickets = 0L

        init {
            command(EngineCommand.SetGroup("working", setOf(K1), Qos.AT_MOST_ONCE))
        }

        fun start() = command(EngineCommand.Start)

        fun command(command: EngineCommand) {
            engine.onCommand(command, now)
            pump()
        }

        fun publish(
            topic: String,
            qos: Qos = Qos.AT_LEAST_ONCE,
            retain: Boolean = false,
            ttlMs: Long = 10_000,
            until: PublishStage = PublishStage.Sent,
        ): PublishTicket {
            val ticket = PublishTicket(++tickets)
            command(EngineCommand.Publish(ticket, topic, "v".encodeToByteArray(), qos, retain, ttlMs, until))
            return ticket
        }

        fun deliver(event: TransportEvent) {
            engine.onTransport(event, now)
            pump()
        }

        fun brokerSends(bytes: ByteArray) = deliver(TransportEvent.Bytes(bytes))

        /** Брокер закрыл соединение. */
        fun drop() {
            check(linkUp) { "no link" }
            linkUp = false
            deliver(TransportEvent.Eof)
        }

        fun advance(ms: Long) {
            val target = now + ms
            var guard = 0
            while (true) {
                val deadline = engine.nextDeadline()
                if (deadline == null || deadline > target) break
                if (++guard > 10_000) fail("deadline loop at $deadline")
                now = maxOf(now, deadline)
                engine.onTick(now)
                pump()
            }
            now = target
            engine.onTick(now)
            pump()
        }

        fun advanceUntil(condition: () -> Boolean) {
            var guard = 0
            while (!condition()) {
                if (++guard > 10_000) fail("condition not reached by $now")
                advance(10)
            }
        }

        fun markConnectTime() {
            gapsFrom = connectTimes.size
            connectTimes += now
        }

        fun gaps(): List<Long> = connectTimes.drop(gapsFrom).zipWithNext { a, b -> b - a }

        fun emitted(): List<MqttIncoming> = actions.filterIsInstance<EngineAction.Emit>().map { it.incoming }
        fun started(): List<Long> = emitted().filterIsInstance<MqttIncoming.SessionStarted>().map { it.epoch }
        fun ended(): List<Long> = emitted().filterIsInstance<MqttIncoming.SessionEnded>().map { it.epoch }
        fun states(): List<ConnectionState> = actions.filterIsInstance<EngineAction.State>().map { it.state }
        fun logs(): List<EngineLogEvent> = actions.filterIsInstance<EngineAction.Log>().map { it.event }
        fun results(ticket: PublishTicket): List<PublishResult> =
            actions.filterIsInstance<EngineAction.Complete>().filter { it.ticket == ticket }.map { it.result }

        private fun pump() {
            var guard = 0
            while (true) {
                if (++guard > 100_000) fail("pump loop")
                val batch = engine.takeActions()
                if (batch.isEmpty()) {
                    val event = inbound.removeFirstOrNull() ?: return
                    engine.onTransport(event, now)
                    continue
                }
                for (action in batch) {
                    actions += action
                    execute(action)
                }
            }
        }

        private fun execute(action: EngineAction) {
            when (action) {
                is EngineAction.Connect -> {
                    connectTimes += now
                    inbound.clear()
                    parser = MQTTCurrentPacket(16u * 1024u * 1024u, version)
                    linkUp = false
                    when {
                        connectHangs -> Unit
                        online -> {
                            linkUp = true
                            inbound += TransportEvent.Connected
                        }
                        else -> inbound += TransportEvent.Failed(MqttError.Io("connect", null))
                    }
                }
                is EngineAction.Write -> if (linkUp) {
                    for (packet in parser.addData(action.bytes.asUByteArray())) {
                        packets += packet
                        respond(packet)
                    }
                }
                EngineAction.CloseTransport -> {
                    linkUp = false
                    inbound.clear()
                }
                else -> Unit
            }
        }

        private fun respond(packet: MQTTPacket) {
            when (packet) {
                is MQTTConnect -> inbound += TransportEvent.Bytes(
                    if (version == MQTTVersion.MQTT5) {
                        script.connack(connackCode, properties = connackProps())
                    } else {
                        script.connack()
                    },
                )
                is MQTTSubscribe -> {
                    val id = packet.packetIdentifier.toInt()
                    val n = packet.subscriptions.size
                    val byFilter = subackCode
                    when (suback) {
                        Suback.SILENT -> Unit
                        else -> if (byFilter != null) {
                            inbound += TransportEvent.Bytes(script.suback(id, packet.subscriptions.map { byFilter(it.topicFilter) }))
                        } else if (suback == Suback.GRANTED) {
                            inbound += TransportEvent.Bytes(script.suback(id, List(n) { ReasonCode.SUCCESS }))
                        } else {
                            inbound += TransportEvent.Bytes(script.suback(id, List(n) { ReasonCode.NOT_AUTHORIZED }))
                        }
                    }
                }
                is MQTTUnsubscribe -> inbound += TransportEvent.Bytes(script.unsuback(packet.packetIdentifier.toInt()))
                is MQTTPublish -> {
                    val code = puback
                    if (packet.qos == Qos.AT_LEAST_ONCE && code != null) {
                        inbound += TransportEvent.Bytes(script.puback(packet.packetId!!.toInt(), code))
                    }
                }
                is MQTTDisconnect -> if (eofOnDisconnect) {
                    linkUp = false
                    inbound += TransportEvent.Eof
                }
                else -> Unit
            }
        }
    }

    internal companion object {
        const val HOST = "broker.example"
        const val PORT = 1883
        const val K1 = "/devices/wb-mr6c_1/controls/K1"
    }
}
