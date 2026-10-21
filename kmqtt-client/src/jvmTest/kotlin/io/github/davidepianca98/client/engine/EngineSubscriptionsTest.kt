package io.github.davidepianca98.client.engine

import io.github.davidepianca98.client.MqttIncoming
import io.github.davidepianca98.client.SubscribeRefused
import io.github.davidepianca98.client.TopicState
import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.engine.EngineReconnectTest.Companion.HOST
import io.github.davidepianca98.client.engine.EngineReconnectTest.Companion.K1
import io.github.davidepianca98.client.engine.EngineReconnectTest.Rig
import io.github.davidepianca98.client.engine.EngineReconnectTest.Suback
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTConnect
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPublish
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTSubscribe
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTUnsubscribe
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Подписки движка (`SubscriptionSet`) на виртуальном времени: сценарии
 * (группы, разница, временные группы обнаружения, отказы SUBACK без разрыва, `refreshShared`,
 * `resubscribe`, переподписка после реконнекта порциями), сопоставление SUBACK по packetId, эпохи, `maxFilters`.
 * Стенд — [Rig] из [EngineReconnectTest]; «активное у брокера» — проигрыш SUBSCRIBE/UNSUBSCRIBE с последнего CONNECT.
 */
@OptIn(MqttCoreApi::class)
class EngineSubscriptionsTest {

    // ---- сценарии ----

    /** `workingModeSubscribesOnlyToSelectedDevicesAndProperties`: желаемое = объединение групп, ровно эти фильтры. */
    @Test
    fun theUnionOfGroupsIsSubscribedExactlyOnceAfterConnack() {
        val rig = Rig()
        rig.command(EngineCommand.SetGroup("working", setOf(ERROR, K1, K2), Qos.AT_MOST_ONCE))
        rig.command(EngineCommand.SetGroup("extra", setOf(K2, ON), Qos.AT_MOST_ONCE))
        assertEquals(TopicState.Pending, rig.topics()[K1], "pending before the session")
        rig.start()

        assertEquals(listOf("SUB $ERROR@0,$K1@0,$K2@0,$ON@0"), rig.wire())
        assertEquals(mapOf(ERROR to 0, K1 to 0, K2 to 0, ON to 0), rig.active())
        assertEquals(setOf(TopicState.Granted(Qos.AT_MOST_ONCE)), rig.topics().values.toSet())
        assertEquals(listOf(ERROR, K1, K2, ON), rig.topics().keys.toList())
    }

    /**
     * `subscriptionQosIsAtMost1`: ограничение QoS — дело конфигурации приложения; движок берёт QoS группы, у общего
     * фильтра — максимум по группам, и переподписывает фильтр при смене максимума (без UNSUBSCRIBE).
     */
    @Test
    fun filterQosIsTheMaximumOverGroupsAndFollowsTheirChanges() {
        val rig = Rig()
        rig.start()
        val from = rig.packets.size
        rig.command(EngineCommand.SetGroup("b", setOf(K1), Qos.AT_LEAST_ONCE))
        rig.command(EngineCommand.SetGroup("c", setOf(K1), Qos.AT_MOST_ONCE)) // максимум не меняется — тишина
        rig.command(EngineCommand.SetGroup("b", emptySet(), Qos.AT_LEAST_ONCE))
        rig.command(EngineCommand.SetGroup("c", emptySet(), Qos.AT_MOST_ONCE)) // K1 держит рабочая группа

        assertEquals(listOf("SUB $K1@1", "SUB $K1@0"), rig.wire(from))
        assertEquals(mapOf(K1 to 0), rig.active())
        assertEquals(TopicState.Granted(Qos.AT_MOST_ONCE), rig.topics()[K1])
    }

    /** `discoveryAddsWideSubscriptionsOnlyForTheRequestWindow`: временная группа — на окно; рабочие не трогаются. */
    @Test
    fun temporaryGroupAddsWideFiltersOnlyForTheWindow() {
        val rig = Rig()
        rig.start()
        val working = rig.active()
        val from = rig.packets.size

        rig.command(EngineCommand.SetGroup(TEMP1, setOf(WIDE_STATE, WIDE_ERROR, META, K1), Qos.AT_MOST_ONCE))
        assertTrue(rig.active().keys.containsAll(setOf(WIDE_STATE, WIDE_ERROR, META)))
        rig.advance(2_500)
        rig.command(EngineCommand.SetGroup(TEMP1, emptySet(), Qos.AT_MOST_ONCE))

        assertEquals(listOf("SUB $WIDE_STATE@0,$WIDE_ERROR@0,$META@0", "UNSUB $WIDE_STATE,$WIDE_ERROR,$META"), rig.wire(from))
        assertEquals(working, rig.active(), "the working filter $K1 is neither re-subscribed nor removed")
        assertEquals(1, rig.started().size)
    }

    /** `discoveryRefusedByTheBrokerIsUnavailableAndTheWorkingSessionLives`: отказ во временной группе. */
    @Test
    fun refusalInATemporaryGroupKeepsTheSessionAndReachesThatGroup() {
        val rig = Rig()
        rig.subackCode = { if ('+' in it) ReasonCode.NOT_AUTHORIZED else ReasonCode.SUCCESS }
        rig.start()

        rig.command(EngineCommand.SetGroup(TEMP1, setOf(WIDE_STATE, META), Qos.AT_MOST_ONCE))
        assertEquals(
            listOf(
                SubscribeRefused(TEMP1, WIDE_STATE, ReasonCode.NOT_AUTHORIZED),
                SubscribeRefused(TEMP1, META, ReasonCode.NOT_AUTHORIZED),
            ),
            rig.refusals(),
        )
        assertEquals(TopicState.Refused(ReasonCode.NOT_AUTHORIZED), rig.topics()[WIDE_STATE])
        assertEquals(TopicState.Granted(Qos.AT_MOST_ONCE), rig.topics()[K1])
        assertTrue(rig.ended().isEmpty(), "the session is not broken")

        rig.command(EngineCommand.SetGroup(TEMP1, emptySet(), Qos.AT_MOST_ONCE)) // фасад снимает группу в finally
        assertEquals(mapOf(K1 to 0), rig.active())
        assertEquals(setOf(K1), rig.topics().keys)
        rig.brokerSends(rig.script.publish(K1, "1".encodeToByteArray()))
        assertEquals(listOf(101L), rig.emitted().filterIsInstance<MqttIncoming.Message>().map { it.epoch }, "still delivers")
    }

    /** `refusedWorkingTopicIsSkippedAndReportedWithoutTheTopic`: частичный отказ — только этот фильтр. */
    @Test
    fun partialRefusalMarksOnlyTheRefusedFilterAndTheSessionLives() {
        val rig = Rig()
        rig.command(EngineCommand.SetGroup("working", setOf(ERROR, K1, K2), Qos.AT_MOST_ONCE))
        rig.subackCode = { if (it == K2) ReasonCode.NOT_AUTHORIZED else ReasonCode.GRANTED_QOS1 }
        rig.start()

        assertEquals(
            mapOf(
                ERROR to TopicState.Granted(Qos.AT_LEAST_ONCE),
                K1 to TopicState.Granted(Qos.AT_LEAST_ONCE),
                K2 to TopicState.Refused(ReasonCode.NOT_AUTHORIZED),
            ),
            rig.topics(),
        )
        assertEquals(listOf(SubscribeRefused("working", K2, ReasonCode.NOT_AUTHORIZED)), rig.refusals())
        assertTrue(EngineLogEvent.Refused(1) in rig.logs())
        rig.brokerSends(rig.script.publish(K1, "1".encodeToByteArray()))
        assertEquals(1, rig.emitted().filterIsInstance<MqttIncoming.Message>().size)
        assertTrue(rig.ended().isEmpty())

        // Обнаружение на этой сессии работает: широкие фильтры брокер разрешает
        rig.command(EngineCommand.SetGroup(TEMP1, setOf(WIDE_STATE), Qos.AT_MOST_ONCE))
        assertEquals(TopicState.Granted(Qos.AT_LEAST_ONCE), rig.topics()[WIDE_STATE])
        assertEquals(1, rig.refusals().size)
    }

    /** То же в 3.1.1: код отказа 0x80 приходит как `UNSPECIFIED_ERROR`. */
    @Test
    fun partialRefusalOnMqtt311() {
        val rig = Rig(version = MQTTVersion.MQTT3_1_1)
        rig.command(EngineCommand.SetGroup("working", setOf(K1, K2), Qos.AT_LEAST_ONCE))
        rig.subackCode = { if (it == K2) ReasonCode.UNSPECIFIED_ERROR else ReasonCode.GRANTED_QOS1 }
        rig.start()

        assertEquals(TopicState.Granted(Qos.AT_LEAST_ONCE), rig.topics()[K1])
        assertEquals(TopicState.Refused(ReasonCode.UNSPECIFIED_ERROR), rig.topics()[K2])
        assertEquals(listOf(SubscribeRefused("working", K2, ReasonCode.UNSPECIFIED_ERROR)), rig.refusals())
        assertTrue(rig.ended().isEmpty())
    }

    /** `overlappingDiscoveryRequestsKeepWideSubscriptionsUntilTheLastOneEnds`: общий фильтр — до последней группы. */
    @Test
    fun overlappingTemporaryGroupsKeepASharedFilterUntilTheLastOneLeaves() {
        val rig = Rig()
        rig.command(EngineCommand.SetGroup("working", emptySet(), Qos.AT_MOST_ONCE))
        rig.start()
        val from = rig.packets.size

        rig.command(EngineCommand.SetGroup(TEMP1, setOf(WIDE_STATE, WIDE_ERROR), Qos.AT_MOST_ONCE))
        rig.advance(1_000)
        rig.command(EngineCommand.SetGroup(TEMP2, setOf(WIDE_STATE, DEVICE_META), Qos.AT_MOST_ONCE))
        rig.advance(1_500)
        rig.command(EngineCommand.SetGroup(TEMP1, emptySet(), Qos.AT_MOST_ONCE))
        assertTrue(WIDE_STATE in rig.active(), "the second group still needs it")
        rig.advance(1_000)
        rig.command(EngineCommand.SetGroup(TEMP2, emptySet(), Qos.AT_MOST_ONCE))

        assertEquals(
            listOf(
                "SUB $WIDE_STATE@0,$WIDE_ERROR@0",
                "SUB $DEVICE_META@0",
                "UNSUB $WIDE_ERROR",
                "UNSUB $WIDE_STATE,$DEVICE_META",
            ),
            rig.wire(from),
        )
        assertTrue(rig.active().isEmpty())
        assertTrue(rig.topics().isEmpty())
    }

    /** `workingSubscriptionsFollowTheSetOfDevicesAndPropertiesByDifference`: UNSUBSCRIBE снятых, затем SUBSCRIBE. */
    @Test
    fun liveChangesGoOutAsTheDifferenceAndTheSameSetIsSilent() {
        val rig = Rig()
        rig.command(EngineCommand.SetGroup("working", setOf(ERROR, K1, K2), Qos.AT_MOST_ONCE))
        rig.start()
        val from = rig.packets.size

        rig.command(EngineCommand.SetGroup("working", setOf(ERROR, K2, K3), Qos.AT_MOST_ONCE))
        assertEquals(listOf("UNSUB $K1", "SUB $K3@0"), rig.wire(from))
        assertEquals(mapOf(ERROR to 0, K2 to 0, K3 to 0), rig.active())

        val actions = rig.actions.size
        rig.command(EngineCommand.SetGroup("working", setOf(ERROR, K2, K3), Qos.AT_MOST_ONCE))
        assertTrue(rig.actions.drop(actions).isEmpty(), "same set: no traffic, no new state")
    }

    /** `refreshShared`: отдельный SUBSCRIBE после разницы, по порядку команды. */
    @Test
    fun refreshSharedSubscribesFiltersOfOtherGroupsAgainAfterTheDifference() {
        val rig = Rig()
        rig.command(EngineCommand.SetGroup("working", setOf(K1, K2), Qos.AT_MOST_ONCE))
        rig.start()
        rig.command(EngineCommand.SetGroup("b", setOf(K2, ON), Qos.AT_MOST_ONCE))
        val from = rig.packets.size

        // ON снимается, K3 добавляется, K1 (подписан рабочей группой) — повтор отдельным SUBSCRIBE; K2 не трогается
        rig.command(EngineCommand.SetGroup("b", setOf(K1, K3), Qos.AT_MOST_ONCE, refreshShared = true))
        assertEquals(listOf("UNSUB $ON", "SUB $K3@0", "SUB $K1@0"), rig.wire(from))

        // Без refreshShared тот же набор — тишина; с ним — повтор подписанных фильтров группы
        val mid = rig.packets.size
        rig.command(EngineCommand.SetGroup("b", setOf(K1, K3), Qos.AT_MOST_ONCE))
        assertEquals(emptyList<String>(), rig.wire(mid))
        rig.command(EngineCommand.SetGroup("b", setOf(K1, K3), Qos.AT_MOST_ONCE, refreshShared = true))
        assertEquals(listOf("SUB $K1@0,$K3@0"), rig.wire(mid))
    }

    /** `refreshSubscriptionsSubscribesTheSetAgainAndRetainedValuesComeAgain`: повтор всего подписанного без реконнекта. */
    @Test
    fun resubscribeAllSendsTheAppliedSetAgainInBatches() {
        val rig = Rig()
        rig.command(EngineCommand.SetGroup("big", (1..60).map { "t/$it" }.toSet(), Qos.AT_MOST_ONCE))
        rig.start()
        val from = rig.packets.size
        rig.suback = Suback.SILENT

        rig.command(EngineCommand.Resubscribe(null))
        assertEquals(listOf(50, 11), rig.packets.drop(from).filterIsInstance<MQTTSubscribe>().map { it.subscriptions.size })
        assertEquals(setOf<TopicState>(TopicState.Pending), rig.topics().values.toSet(), "pending until the new SUBACK")
        assertEquals(1, rig.connectTimes.size, "without reconnecting")
        assertTrue(rig.packets.drop(from).none { it is MQTTUnsubscribe })
    }

    /** `refreshGroup`: только подписанные фильтры группы; без живой сессии и при закрытии — ничего. */
    @Test
    fun resubscribeGroupSendsOnlyThatGroupAndNothingWithoutALiveSession() {
        val rig = Rig()
        rig.command(EngineCommand.Resubscribe("working")) // до старта
        rig.command(EngineCommand.SetGroup("b", setOf(K2, K3), Qos.AT_LEAST_ONCE))
        rig.command(EngineCommand.SetGroup("c", setOf(K3), Qos.EXACTLY_ONCE))
        rig.start()
        val from = rig.packets.size

        rig.command(EngineCommand.Resubscribe("b"))
        rig.command(EngineCommand.Resubscribe("unknown"))
        assertEquals(listOf("SUB $K2@1,$K3@2"), rig.wire(from), "applied QoS of the filters, no other group")

        rig.online = false
        rig.drop()
        val offline = rig.packets.size
        rig.command(EngineCommand.Resubscribe(null))
        rig.command(EngineCommand.Resubscribe("b"))
        assertEquals(offline, rig.packets.size, "nothing without a session")

        rig.online = true
        rig.advanceUntil { rig.started().size == 2 }
        rig.eofOnDisconnect = false
        rig.command(EngineCommand.Close())
        val closing = rig.packets.size
        rig.command(EngineCommand.Resubscribe(null))
        rig.command(EngineCommand.SetGroup("d", setOf(ON), Qos.AT_MOST_ONCE))
        assertEquals(closing, rig.packets.size, "nothing while closing")
    }

    /** `afterReconnectTheWholeSetIsSubscribedAgain`: весь набор порциями, без UNSUBSCRIBE. */
    @Test
    fun afterReconnectTheWholeSetIsSubscribedAgainInBatches() {
        val rig = Rig()
        rig.command(EngineCommand.SetGroup("big", (1..120).map { "t/$it" }.toSet(), Qos.AT_MOST_ONCE))
        rig.start()
        val before = rig.active()

        rig.drop()
        rig.advanceUntil { rig.started().size == 2 }
        assertEquals(before, rig.active())
        val second = rig.packets.drop(rig.packets.indexOfLast { it is MQTTConnect })
        assertEquals(listOf(50, 50, 21), second.filterIsInstance<MQTTSubscribe>().map { it.subscriptions.size })
        assertTrue(second.none { it is MQTTUnsubscribe })
    }

    /** Разницы времени обрыва не копятся: после CONNACK — один полный набор. */
    @Test
    fun groupChurnWithoutASessionGivesOneFullSetAfterConnack() {
        val rig = Rig()
        rig.start()
        rig.online = false
        rig.drop()

        rig.command(EngineCommand.SetGroup("b", setOf(K2), Qos.AT_MOST_ONCE))
        rig.command(EngineCommand.SetGroup(TEMP1, setOf(WIDE_STATE), Qos.AT_MOST_ONCE))
        rig.command(EngineCommand.SetGroup("b", setOf(K3), Qos.AT_MOST_ONCE))
        rig.command(EngineCommand.SetGroup(TEMP1, emptySet(), Qos.AT_MOST_ONCE))
        assertEquals(mapOf(K1 to TopicState.Pending, K3 to TopicState.Pending), rig.topics())

        rig.online = true
        rig.advanceUntil { rig.started().size == 2 }
        val second = rig.packets.drop(rig.packets.indexOfLast { it is MQTTConnect })
        assertEquals(listOf("SUB $K1@0,$K3@0"), rig.wire(rig.packets.size - second.size))
    }

    /** `connectionLossMakesValuesStale…`: после обрыва всё `Pending`; новая эпоха подписывает и выдаёт свои сообщения. */
    @Test
    fun statesReturnToPendingAfterADropAndTheNewEpochGrantsThem() {
        val rig = Rig()
        rig.start()
        assertEquals(TopicState.Granted(Qos.AT_MOST_ONCE), rig.topics()[K1])

        rig.online = false
        rig.drop()
        assertEquals(mapOf(K1 to TopicState.Pending), rig.topics())
        val endIndex = rig.actions.indexOfFirst { it is EngineAction.Emit && it.incoming is MqttIncoming.SessionEnded }
        val pendingIndex = rig.actions.indexOfLast { it is EngineAction.TopicStates }
        assertTrue(pendingIndex < endIndex, "states are pending before the end of the session is emitted")

        rig.online = true
        rig.advanceUntil { rig.started().size == 2 }
        assertEquals(TopicState.Granted(Qos.AT_MOST_ONCE), rig.topics()[K1])
        rig.brokerSends(rig.script.publish(K1, "0".encodeToByteArray(), retain = true))
        assertEquals(listOf(102L), rig.emitted().filterIsInstance<MqttIncoming.Message>().map { it.epoch })
    }

    // ---- сопоставление SUBACK/UNSUBACK ----

    @Test
    fun subackOfARemovedOrResentFilterDoesNotConfuseTheState() {
        val rig = Rig()
        rig.suback = Suback.SILENT
        rig.start()
        val first = rig.lastSubscribeId()

        // Повтор до SUBACK: применяется только код последнего SUBSCRIBE фильтра
        rig.command(EngineCommand.Resubscribe(null))
        val second = rig.lastSubscribeId()
        rig.brokerSends(rig.script.suback(first, listOf(ReasonCode.NOT_AUTHORIZED)))
        assertEquals(TopicState.Pending, rig.topics()[K1], "an older SUBACK does not decide")
        assertTrue(rig.refusals().isEmpty())
        rig.brokerSends(rig.script.suback(second, listOf(ReasonCode.SUCCESS)))
        assertEquals(TopicState.Granted(Qos.AT_MOST_ONCE), rig.topics()[K1])

        // Снятие до SUBACK: поздний отказ снятого фильтра ничего не выдаёт; UNSUBACK состояния не меняет
        rig.command(EngineCommand.SetGroup("b", setOf(K2), Qos.AT_MOST_ONCE))
        val third = rig.lastSubscribeId()
        rig.command(EngineCommand.SetGroup("b", emptySet(), Qos.AT_MOST_ONCE))
        val states = rig.topics()
        rig.brokerSends(rig.script.suback(third, listOf(ReasonCode.NOT_AUTHORIZED)))
        assertTrue(rig.refusals().isEmpty())
        assertEquals(states, rig.topics())
        assertEquals(mapOf(K1 to TopicState.Granted(Qos.AT_MOST_ONCE)), states)
        assertTrue(rig.ended().isEmpty())
    }

    /** Эпохи: ожидание SUBACK прошлого подключения забывается; новое ядро начинает packetId заново. */
    @Test
    fun anOutstandingSubackOfALostConnectionDoesNotLeakIntoTheNextEpoch() {
        val rig = Rig()
        rig.suback = Suback.SILENT
        rig.start()
        val lost = rig.lastSubscribeId()
        rig.drop()
        rig.suback = Suback.GRANTED
        rig.advanceUntil { rig.started().size == 2 }

        assertEquals(lost, rig.lastSubscribeId(), "the new core reuses the packet id")
        assertEquals(TopicState.Granted(Qos.AT_MOST_ONCE), rig.topics()[K1])
        assertTrue(rig.refusals().isEmpty() && rig.logs().none { it is EngineLogEvent.Refused })
    }

    @Test
    fun missingSubackCodesCountAsRefusals() {
        val rig = Rig()
        rig.suback = Suback.SILENT
        rig.command(EngineCommand.SetGroup("working", setOf(K1, K2), Qos.AT_MOST_ONCE))
        rig.start()
        rig.brokerSends(rig.script.suback(rig.lastSubscribeId(), listOf(ReasonCode.SUCCESS)))

        assertEquals(TopicState.Granted(Qos.AT_MOST_ONCE), rig.topics()[K1])
        assertEquals(TopicState.Refused(ReasonCode.UNSPECIFIED_ERROR), rig.topics()[K2])
        assertEquals(listOf(SubscribeRefused("working", K2, ReasonCode.UNSPECIFIED_ERROR)), rig.refusals())
    }

    @Test
    fun aGroupAddingAnAlreadyRefusedFilterLearnsTheRefusalWithoutTraffic() {
        val rig = Rig()
        rig.subackCode = { if (it == WIDE_STATE) ReasonCode.NOT_AUTHORIZED else ReasonCode.SUCCESS }
        rig.command(EngineCommand.SetGroup("working", setOf(K1, WIDE_STATE), Qos.AT_MOST_ONCE))
        rig.start()
        assertEquals(listOf(SubscribeRefused("working", WIDE_STATE, ReasonCode.NOT_AUTHORIZED)), rig.refusals())
        val from = rig.packets.size

        rig.command(EngineCommand.SetGroup(TEMP1, setOf(WIDE_STATE), Qos.AT_MOST_ONCE))
        assertEquals(emptyList<String>(), rig.wire(from), "no SUBSCRIBE: the filter is already subscribed (refused)")
        assertEquals(SubscribeRefused(TEMP1, WIDE_STATE, ReasonCode.NOT_AUTHORIZED), rig.refusals().last())
        // Повторная установка той же группы отказ не дублирует
        rig.command(EngineCommand.SetGroup("working", setOf(K1, WIDE_STATE), Qos.AT_MOST_ONCE))
        assertEquals(2, rig.refusals().size)
    }

    /** Фильтр двух групп: отказ — по событию на каждую группу (приложение журналит различные фильтры). */
    @Test
    fun aRefusedFilterOfTwoGroupsGivesAnEventPerGroup() {
        val rig = Rig()
        rig.subackCode = { if (it == K2) ReasonCode.NOT_AUTHORIZED else ReasonCode.SUCCESS }
        rig.command(EngineCommand.SetGroup("a", setOf(K2), Qos.AT_MOST_ONCE))
        rig.command(EngineCommand.SetGroup("b", setOf(K2), Qos.AT_MOST_ONCE))
        rig.start()
        assertEquals(listOf("a", "b"), rig.refusals().map { it.group })
    }

    /** Порядок команд (FIFO): окно обнаружения и публикация уходят в сокет в порядке вызовов. */
    @Test
    fun temporaryWindowKeepsFifoOrderWithPublishes() {
        val rig = Rig()
        rig.start()
        val from = rig.packets.size
        rig.command(EngineCommand.SetGroup(TEMP1, setOf(WIDE_STATE), Qos.AT_MOST_ONCE))
        rig.publish(GET)
        rig.command(EngineCommand.SetGroup(TEMP1, emptySet(), Qos.AT_MOST_ONCE))
        assertEquals(listOf("SUB $WIDE_STATE@0", "PUB $GET", "UNSUB $WIDE_STATE"), rig.wire(from))
    }

    /** Стабильность: SUBACK снятого позже фильтра всё равно подтверждает свой SUBSCRIBE. */
    @Test
    fun subackOfAFilterRemovedMeanwhileStillCountsForTheStableSession() {
        val rig = Rig()
        rig.suback = Suback.SILENT
        rig.online = false
        rig.start()
        rig.advance(3_500) // попытки в 0, 1, 3 с: следующая задержка — 4 с
        rig.online = true
        rig.advanceUntil { rig.started().isNotEmpty() }
        val first = rig.lastSubscribeId()
        rig.command(EngineCommand.SetGroup("working", emptySet(), Qos.AT_MOST_ONCE))
        rig.brokerSends(rig.script.suback(first, listOf(ReasonCode.SUCCESS)))

        rig.markConnectTime()
        rig.drop()
        rig.advanceUntil { rig.started().size >= 2 }
        assertEquals(listOf(1_000L), rig.gaps(), "the session was stable: delay starts from 1 s")
    }

    // ---- maxFilters ----

    @Test
    fun maxFiltersRejectsTheCommandAndKeepsTheGroup() {
        val rig = Rig(maxFilters = 3)
        rig.start()
        rig.command(EngineCommand.SetGroup("b", setOf(K2), Qos.AT_MOST_ONCE))
        val from = rig.packets.size
        val actions = rig.actions.size

        rig.command(EngineCommand.SetGroup("b", setOf(K2, K3, ON), Qos.AT_MOST_ONCE))
        assertEquals(listOf<EngineAction>(EngineAction.GroupRejected("b", 4, 3)), rig.actions.drop(actions))
        assertEquals(emptyList<String>(), rig.wire(from))
        assertEquals(setOf(K1, K2), rig.topics().keys, "the group is unchanged")

        // Снятие группы не отклоняется; набор в пределе — проходит
        rig.command(EngineCommand.SetGroup("b", emptySet(), Qos.AT_MOST_ONCE))
        rig.command(EngineCommand.SetGroup("c", setOf(K2, K3), Qos.AT_MOST_ONCE))
        assertEquals(listOf("UNSUB $K2", "SUB $K2@0,$K3@0"), rig.wire(from))
        // Без сессии предел тот же
        rig.drop()
        rig.command(EngineCommand.SetGroup(TEMP1, setOf(WIDE_STATE), Qos.AT_MOST_ONCE))
        assertTrue(rig.actions.last() is EngineAction.GroupRejected)
        assertFalse(WIDE_STATE in rig.topics())
    }

    // ---- недопустимые фильтры ----

    @Test
    fun invalidFiltersAreNeverSentAndAreRefusedLocallyWhileTheRestWorks() {
        val rig = Rig()
        rig.start()
        val from = rig.packets.size
        val bad = listOf("", "a/#/b", "a#", "a+/b", "\$share/g", "\$share//a", "\$share/+/a", "\$share/g/a/#x")

        rig.command(EngineCommand.SetGroup("g", setOf(K2) + bad, Qos.AT_MOST_ONCE))

        assertEquals(listOf("SUB $K2@0"), rig.wire(from), "only the valid filter goes to the broker")
        assertEquals(TopicState.Granted(Qos.AT_MOST_ONCE), rig.topics()[K2])
        for (filter in bad) assertEquals(TopicState.Refused(ReasonCode.TOPIC_FILTER_INVALID), rig.topics()[filter], filter)
        assertEquals(
            bad.map { SubscribeRefused("g", it, ReasonCode.TOPIC_FILTER_INVALID) },
            rig.refusals(),
            "one event per invalid filter and group"
        )
        assertEquals(mapOf(K1 to 0, K2 to 0), rig.active())
        assertTrue(rig.ended().isEmpty(), "the session lives")

        // Тот же набор ещё раз: тишина (ни пакетов, ни новых событий)
        val events = rig.refusals().size
        rig.command(EngineCommand.SetGroup("g", setOf(K2) + bad, Qos.AT_MOST_ONCE))
        assertEquals(listOf("SUB $K2@0"), rig.wire(from))
        assertEquals(events, rig.refusals().size)

        // Исправленная группа: недопустимые уходят из состояния
        rig.command(EngineCommand.SetGroup("g", setOf(K2), Qos.AT_MOST_ONCE))
        assertEquals(setOf(K1, K2), rig.topics().keys)
        // Снятие группы убирает и её недопустимые фильтры
        rig.command(EngineCommand.SetGroup("h", setOf("x/#/y"), Qos.AT_MOST_ONCE))
        assertEquals(TopicState.Refused(ReasonCode.TOPIC_FILTER_INVALID), rig.topics()["x/#/y"])
        rig.command(EngineCommand.SetGroup("h", emptySet(), Qos.AT_MOST_ONCE))
        assertEquals(setOf(K1, K2), rig.topics().keys)
    }

    @Test
    fun invalidFiltersWithoutSessionAreRefusedAndNotSubscribedAfterConnack() {
        val rig = Rig()
        rig.command(EngineCommand.SetGroup("g", setOf(K2, "a/#/b"), Qos.AT_MOST_ONCE))
        assertEquals(TopicState.Pending, rig.topics()[K2])
        assertEquals(TopicState.Refused(ReasonCode.TOPIC_FILTER_INVALID), rig.topics()["a/#/b"])
        assertEquals(listOf(SubscribeRefused("g", "a/#/b", ReasonCode.TOPIC_FILTER_INVALID)), rig.refusals())

        rig.start()
        assertEquals(listOf("SUB $K1@0,$K2@0"), rig.wire())
        assertEquals(TopicState.Refused(ReasonCode.TOPIC_FILTER_INVALID), rig.topics()["a/#/b"])
        assertEquals(TopicState.Granted(Qos.AT_MOST_ONCE), rig.topics()[K2])
    }

    @Test
    fun invalidFiltersDoNotCountTowardsMaxFilters() {
        val rig = Rig(maxFilters = 2)
        rig.start()
        rig.command(EngineCommand.SetGroup("g", setOf(K2, "a/#/b", "", "+x"), Qos.AT_MOST_ONCE))
        assertTrue(rig.actions.none { it is EngineAction.GroupRejected })
        assertEquals(mapOf(K1 to 0, K2 to 0), rig.active())
    }

    // ---- H11 ----

    @Test
    fun subscriptionActionsDoNotPrintFiltersOrGroups() {
        val rig = Rig(maxFilters = 3)
        rig.subackCode = { if ("refused" in it) ReasonCode.NOT_AUTHORIZED else ReasonCode.SUCCESS }
        rig.command(EngineCommand.SetGroup("secret-group", setOf("secret/ok", "secret/refused"), Qos.AT_MOST_ONCE))
        rig.start()
        rig.command(EngineCommand.SetGroup("secret-temp", setOf("secret/a", "secret/b"), Qos.AT_MOST_ONCE))
        rig.command(EngineCommand.Resubscribe("secret-group"))
        val refusal = rig.refusals().first()
        val printed = rig.actions.joinToString { it.toString() } + refusal + TopicState.Refused(ReasonCode.NOT_AUTHORIZED) +
            EngineCommand.SetGroup("secret-group", setOf("secret/x"), Qos.AT_MOST_ONCE, refreshShared = true) +
            EngineCommand.Resubscribe("secret-group")
        assertTrue(rig.actions.any { it is EngineAction.GroupRejected } && rig.actions.any { it is EngineAction.TopicStates })
        for (secret in listOf("secret", HOST)) assertFalse(printed.contains(secret), "leaked '$secret': $printed")
    }

    // ---- стенд ----

    private fun Rig.topics(): Map<String, TopicState> =
        actions.filterIsInstance<EngineAction.TopicStates>().lastOrNull()?.states ?: emptyMap()

    private fun Rig.refusals(): List<SubscribeRefused> =
        actions.filterIsInstance<EngineAction.SubscriptionRefused>().map { it.refusal }

    private fun Rig.lastSubscribeId(): Int = packets.filterIsInstance<MQTTSubscribe>().last().packetIdentifier.toInt()

    /** Подписки у брокера: SUBSCRIBE/UNSUBSCRIBE с последнего CONNECT (фильтр → QoS). */
    private fun Rig.active(): Map<String, Int> {
        val result = LinkedHashMap<String, Int>()
        for (packet in packets.drop(maxOf(0, packets.indexOfLast { it is MQTTConnect }))) {
            when (packet) {
                is MQTTSubscribe -> packet.subscriptions.forEach { result[it.topicFilter] = it.options.qos.value }
                is MQTTUnsubscribe -> packet.topicFilters.forEach { result.remove(it) }
                else -> Unit
            }
        }
        return result
    }

    /** Подписки и публикации, полученные брокером с пакета [from], в порядке записи. */
    private fun Rig.wire(from: Int = 0): List<String> = packets.drop(from).mapNotNull { packet ->
        when (packet) {
            is MQTTSubscribe -> "SUB " + packet.subscriptions.joinToString(",") { "${it.topicFilter}@${it.options.qos.value}" }
            is MQTTUnsubscribe -> "UNSUB " + packet.topicFilters.joinToString(",")
            is MQTTPublish -> "PUB " + packet.topicName
            else -> null
        }
    }

    private companion object {
        const val DEVICE = "/devices/dev_1"
        const val ERROR = "$DEVICE/meta/error"
        const val K2 = "$DEVICE/controls/K2"
        const val K3 = "$DEVICE/controls/K3"
        const val ON = "$DEVICE/controls/K1/on"
        const val GET = "$DEVICE/controls/K1/get"
        const val WIDE_STATE = "/devices/+/controls/+"
        const val WIDE_ERROR = "/devices/+/meta/error"
        const val META = "/devices/+/meta"
        const val DEVICE_META = "$DEVICE/controls/+/meta"
        const val TEMP1 = "temporary-1"
        const val TEMP2 = "temporary-2"
    }
}
