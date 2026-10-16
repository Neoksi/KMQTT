package io.github.davidepianca98.client.engine

import io.github.davidepianca98.client.SubscribeRefused
import io.github.davidepianca98.client.TopicState
import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.core.MqttSessionCore
import io.github.davidepianca98.client.core.SubscriptionRequest
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode

/**
 * Декларативные подписки движка (S06b-T03b, W6 §4.3): перенос групп `MQTTConnection` (`setSubscriptions`,
 * `resubscribeAll`, `refreshSubscriptions`, `refreshGroup`, `onSubscribeFailed`). Однопоточный, без ввода-вывода:
 * пакеты отдаются ядру текущего подключения (`core` в вызовах; `null` — живой сессии нет), события — в [actions].
 *
 * - Желаемое = объединение групп в порядке их появления; QoS фильтра — максимум по группам. Фильтр, общий для
 *   нескольких групп, держится, пока он есть хоть в одной (временные группы обнаружения поверх рабочих).
 * - В живой сессии изменение группы уходит разницей по порядку команд: UNSUBSCRIBE снятых, SUBSCRIBE добавленных (и
 *   сменивших QoS), затем отдельный SUBSCRIBE `refreshShared` (`MQTTConnection.kt:536-565`). Без живой сессии
 *   меняется только желаемое: после CONNACK подписывается весь набор порциями [batch] (`resubscribeAll`), так что
 *   разницы времени обрыва не копятся.
 * - SUBACK сопоставляется с фильтрами по packetId и порядку кодов (`MqttClientSession.kt:126-139`): 0/1/2 →
 *   `Granted(qos)`, ≥ 0x80 → `Refused(code)` + [EngineAction.SubscriptionRefused] на каждую группу фильтра; сессия
 *   не рвётся (H7). Кодов меньше, чем фильтров, — недостающие `Refused(UNSPECIFIED_ERROR)`. Код применяется, только
 *   если этот SUBSCRIBE — последний для фильтра (повторы и снятие посреди ожидания не путают состояние).
 * - Группа добавляет фильтр, которому в этой сессии уже отказано (другой группой), — SUBSCRIBE не уходит, а отказ
 *   сразу выдаётся и этой группе (временная группа обнаружения узнаёт отказ без ожидания окна).
 * - Стабильность сессии: все SUBSCRIBE подключения подтверждены SUBACK без отказов ([confirmed]).
 * - Продолжаемая сессия ([persistent], SEVEN-251): брокер помнит подписки между подключениями. Помнится, какие фильтры
 *   могли остаться у брокера (отправлены в SUBSCRIBE и не сняты UNSUBSCRIBE в живой сессии); при Session Present = 1
 *   снятые за время обрыва уходят UNSUBSCRIBE перед подпиской всего набора, при Session Present = 0 память
 *   сбрасывается. Весь желаемый набор подписывается после каждого CONNACK как обычно (брокер заново пришлёт retained).
 */
@OptIn(MqttCoreApi::class)
internal class SubscriptionSet(
    private val batch: Int,
    private val maxFilters: Int,
    private val actions: MutableList<EngineAction>,
    private val persistent: Boolean = false,
) {
    // Продолжаемая сессия: фильтры, которые брокер может держать в своей сессии
    private val held = LinkedHashSet<String>()
    private val groups = LinkedHashMap<String, Map<String, Qos>>()

    // Живая сессия: что подписано (фильтр → QoS); состояние желаемых; SUBSCRIBE в ожидании SUBACK
    private var live = false
    private var applied: Map<String, Qos> = emptyMap()
    private var states = LinkedHashMap<String, TopicState>()
    private val inFlight = HashMap<Int, List<String>>()
    private val lastPacket = HashMap<String, Int>()

    private var subscribesSent = 0
    private var subacksOk = 0

    /** Все SUBSCRIBE подключения подтверждены SUBACK без отказов (хотя бы один был). */
    var confirmed: Boolean = false
        private set

    /** Задать группу; `false` — отклонено по [maxFilters] (выдан [EngineAction.GroupRejected]). */
    fun set(
        group: String,
        filters: Set<String>,
        qos: Qos,
        refreshShared: Boolean,
        core: MqttSessionCore?,
        nowMs: Long,
    ): Boolean {
        val previous = groups[group]
        if (filters.isEmpty()) groups.remove(group) else groups[group] = filters.associateWith { qos }
        val target = desired()
        if (filters.isNotEmpty() && target.size > maxFilters) {
            if (previous == null) groups.remove(group) else groups[group] = previous
            actions += EngineAction.GroupRejected(group, target.size, maxFilters)
            return false
        }
        if (!live || core == null) {
            updateStates(target)
            return true
        }
        val removed = applied.keys.filter { it !in target }
        val added = LinkedHashMap<String, Qos>()
        val shared = LinkedHashMap<String, Qos>()
        for ((filter, wanted) in target) {
            val had = applied[filter]
            when {
                had != wanted -> added[filter] = wanted
                refreshShared && filter in filters -> shared[filter] = wanted
            }
        }
        // Фильтр, новый для группы, которому в сессии уже отказано: SUBSCRIBE не уходит, отказ — сразу и ей
        val known = filters.filter { it in applied && it !in added && previous?.containsKey(it) != true }
            .mapNotNull { filter -> (states[filter] as? TopicState.Refused)?.let { filter to it } }
        applied = target
        for (filter in removed) lastPacket.remove(filter)
        removed.chunked(batch).forEach { core.unsubscribe(it, nowMs) }
        if (persistent) held.removeAll(removed.toSet())
        val next = LinkedHashMap(states)
        next.keys.retainAll(target.keys)
        subscribe(core, added, next, nowMs)
        subscribe(core, shared, next, nowMs)
        for ((filter, state) in known) if (filter !in shared) refuse(filter, state.code, listOf(group))
        publish(next)
        return true
    }

    /**
     * CONNACK: весь набор порциями (накопленные разницы обрыва не нужны). [resumed] — Session Present = 1 продолжаемой
     * сессии: сначала UNSUBSCRIBE фильтров, снятых за время обрыва, но оставшихся у брокера.
     */
    fun sessionStarted(core: MqttSessionCore, nowMs: Long, resumed: Boolean = false) {
        live = true
        applied = desired()
        if (persistent) {
            if (resumed) {
                val leftovers = held.filter { it !in applied }
                leftovers.chunked(batch).forEach { core.unsubscribe(it, nowMs) }
                held.removeAll(leftovers.toSet())
            } else {
                held.clear()
            }
        }
        val next = LinkedHashMap<String, TopicState>()
        subscribe(core, applied, next, nowMs)
        publish(next)
    }

    /** Конец подключения: всё снова `Pending`, счётчики и ожидания — с нуля. */
    fun sessionEnded() {
        live = false
        applied = emptyMap()
        inFlight.clear()
        lastPacket.clear()
        subscribesSent = 0
        subacksOk = 0
        confirmed = false
        updateStates(desired())
    }

    /** Повторный SUBSCRIBE подписанных фильтров группы [group] (`null` — всех): брокер заново пришлёт retained. */
    fun resubscribe(group: String?, core: MqttSessionCore?, nowMs: Long) {
        if (!live || core == null) return
        val filters = if (group == null) applied else applied.filterKeys { groups[group]?.containsKey(it) == true }
        if (filters.isEmpty()) return
        val next = LinkedHashMap(states)
        subscribe(core, filters, next, nowMs)
        publish(next)
    }

    fun onSubAck(packetId: Int, codes: List<ReasonCode>) {
        val filters = inFlight.remove(packetId) ?: return
        var refused = 0
        val next = LinkedHashMap(states)
        filters.forEachIndexed { i, filter ->
            val code = codes.getOrNull(i) ?: ReasonCode.UNSPECIFIED_ERROR
            val failed = code.value >= 0x80
            if (failed) refused++
            if (lastPacket[filter] != packetId) return@forEachIndexed
            lastPacket.remove(filter)
            if (filter !in next) return@forEachIndexed
            next[filter] = if (failed) TopicState.Refused(code) else TopicState.Granted(grantedQos(code))
            if (failed) refuse(filter, code, groupsOf(filter))
        }
        if (refused == 0) subacksOk++ else actions += EngineAction.Log(EngineLogEvent.Refused(refused))
        if (subscribesSent > 0 && subacksOk >= subscribesSent) confirmed = true
        publish(next)
    }

    private fun subscribe(
        core: MqttSessionCore,
        filters: Map<String, Qos>,
        next: MutableMap<String, TopicState>,
        nowMs: Long,
    ) {
        if (filters.isEmpty()) return
        actions += EngineAction.Log(EngineLogEvent.Subscribe(filters.size))
        filters.entries.chunked(batch).forEach { chunk ->
            val packetId = core.subscribe(chunk.map { SubscriptionRequest(it.key, it.value) }, nowMs)
            if (persistent) chunk.forEach { held += it.key }
            subscribesSent++
            inFlight[packetId] = chunk.map { it.key }
            for (entry in chunk) {
                lastPacket[entry.key] = packetId
                next[entry.key] = TopicState.Pending
            }
        }
    }

    private fun refuse(filter: String, code: ReasonCode, owners: List<String>) {
        for (owner in owners) actions += EngineAction.SubscriptionRefused(SubscribeRefused(owner, filter, code))
    }

    /** Без живой сессии: желаемые фильтры — `Pending`, снятые уходят из состояния. */
    private fun updateStates(target: Map<String, Qos>) {
        val next = LinkedHashMap<String, TopicState>()
        for (filter in target.keys) next[filter] = if (live) states[filter] ?: TopicState.Pending else TopicState.Pending
        publish(next)
    }

    private fun publish(next: LinkedHashMap<String, TopicState>) {
        if (next == states) return
        states = next
        actions += EngineAction.TopicStates(next.toMap())
    }

    private fun desired(): Map<String, Qos> {
        val result = LinkedHashMap<String, Qos>()
        for (filters in groups.values) {
            for ((filter, qos) in filters) {
                val known = result[filter]
                if (known == null || qos.value > known.value) result[filter] = qos
            }
        }
        return result
    }

    private fun groupsOf(filter: String): List<String> = groups.filterValues { filter in it }.keys.toList()

    private fun grantedQos(code: ReasonCode): Qos = when (code.value) {
        1 -> Qos.AT_LEAST_ONCE
        2 -> Qos.EXACTLY_ONCE
        else -> Qos.AT_MOST_ONCE
    }
}
