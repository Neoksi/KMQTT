package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.engine.EngineCommand
import io.github.davidepianca98.client.internal.ClientRequest
import io.github.davidepianca98.client.internal.MqttClientImpl
import io.github.davidepianca98.mqtt.packets.Qos
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch

/**
 * Declarative subscriptions of an [MqttClient]. Subscriptions are set in named **groups**: the desired set is the union
 * of all groups, and the QoS of a filter is the maximum over the groups that contain it. In a live session a change
 * sends only the difference (UNSUBSCRIBE of the removed filters, then SUBSCRIBE of the added ones) in the order of the
 * calls; after every CONNACK the whole desired set is subscribed again, in batches of [MqttLimits.subscribeBatch]
 * filters (a session is always clean, so the broker remembers nothing).
 *
 * A broker refusal for a filter (SUBACK 0x80 or above) does not end the session: the filter gets [TopicState.Refused]
 * and an event is emitted on [refusals]. A group that adds a filter already refused in the current session gets the
 * refusal at once without another SUBSCRIBE. Filters are unsubscribed in batches of [MqttLimits.subscribeBatch] too.
 *
 * A filter that is not a valid MQTT topic filter (empty, containing U+0000, longer than 65 535 bytes in UTF-8, `#`
 * that is not the whole last level, `+` that is not a whole level, a malformed `$share/<name>/<filter>`) is never sent
 * to the broker: it gets [TopicState.Refused] with [io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode.TOPIC_FILTER_INVALID]
 * and an event on [refusals] for its group, while the other filters of the group are subscribed as usual; such a
 * filter does not count towards [MqttLimits.maxFilters].
 *
 * The methods do not block and are queued together with publications, so they are executed in the order of the calls.
 * Group names of the form `temporary-<n>` are reserved for [withTemporary].
 */
public interface MqttSubscriptions {
    /**
     * State of every desired filter in the current session. Without a live session all filters are
     * [TopicState.Pending]. A repeated SUBSCRIBE of a filter returns it to [TopicState.Pending] until the new SUBACK.
     */
    public val state: StateFlow<Map<String, TopicState>>

    /**
     * Refusals of the broker (typically an access control list): one event for every group that contains a refused
     * filter; the session continues. This is a shared flow without replay and with a buffer of 64 events: only active
     * collectors see an event, and if a slow collector overflows the buffer the new event is lost (the state of the
     * filter always stays in [state]).
     */
    public val refusals: Flow<SubscribeRefused>

    /**
     * Sets the filters of group [group] to [filters]; an empty set removes the group. If the groups together would hold
     * more than [MqttLimits.maxFilters] distinct filters the call is refused: the group stays unchanged,
     * [MqttLogger.groupRejected] is called, and [awaitApplied] of that group throws [IllegalStateException] until a
     * later `set` of the group succeeds. After the client was closed the call does nothing.
     *
     * @param qos requested QoS of the filters of this group.
     * @param refreshShared when `true`, filters that another group has already subscribed are subscribed once more, so
     *   that the broker sends their retained messages again.
     */
    public fun set(group: String, filters: Set<String>, qos: Qos = Qos.AT_MOST_ONCE, refreshShared: Boolean = false)

    /**
     * Makes the group follow the flow [filters] while [scope] is alive and the client is not closed. When the returned
     * [Job] ends (the scope is cancelled, the flow completes, or the client is closed) the group is removed.
     */
    public fun bind(group: String, filters: Flow<Set<String>>, qos: Qos, scope: CoroutineScope): Job

    /**
     * Suspends until the broker answered (SUBACK) for every filter of [group] in the current session; without a live
     * session it waits for the connection. Returns the state of the filters of the group, each
     * [TopicState.Granted] or [TopicState.Refused]; an empty or unknown group returns an empty map at once. It sees all
     * `set` calls made before it. Throws [IllegalStateException] when the client is closed (by [MqttClient.close] or by
     * itself, with [ReconnectPolicy.Never] or exhausted attempts) or when the last `set` of the group was refused for
     * [MqttLimits.maxFilters].
     */
    public suspend fun awaitApplied(group: String): Map<String, TopicState>

    /**
     * Subscribes [filters] in a temporary group for the duration of [block] and removes the group afterwards, also when
     * [block] is cancelled or throws. A broker refusal for any of the filters completes [TemporaryGroup.refused], which
     * allows telling "not allowed" from "nothing arrived in time" (for example while discovering devices). Throws
     * [IllegalStateException] when the client is already closed.
     */
    public suspend fun <T> withTemporary(
        filters: Set<String>,
        qos: Qos = Qos.AT_MOST_ONCE,
        block: suspend (TemporaryGroup) -> T,
    ): T

    /**
     * Subscribes the filters of [group] again (`null` means all groups) so that the broker re-sends their retained
     * messages. Does nothing without a live session.
     */
    public fun resubscribe(group: String? = null)
}

/** A temporary group created by [MqttSubscriptions.withTemporary]. */
public interface TemporaryGroup {
    /**
     * Completes with the first refusal of the broker for a filter of the group. It completes exceptionally when the
     * client is closed and is cancelled when the block of [MqttSubscriptions.withTemporary] ends.
     */
    public val refused: Deferred<SubscribeRefused>
}

/**
 * Реализация [MqttSubscriptions]. Публичные методы — с любого потока (кладут запрос в очередь клиента); методы
 * `on*`/`add*`/`temporary*`/`failAll` — только на потоке хоста (вызывает [MqttClientImpl]).
 */
@OptIn(MqttCoreApi::class)
internal class MqttSubscriptionsImpl(
    private val submit: (ClientRequest) -> Boolean,
    private val closed: Deferred<Unit>,
) : MqttSubscriptions {
    private val states = MutableStateFlow<Map<String, TopicState>>(emptyMap())
    private val refusalFlow = MutableSharedFlow<SubscribeRefused>(replay = 0, extraBufferCapacity = 64)

    override val state: StateFlow<Map<String, TopicState>> = states.asStateFlow()
    override val refusals: SharedFlow<SubscribeRefused> = refusalFlow.asSharedFlow()

    // ---- поток хоста ----
    private val groups = HashMap<String, Set<String>>()
    private val rejected = HashSet<String>()
    private val waiters = ArrayList<ClientRequest.AwaitApplied>()
    private val temporary = HashMap<String, CompletableDeferred<SubscribeRefused>>()

    override fun set(group: String, filters: Set<String>, qos: Qos, refreshShared: Boolean) {
        submit(ClientRequest.Command(EngineCommand.SetGroup(group, filters.toSet(), qos, refreshShared)))
    }

    override fun resubscribe(group: String?) {
        submit(ClientRequest.Command(EngineCommand.Resubscribe(group)))
    }

    override fun bind(group: String, filters: Flow<Set<String>>, qos: Qos, scope: CoroutineScope): Job =
        scope.launch {
            try {
                coroutineScope {
                    val collector = launch { filters.collect { set(group, it, qos) } }
                    val watcher = launch {
                        closed.await()
                        collector.cancel()
                    }
                    collector.join()
                    watcher.cancel()
                }
            } finally {
                set(group, emptySet(), qos)
            }
        }

    override suspend fun awaitApplied(group: String): Map<String, TopicState> {
        val result = CompletableDeferred<Map<String, TopicState>>()
        check(submit(ClientRequest.AwaitApplied(group, result))) { "MqttClient closed" }
        return result.await()
    }

    override suspend fun <T> withTemporary(
        filters: Set<String>,
        qos: Qos,
        block: suspend (TemporaryGroup) -> T,
    ): T {
        val group = TEMPORARY_PREFIX + MqttClientImpl.nextTemporary()
        val refused = CompletableDeferred<SubscribeRefused>()
        check(submit(ClientRequest.TemporaryStart(group, filters.toSet(), qos, refused))) { "MqttClient closed" }
        try {
            return block(TemporaryGroupImpl(refused))
        } finally {
            refused.cancel()
            submit(ClientRequest.TemporaryEnd(group, qos))
        }
    }

    /** `SetGroup` исполнен движком; [wasRejected] — выдан `GroupRejected` (группа не изменилась). */
    fun onSetGroup(command: EngineCommand.SetGroup, wasRejected: Boolean) {
        val group = command.group
        if (wasRejected) {
            rejected += group
        } else {
            rejected -= group
            if (command.filters.isEmpty()) groups.remove(group) else groups[group] = command.filters
        }
        checkWaiters()
    }

    fun onTopicStates(snapshot: Map<String, TopicState>) {
        states.value = snapshot
        checkWaiters()
    }

    fun onRefused(refusal: SubscribeRefused) {
        refusalFlow.tryEmit(refusal)
        temporary[refusal.group]?.complete(refusal)
    }

    fun addWaiter(request: ClientRequest.AwaitApplied) {
        waiters += request
        checkWaiters()
    }

    fun temporaryStarted(group: String, refused: CompletableDeferred<SubscribeRefused>) {
        temporary[group] = refused
    }

    fun temporaryEnded(group: String) {
        temporary.remove(group)
    }

    /** Клиент закрылся (сам или через `close()`): ждущие [awaitApplied] завершаются исключением. */
    fun failWaiters() {
        val error = IllegalStateException("MqttClient closed")
        for (waiter in waiters) waiter.result.completeExceptionally(error)
        waiters.clear()
    }

    /** Клиент закончен: ожидания завершаются исключением. */
    fun failAll() {
        val error = IllegalStateException("MqttClient closed")
        failWaiters()
        for (refused in temporary.values) refused.completeExceptionally(error)
        temporary.clear()
    }

    private fun checkWaiters() {
        if (waiters.isEmpty()) return
        val current = states.value
        val iterator = waiters.iterator()
        while (iterator.hasNext()) {
            val waiter = iterator.next()
            if (waiter.group in rejected) {
                iterator.remove()
                waiter.result.completeExceptionally(IllegalStateException("subscriptions.set rejected: maxFilters"))
                continue
            }
            val filters = groups[waiter.group].orEmpty()
            val applied = filters.associateWith { current[it] ?: TopicState.Pending }
            if (applied.values.none { it == TopicState.Pending }) {
                iterator.remove()
                waiter.result.complete(applied)
            }
        }
    }

    private class TemporaryGroupImpl(override val refused: Deferred<SubscribeRefused>) : TemporaryGroup

    private companion object {
        const val TEMPORARY_PREFIX = "temporary-"
    }
}
