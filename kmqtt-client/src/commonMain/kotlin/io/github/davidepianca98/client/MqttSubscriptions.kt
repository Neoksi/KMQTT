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
 * Декларативные подписки клиента (W6 §4.1, §4.3). Подписки задаются **группами**: желаемое = объединение групп (QoS
 * фильтра — максимум по группам). В живой сессии изменение отправляет разницу (UNSUBSCRIBE снятых, затем SUBSCRIBE
 * добавленных) в порядке вызовов; после каждого CONNACK весь набор подписывается заново порциями
 * [MqttLimits.subscribeBatch]. Отказ брокера по фильтру (SUBACK ≥ 0x80) сессию не рвёт (H7): фильтр получает
 * [TopicState.Refused], выдаётся событие [refusals].
 *
 * Методы не блокируют и исполняются в одной очереди с публикациями (FIFO). Имена групп вида `temporary-<n>` заняты
 * [withTemporary].
 *
 * Отличия от `MQTTConnection` приложения (S06b-T03b): группа, добавившая фильтр, которому в этой сессии уже отказано,
 * получает отказ сразу (SUBSCRIBE не уходит); UNSUBSCRIBE уходит порциями по [MqttLimits.subscribeBatch]; повторный
 * SUBSCRIBE переводит фильтр в [TopicState.Pending] до нового SUBACK.
 */
public interface MqttSubscriptions {
    /** Состояние каждого желаемого фильтра в текущей эпохе (без сессии — все [TopicState.Pending]). */
    public val state: StateFlow<Map<String, TopicState>>

    /**
     * Отказы брокера (ACL): событие на каждую группу, где есть отказанный фильтр; сессия продолжается.
     * `SharedFlow` без повтора с буфером 64: событие видят только активные подписчики, при переполнении буфера
     * медленным подписчиком новое событие теряется (состояние фильтра остаётся в [state]).
     */
    public val refusals: Flow<SubscribeRefused>

    /**
     * Задать набор группы [group] (пустой — снять группу). Набор, при котором различных фильтров во всех группах
     * стало бы больше [MqttLimits.maxFilters], отклоняется: группа не меняется, `MqttLogger.groupRejected`, а
     * [awaitApplied] этой группы бросает `IllegalStateException`, пока следующий `set` группы не пройдёт.
     *
     * [refreshShared] (дополнение к эскизу W6 §4.1, нужно миграции `setSubscriptions(refreshShared)`): фильтры
     * группы, уже подписанные другой группой, подписываются ещё раз — брокер заново пришлёт их retained.
     * После `close()` — ничего.
     */
    public fun set(group: String, filters: Set<String>, qos: Qos = Qos.AT_MOST_ONCE, refreshShared: Boolean = false)

    /**
     * Группа следует за потоком наборов [filters], пока жив [scope] и клиент не закрыт. Когда задание кончается
     * (отмена [scope], конец потока, `close()`), группа снимается.
     */
    public fun bind(group: String, filters: Flow<Set<String>>, qos: Qos, scope: CoroutineScope): Job

    /**
     * Ждать SUBACK по всем фильтрам группы в текущей эпохе (нет живой сессии — ждёт подключения). Возвращает
     * состояние фильтров группы (каждое — [TopicState.Granted] или [TopicState.Refused]); пустая или неизвестная
     * группа — сразу пустая карта. Видит все `set`, вызванные раньше. После `close()` — `IllegalStateException`.
     */
    public suspend fun awaitApplied(group: String): Map<String, TopicState>

    /**
     * Временная группа (`temporary-<n>`) на время [block]; снимается и при отмене/исключении. Отказ брокера по любому
     * её фильтру завершает [TemporaryGroup.refused] (обнаружение устройств: «нет прав» против «окно истекло»).
     */
    public suspend fun <T> withTemporary(
        filters: Set<String>,
        qos: Qos = Qos.AT_MOST_ONCE,
        block: suspend (TemporaryGroup) -> T,
    ): T

    /**
     * Повторный SUBSCRIBE подписанных фильтров группы [group] (`null` — всех): брокер заново пришлёт retained
     * (`refreshGroup`/`refreshSubscriptions` приложения). Без живой сессии — ничего.
     */
    public fun resubscribe(group: String? = null)
}

/** Временная группа [MqttSubscriptions.withTemporary]. */
public interface TemporaryGroup {
    /** Первый отказ брокера по фильтру группы; после `close()` клиента завершается исключением. */
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

    /** Клиент закончен: ожидания завершаются исключением. */
    fun failAll() {
        val error = IllegalStateException("MqttClient closed")
        for (waiter in waiters) waiter.result.completeExceptionally(error)
        waiters.clear()
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
