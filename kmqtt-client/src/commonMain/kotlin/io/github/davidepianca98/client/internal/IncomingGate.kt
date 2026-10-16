package io.github.davidepianca98.client.internal

import kotlinx.atomicfu.atomic
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel

/**
 * Ворота входящих (SEVEN-260, W6 §4.5, разбор `W6-flow-vs-channel.md` §4.4): автомат «канал ↔ удержанные ↔ пауза
 * чтения ↔ снятие паузы», вынесенный из [MqttClientImpl] без изменения поведения.
 *
 * Потоки: [offer], [drain], [close] — только владелец (поток хоста; после его остановки — тот, кто держит замок шага
 * фасада); [consume]/[tryTake] — сборщик; [paused] — атомик, общий для обоих; [closeChannel] — любой поток.
 *
 * Состояния владельца:
 * - **Открыто** (`pauseRequested = false`, удержанных нет): [offer] кладёт прямо в канал.
 * - **Пауза** (`pauseRequested = true`): канал был полон; удержанные идут по порядку за каналом, [paused] = `true`,
 *   и каждый взятый сборщиком элемент будит владельца ([wake]). Удержанных может не остаться (повтор, Г4) — тогда
 *   ближайший [drain] снимает паузу.
 * - **Закрыто**: канал закрыт, удержанные переданы хвостом, который [consume] отдаёт после канала (D2).
 *
 * Переходы: Открыто → Пауза в [offer] — флаг, затем [onPause], затем **повтор** переноса удержанных: сборщик мог
 * опустошить канал между неудачным `trySend` и флагом и уснуть без [wake] («спящий парикмахер», D5); после флага
 * любое взятие будит владельца. Пауза → Открыто в [drain], когда удержанных не осталось. Любое → Закрыто в [close].
 *
 * Память: канал — `capacity` элементов; удержанные растут только в шаге, поставившем паузу (дальше хост не читает
 * сокет, пока пауза не снята), — см. KDoc `MqttLimits.incomingBuffer`.
 *
 * @param beforePauseFlag тестовый хук: вызывается между неудачным `trySend` и установкой флага паузы.
 */
internal class IncomingGate<T : Any>(
    capacity: Int,
    private val wake: () -> Unit,
    private val onPause: () -> Unit,
    private val beforePauseFlag: () -> Unit = {},
) {
    private val channel = Channel<T>(capacity)
    private val pausedFlag = atomic(false)
    private val tail = atomic<List<T>?>(null)

    // ---- только владелец ----
    private val held = ArrayDeque<T>()
    private var pauseRequested = false
    private var closed = false

    /** Флаг паузы, который видит сборщик. */
    val paused: Boolean get() = pausedFlag.value

    /** Пауза запрошена владельцем (снимается только в [drain]). Владелец. */
    val pausing: Boolean get() = pauseRequested

    /** Сколько удержано сверх канала. Владелец. */
    val heldCount: Int get() = held.size

    /** Канал пуст — сборщику нечего взять (тесты живости). */
    @OptIn(ExperimentalCoroutinesApi::class)
    val channelEmpty: Boolean get() = channel.isEmpty

    /** Отдать элемент; нет места — удержать по порядку и (впервые) поставить паузу. Владелец. */
    fun offer(item: T) {
        if (held.isEmpty() && channel.trySend(item).isSuccess) return
        held.addLast(item)
        if (pauseRequested) return
        pauseRequested = true
        beforePauseFlag()
        pausedFlag.value = true
        onPause()
        moveHeld()
    }

    /** Начало шага: переложить удержанные; `true` — пауза снята (удержанных нет). Владелец. */
    fun drain(): Boolean {
        moveHeld()
        if (held.isNotEmpty() || !pauseRequested) return false
        pauseRequested = false
        pausedFlag.value = false
        return true
    }

    /** Закрыть: удержанные — хвостом после канала (D2). Повторный вызов ничего не делает. Владелец. */
    fun close() {
        if (closed) return
        closed = true
        if (held.isNotEmpty()) tail.value = held.toList()
        held.clear()
        channel.close()
    }

    /**
     * Закрыть только канал — с любого потока (аварийный путь D6, владелец, возможно, ещё работает). Сборщик завершится
     * после канала; хвост, переданный позже [close], он может уже не увидеть.
     */
    fun closeChannel() {
        channel.close()
    }

    /** Сборщик: все элементы по порядку (канал, затем хвост), пока ворота не закрыты. */
    suspend fun consume(sink: suspend (T) -> Unit) {
        for (item in channel) {
            if (pausedFlag.value) wake()
            sink(item)
        }
        tail.value?.forEach { sink(it) }
    }

    /** Сборщик без приостановки (тесты): взять один элемент канала, с тем же правилом пробуждения, что [consume]. */
    fun tryTake(): T? {
        val item = channel.tryReceive().getOrNull() ?: return null
        if (pausedFlag.value) wake()
        return item
    }

    private fun moveHeld() {
        while (held.isNotEmpty() && channel.trySend(held.first()).isSuccess) held.removeFirst()
    }
}
