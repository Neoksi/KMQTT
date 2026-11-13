package io.github.davidepianca98.client.internal

import kotlinx.atomicfu.atomic
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel

/**
 * Ворота входящих (): автомат «канал ↔ удержанные ↔ пауза
 * чтения ↔ снятие паузы», вынесенный из [MqttClientImpl] без изменения поведения.
 *
 * Потоки: [offer], [drain], [close] — только владелец (поток хоста; после его остановки — тот, кто держит замок шага
 * фасада); [consume]/[tryTake] — сборщик; [paused] — атомик, общий для обоих; [closeChannel] — любой поток.
 *
 * Состояния владельца:
 * - **Открыто** (`pauseRequested = false`, удержанных нет): [offer] кладёт прямо в канал.
 * - **Пауза** (`pauseRequested = true`): канал был полон; удержанные идут по порядку за каналом, [paused] = `true`,
 *   и каждый взятый сборщиком элемент будит владельца ([wake]). Удержанных может не остаться (повтор) — тогда
 *   ближайший [drain] снимает паузу.
 * - **Закрыто**: канал закрыт, удержанные переданы хвостом, который [consume] отдаёт после канала.
 *
 * Переходы: Открыто → Пауза в [offer] — флаг, затем [onPause], затем **повтор** переноса удержанных: сборщик мог
 * опустошить канал между неудачным `trySend` и флагом и уснуть без [wake] («спящий парикмахер»); после флага
 * любое взятие будит владельца. Пауза → Открыто в [drain], когда удержанных не осталось. Любое → Закрыто в [close].
 *
 * Память: канал — `capacity` элементов; удержанные растут только в шаге, поставившем паузу (дальше хост не читает
 * сокет, пока пауза не снята), — см. KDoc `MqttLimits.incomingBuffer`.
 *
 * Наблюдаемость: [depth] — сколько элементов в канале и удержано, считает атомарный счётчик (владелец
 * прибавляет в [offer], сборщик вычитает при взятии). Уровень глубины проверяет только владелец ([checkLevel], в конце
 * [offer] и в начале шага): вверх — по порогам 50, 75 и 100 % ёмкости, вниз — одним сообщением, когда глубина опала до
 * 25 % ёмкости и ниже; вызов [onDepth] делается на потоке владельца, поэтому сборщик журнал не трогает.
 *
 * @param beforePauseFlag тестовый хук: вызывается между неудачным `trySend` и установкой флага паузы.
 * @param onDepth уровень глубины сменился (пересечён порог или буфер опустел до 25 %); аргумент — текущая глубина.
 */
internal class IncomingGate<T : Any>(
    private val capacity: Int,
    private val wake: () -> Unit,
    private val onPause: () -> Unit,
    private val beforePauseFlag: () -> Unit = {},
    private val onDepth: (depth: Int) -> Unit = {},
) {
    private val channel = Channel<T>(capacity)
    private val depthCounter = atomic(0)
    private val pausedFlag = atomic(false)
    private val tail = atomic<List<T>?>(null)

    // ---- только владелец ----
    private val held = ArrayDeque<T>()
    private var pauseRequested = false
    private var closed = false
    private var level = 0 // 0 — ниже первого порога; 1..3 — пересечены пороги [UP_PERCENT]

    /** Сколько элементов ждёт сборщика: в канале и удержано сверх него (сборщик увидит приблизительное значение). */
    val depth: Int get() = depthCounter.value

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
        depthCounter.incrementAndGet()
        place(item)
        checkLevel()
    }

    /**
     * Проверить уровень глубины и сообщить [onDepth], если он сменился. Владелец: в конце [offer] и в начале шага (вниз
     * глубина падает на потоке сборщика, владелец узнаёт об этом на ближайшем шаге).
     */
    fun checkLevel() {
        val depth = depthCounter.value
        var next = level
        while (next < UP_PERCENT.size && depth.toLong() * 100 >= capacity.toLong() * UP_PERCENT[next]) next++
        if (next == level && level > 0 && depth.toLong() * 100 <= capacity.toLong() * DRAINED_PERCENT) next = 0
        if (next == level) return
        level = next
        onDepth(depth)
    }

    private fun place(item: T) {
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

    /** Закрыть: удержанные — хвостом после канала. Повторный вызов ничего не делает. Владелец. */
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
            depthCounter.decrementAndGet()
            if (pausedFlag.value) wake()
            sink(item)
        }
        tail.value?.forEach { sink(it) }
    }

    /** Сборщик без приостановки (тесты): взять один элемент канала, с тем же правилом пробуждения, что [consume]. */
    fun tryTake(): T? {
        val item = channel.tryReceive().getOrNull() ?: return null
        depthCounter.decrementAndGet()
        if (pausedFlag.value) wake()
        return item
    }

    private fun moveHeld() {
        while (held.isNotEmpty() && channel.trySend(held.first()).isSuccess) held.removeFirst()
    }

    private companion object {
        /** Пороги глубины вверх, % ёмкости. */
        val UP_PERCENT = intArrayOf(50, 75, 100)

        /** Глубина, при которой (и ниже) сообщается, что буфер опал, % ёмкости. */
        const val DRAINED_PERCENT = 25
    }
}
