package io.github.davidepianca98.client.engine

import io.github.davidepianca98.client.core.MqttCoreApi

/**
 * Оффлайн-очередь движка: FIFO публикаций, ещё не отданных ядру (нет живой сессии или окно Receive Maximum брокера
 * занято), каждая со своим дедлайном `nowMs + ttlMs` (W6 §4.4). Заменяет очередь команд `MQTTConnection` с
 * `launch { delay(ttl) }` на команду и атомарным `claim()`: владелец один (движок), исход у публикации ровно один.
 *
 * Однопоточная, без часов: время — параметром. Порядок публикаций сохраняется (дедлайн на порядок не влияет).
 */
@OptIn(MqttCoreApi::class)
internal class OfflineQueue(private val capacity: Int) {

    class Entry(val command: EngineCommand.Publish, val deadlineMs: Long)

    private val items = ArrayDeque<Entry>()

    val size: Int get() = items.size

    fun isEmpty(): Boolean = items.isEmpty()

    /** В конец очереди; `false` — очередь полна (публикация не принята, исход — `QueueFull`). */
    fun add(command: EngineCommand.Publish, nowMs: Long): Boolean {
        if (items.size >= capacity) return false
        items.addLast(Entry(command, deadlineOf(command, nowMs)))
        return true
    }

    fun peek(): Entry? = items.firstOrNull()

    fun removeFirst(): Entry = items.removeFirst()

    /** Снять публикацию по билету (отмена); `true` — была в очереди. */
    fun remove(ticket: PublishTicket): Boolean {
        val index = items.indexOfFirst { it.command.ticket == ticket }
        if (index < 0) return false
        items.removeAt(index)
        return true
    }

    /** Снять все публикации с истёкшим дедлайном (`deadline ≤ nowMs`), по порядку. */
    fun expire(nowMs: Long): List<EngineCommand.Publish> {
        if (items.none { it.deadlineMs <= nowMs }) return emptyList()
        val expired = ArrayList<EngineCommand.Publish>()
        val iterator = items.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (entry.deadlineMs <= nowMs) {
                expired += entry.command
                iterator.remove()
            }
        }
        return expired
    }

    /** Ближайший дедлайн или `null`. */
    fun nextDeadline(): Long? = items.minOfOrNull { it.deadlineMs }

    /** Снять всё (закрытие клиента). */
    fun drain(): List<EngineCommand.Publish> = items.map { it.command }.also { items.clear() }

    companion object {
        /** Дедлайн TTL публикации, принятой в [nowMs] (без переполнения). */
        fun deadlineOf(command: EngineCommand.Publish, nowMs: Long): Long {
            val ttl = command.ttlMs.coerceAtLeast(0)
            return if (ttl > Long.MAX_VALUE - nowMs) Long.MAX_VALUE else nowMs + ttl
        }
    }
}
