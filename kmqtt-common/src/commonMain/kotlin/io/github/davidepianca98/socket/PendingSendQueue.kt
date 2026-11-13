package io.github.davidepianca98.socket

/**
 * Очередь исходящих данных с сохранением порядка для native-сокетов (posix `Socket`, `TLSSocket`).
 *
 * Правило: новые данные никогда не уходят «в обход» накопленных. [submit] ставит данные в хвост очереди и пытается
 * отправить очередь с головы; если голова не ушла целиком, остаток остаётся головой, а всё, что за ней, ждёт.
 * Данные не теряются и не дублируются: при частичной отправке в очереди остаётся только неотправленный остаток.
 *
 * Отправка одного куска — функция `attempt`: возвращает число принятых байтов (`0` — сейчас не принимается, например
 * `EAGAIN`; `size` и больше — кусок ушёл целиком) и бросает исключение при настоящей ошибке (очередь при этом не
 * меняется: бросивший кусок остаётся в голове, закрытие соединения — дело вызывающего).
 *
 * Повторный вход (`attempt` сам зовёт [submit]/[drain], как TLS-запись, читающая по `WANT_READ`) безопасен: вложенный
 * вызов только ставит данные в хвост, их отправит внешний цикл.
 *
 * Не потокобезопасна (как и сами сокеты, которыми владеет один поток).
 */
internal class PendingSendQueue {

    private val queue = ArrayDeque<UByteArray>()
    private var draining = false

    /** Очередь пуста: всё, что было передано в [submit], ушло. */
    fun isEmpty(): Boolean = queue.isEmpty()

    /** Сколько кусков ждёт (для тестов и диагностики). */
    val size: Int get() = queue.size

    /**
     * Поставить [data] в хвост и отправить очередь с головы. Возвращает `true`, если очередь опустела, и `false`, если
     * что-то осталось ждать (вызывающему нужно запросить событие «можно писать»).
     */
    fun submit(data: UByteArray, attempt: (UByteArray) -> Int): Boolean {
        val before = queue.size
        queue.addLast(data)
        val done = drain(attempt)
        // Хвост — data либо её остаток; если он не копия (ничего не ушло), это ссылка на буфер вызывающего: владеем копией
        if (queue.size == before + 1) queue[queue.lastIndex] = queue.last().copyOf()
        return done
    }

    /** Отправить очередь с головы, пока принимается. `true` — очередь пуста. */
    fun drain(attempt: (UByteArray) -> Int): Boolean {
        if (draining) return queue.isEmpty()
        draining = true
        try {
            while (queue.isNotEmpty()) {
                val head = queue.first()
                val sent = attempt(head)
                when {
                    sent >= head.size -> queue.removeFirst()
                    sent > 0 -> {
                        queue[0] = head.copyOfRange(sent, head.size)
                        return false
                    }
                    else -> return false
                }
            }
            return true
        } finally {
            draining = false
        }
    }
}
