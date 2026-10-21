package io.github.davidepianca98.client.internal

import io.github.davidepianca98.mqtt.packets.MQTTControlPacketType

/**
 * Границы MQTT-пакетов в потоке байтов по заголовку фиксированного формата (тип — старшие 4 бита первого байта, длина
 * остатка — varint из 1–4 байт) для `MqttLogger.packet`. Только счёт: тело не копируется и не
 * разбирается, пропускается арифметикой. Пакет сообщается, когда пришёл его последний байт (как у `MqttFramer`).
 *
 * Состояние переживает границы порций (заголовок и длина могут быть разрезаны); [reset] — на каждое новое подключение,
 * чтобы недочитанный пакет прошлого соединения не сдвинул счёт. Неверный заголовок (тип 0, пятый байт длины) —
 * остановка до [reset], ничего не сообщается: ядро в этот момент само рвёт сессию ошибкой протокола.
 *
 * Только поток хоста.
 */
internal class PacketScanner(private val emit: (MQTTControlPacketType, Int) -> Unit) {
    private var stage = HEADER
    private var type = MQTTControlPacketType.Reserved
    private var lengthBytes = 0
    private var multiplier = 1L
    private var remaining = 0L
    private var size = 0

    /** Последний поданный байт закончил пакет (или байтов не было). */
    val atBoundary: Boolean get() = stage == HEADER

    fun reset() {
        stage = HEADER
    }

    fun feed(bytes: ByteArray) {
        var i = 0
        val end = bytes.size
        while (i < end) {
            when (stage) {
                HEADER -> {
                    val value = (bytes[i].toInt() and 0xFF) ushr 4
                    i++
                    if (value == 0) {
                        stage = STOPPED
                        return
                    }
                    type = TYPES[value]
                    lengthBytes = 0
                    multiplier = 1
                    remaining = 0
                    stage = LENGTH
                }
                LENGTH -> {
                    val b = bytes[i].toInt() and 0xFF
                    i++
                    remaining += (b and 0x7F) * multiplier
                    lengthBytes++
                    if (b and 0x80 == 0) {
                        size = (1 + lengthBytes + remaining).toInt()
                        if (remaining == 0L) done() else stage = BODY
                    } else if (lengthBytes == MAX_LENGTH_BYTES) {
                        stage = STOPPED
                        return
                    } else {
                        multiplier *= 128
                    }
                }
                BODY -> {
                    val n = minOf(remaining, (end - i).toLong()).toInt()
                    i += n
                    remaining -= n
                    if (remaining == 0L) done()
                }
                else -> return
            }
        }
    }

    private fun done() {
        stage = HEADER
        emit(type, size)
    }

    private companion object {
        const val HEADER = 0
        const val LENGTH = 1
        const val BODY = 2
        const val STOPPED = 3
        const val MAX_LENGTH_BYTES = 4

        /** Индекс — значение типа (0..15), порядок перечисления совпадает со значениями. */
        val TYPES: Array<MQTTControlPacketType> =
            Array(16) { value -> MQTTControlPacketType.entries.first { it.value == value } }
    }
}
