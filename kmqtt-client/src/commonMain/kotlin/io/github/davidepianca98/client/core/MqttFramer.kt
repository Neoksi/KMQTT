package io.github.davidepianca98.client.core

import io.github.davidepianca98.client.DropReason
import io.github.davidepianca98.mqtt.MQTTException
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.MQTTControlPacketType
import io.github.davidepianca98.mqtt.packets.MQTTPacket
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Connack
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Disconnect
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pingresp
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Puback
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pubcomp
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pubrec
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pubrel
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Suback
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Unsuback
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Auth
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Connack
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Disconnect
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pingresp
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Puback
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubcomp
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubrec
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubrel
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Suback
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Unsuback
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode

/** Результат разбора входящего потока байтов. */
@MqttCoreApi
public sealed interface FramerOutput {
    /** Любой пакет, кроме PUBLISH, разобранный десериализаторами `kmqtt-common`. */
    public class Packet(public val packet: MQTTPacket) : FramerOutput

    /** Входящий PUBLISH, разобранный собственным лёгким декодером. */
    public class Publish(public val publish: IncomingPublish) : FramerOutput

    /**
     * Пакет отброшен без разрыва сессии. Для PUBLISH с QoS ≥ 1 заполнены [qos] и [packetId] (если они успели
     * прийти в начале тела): ядро должно подтвердить такой пакет (PUBACK/PUBREC), иначе окно неподтверждённых
     * у брокера (receive maximum / `max_inflight_messages`) со временем закроется.
     */
    public class Dropped(
        public val reason: DropReason,
        public val packetId: Int? = null,
        public val qos: Qos? = null,
    ) : FramerOutput {
        override fun toString(): String = "Dropped(reason=$reason, packetId=$packetId, qos=$qos)"
    }
}

/**
 * Входящая публикация. [topic] пуст, если брокер прислал только [topicAlias] (MQTT 5) — сопоставление алиаса
 * с топиком делает ядро. `toString()` не печатает топик и нагрузку (H11).
 */
@MqttCoreApi
public class IncomingPublish(
    public val topic: String,
    public val packetId: Int?,
    public val qos: Qos,
    public val retain: Boolean,
    public val dup: Boolean,
    public val topicAlias: Int?,
    public val payload: ByteArray,
) {
    override fun toString(): String =
        "IncomingPublish(qos=$qos, packetId=$packetId, retain=$retain, dup=$dup, alias=$topicAlias, size=${payload.size})"
}

/**
 * Фрейминг входящего потока MQTT-клиента: байты из сокета → пакеты.
 *
 * - Собственный сжимаемый буфер: начальный размер [initialBuffer], рост только до `maxIncomingPacket + 5`;
 *   буфер сбрасывается в начало, как только прочитано всё, и сдвигается (компактируется), когда кончилось место
 *   и в конце каждого [feed]. Вход [feed] копируется в буфер порциями, а не целиком, так что длинная пачка байтов
 *   не раздувает буфер.
 * - Пакет больше [maxIncomingPacket] (весь, с фиксированным заголовком) не буферизуется: тело пропускается потоково,
 *   по окончании пропуска выдаётся [FramerOutput.Dropped] с [DropReason.TooLarge], следующий пакет разбирается штатно.
 * - PUBLISH с топиком длиннее [maxTopicLength] (в байтах UTF-8) → [FramerOutput.Dropped] с [DropReason.TopicTooLong].
 * - Нарушение протокола (некорректная длина, неизвестный или недопустимый для клиента тип, битый пакет) —
 *   [MQTTException] с `MALFORMED_PACKET`/`PROTOCOL_ERROR`/`TOPIC_NAME_INVALID`; после него экземпляр не используется.
 *
 * Однопоточный: вызывается только с потока цикла (как и ядро). Один экземпляр — одно подключение.
 */
@MqttCoreApi
public class MqttFramer(
    private val version: MQTTVersion,
    private val maxIncomingPacket: Int,
    private val maxTopicLength: Int,
    initialBuffer: Int = 16 * 1024,
) {
    private val maxBuffer: Int

    private var buf: ByteArray
    private var readPos = 0
    private var writePos = 0

    // Потоковый пропуск слишком большого пакета.
    private var skipRemaining = 0L
    private var skipSize = 0
    private var skipIsPublish = false
    private var skipQos: Qos? = null
    private var skipBodyPos = 0L
    private var skipCapture = false
    private var skipTopicLength = 0
    private var skipPacketId = 0
    private var skipPacketIdKnown = false

    init {
        require(maxIncomingPacket >= 2) { "maxIncomingPacket < 2" }
        require(maxTopicLength >= 1) { "maxTopicLength < 1" }
        require(initialBuffer >= 1) { "initialBuffer < 1" }
        maxBuffer = if (maxIncomingPacket > Int.MAX_VALUE - HEADER_MAX) Int.MAX_VALUE else maxIncomingPacket + HEADER_MAX
        buf = ByteArray(initialBuffer.coerceIn(HEADER_MAX, maxBuffer))
    }

    /** Текущая ёмкость буфера (для тестов: не больше `maxIncomingPacket + 5`). */
    internal val capacity: Int get() = buf.size

    /** Скармливает очередную порцию байтов; возвращает всё, что из неё (и ранее накопленного) удалось разобрать. */
    public fun feed(bytes: ByteArray, offset: Int, length: Int): List<FramerOutput> {
        require(offset >= 0 && length >= 0 && offset + length <= bytes.size) { "bad range" }
        val out = ArrayList<FramerOutput>(4)
        var off = offset
        val end = offset + length
        while (true) {
            if (skipRemaining > 0) {
                val n = minOf(skipRemaining, (end - off).toLong()).toInt()
                observeSkipped(bytes, off, n)
                off += n
                skipRemaining -= n
                if (skipRemaining == 0L) out += finishSkip()
                if (off == end) break
                continue
            }
            parseBuffered(out)
            if (skipRemaining > 0) continue
            if (off == end) break
            if (writePos == buf.size) makeRoom()
            val n = minOf(end - off, buf.size - writePos)
            bytes.copyInto(buf, writePos, off, off + n)
            writePos += n
            off += n
        }
        if (readPos > 0) compact()
        return out
    }

    /** Разбирает все полные пакеты из буфера; при слишком большом пакете переводит фреймер в режим пропуска. */
    private fun parseBuffered(out: MutableList<FramerOutput>) {
        while (true) {
            val avail = writePos - readPos
            if (avail < 1) return
            val b0 = buf[readPos].toInt() and 0xFF
            val type = checkType(b0 ushr 4)
            val flags = b0 and 0x0F

            var remaining = 0
            var multiplier = 1
            var i = 1
            while (true) {
                if (i >= avail) return // длина ещё не пришла целиком
                val b = buf[readPos + i].toInt() and 0xFF
                remaining += (b and 0x7F) * multiplier
                i++
                if (b and 0x80 == 0) break
                if (i == HEADER_MAX) throw MQTTException(ReasonCode.MALFORMED_PACKET)
                multiplier *= 128
            }
            val headerLength = i
            val total = headerLength.toLong() + remaining

            if (total > maxIncomingPacket) {
                startSkip(type, flags, total, remaining.toLong())
                val buffered = minOf((avail - headerLength).toLong(), remaining.toLong()).toInt()
                observeSkipped(buf, readPos + headerLength, buffered)
                skipRemaining -= buffered
                readPos += headerLength + buffered
                if (readPos == writePos) {
                    readPos = 0
                    writePos = 0
                }
                if (skipRemaining > 0) return
                out += finishSkip()
                continue
            }

            if (avail < total) {
                val needed = total.toInt()
                if (needed > buf.size) grow(needed)
                return
            }

            val start = readPos + headerLength
            val end = start + remaining
            out += decode(type, flags, start, end)
            readPos = end
            if (readPos == writePos) {
                readPos = 0
                writePos = 0
            }
        }
    }

    private fun checkType(value: Int): MQTTControlPacketType {
        val type = MQTTControlPacketType.valueOf(value) ?: throw MQTTException(ReasonCode.MALFORMED_PACKET)
        when (type) {
            MQTTControlPacketType.Reserved -> throw MQTTException(ReasonCode.MALFORMED_PACKET)
            // Пакеты «клиент → брокер»: от брокера — нарушение протокола.
            MQTTControlPacketType.CONNECT,
            MQTTControlPacketType.SUBSCRIBE,
            MQTTControlPacketType.UNSUBSCRIBE,
            MQTTControlPacketType.PINGREQ,
            -> throw MQTTException(ReasonCode.PROTOCOL_ERROR)
            MQTTControlPacketType.AUTH ->
                if (version != MQTTVersion.MQTT5) throw MQTTException(ReasonCode.PROTOCOL_ERROR)
            else -> Unit
        }
        return type
    }

    private fun decode(type: MQTTControlPacketType, flags: Int, start: Int, end: Int): FramerOutput {
        if (type == MQTTControlPacketType.PUBLISH) return decodePublish(flags, start, end)
        val data = buf.copyOfRange(start, end).asUByteArray()
        val packet = try {
            if (version == MQTTVersion.MQTT5) decode5(type, flags, data) else decode4(type, flags, data)
        } catch (e: MQTTException) {
            throw e
        } catch (e: Exception) {
            throw MQTTException(ReasonCode.MALFORMED_PACKET)
        }
        return FramerOutput.Packet(packet)
    }

    private fun decode4(type: MQTTControlPacketType, flags: Int, data: UByteArray): MQTTPacket = when (type) {
        MQTTControlPacketType.CONNACK -> MQTT4Connack.fromByteArray(flags, data)
        MQTTControlPacketType.PUBACK -> MQTT4Puback.fromByteArray(flags, data)
        MQTTControlPacketType.PUBREC -> MQTT4Pubrec.fromByteArray(flags, data)
        MQTTControlPacketType.PUBREL -> MQTT4Pubrel.fromByteArray(flags, data)
        MQTTControlPacketType.PUBCOMP -> MQTT4Pubcomp.fromByteArray(flags, data)
        MQTTControlPacketType.SUBACK -> MQTT4Suback.fromByteArray(flags, data)
        MQTTControlPacketType.UNSUBACK -> MQTT4Unsuback.fromByteArray(flags, data)
        MQTTControlPacketType.PINGRESP -> MQTT4Pingresp.fromByteArray(flags, data)
        MQTTControlPacketType.DISCONNECT -> MQTT4Disconnect.fromByteArray(flags, data)
        else -> throw MQTTException(ReasonCode.PROTOCOL_ERROR)
    }

    private fun decode5(type: MQTTControlPacketType, flags: Int, data: UByteArray): MQTTPacket = when (type) {
        MQTTControlPacketType.CONNACK -> MQTT5Connack.fromByteArray(flags, data)
        MQTTControlPacketType.PUBACK -> MQTT5Puback.fromByteArray(flags, data)
        MQTTControlPacketType.PUBREC -> MQTT5Pubrec.fromByteArray(flags, data)
        MQTTControlPacketType.PUBREL -> MQTT5Pubrel.fromByteArray(flags, data)
        MQTTControlPacketType.PUBCOMP -> MQTT5Pubcomp.fromByteArray(flags, data)
        MQTTControlPacketType.SUBACK -> MQTT5Suback.fromByteArray(flags, data)
        MQTTControlPacketType.UNSUBACK -> MQTT5Unsuback.fromByteArray(flags, data)
        MQTTControlPacketType.PINGRESP -> MQTT5Pingresp.fromByteArray(flags, data)
        MQTTControlPacketType.DISCONNECT -> MQTT5Disconnect.fromByteArray(flags, data)
        MQTTControlPacketType.AUTH -> MQTT5Auth.fromByteArray(flags, data)
        else -> throw MQTTException(ReasonCode.PROTOCOL_ERROR)
    }

    private fun publishQos(flags: Int): Qos {
        val qos = Qos.valueOf((flags shr 1) and 0x3) ?: throw MQTTException(ReasonCode.MALFORMED_PACKET)
        if (qos == Qos.AT_MOST_ONCE && flags and 0x8 != 0) throw MQTTException(ReasonCode.MALFORMED_PACKET)
        return qos
    }

    /** Лёгкий декодер PUBLISH 3.1.1/5: топик сразу в `String`, нагрузка — одна копия, из свойств — только алиас. */
    private fun decodePublish(flags: Int, start: Int, end: Int): FramerOutput {
        val qos = publishQos(flags)
        val retain = flags and 0x1 != 0
        val dup = flags and 0x8 != 0
        var p = start

        if (end - p < 2) throw MQTTException(ReasonCode.MALFORMED_PACKET)
        val topicLength = u16(p)
        p += 2
        if (topicLength > maxTopicLength) {
            val idPos = p + topicLength
            val packetId = if (qos != Qos.AT_MOST_ONCE && idPos + 2 <= end) u16(idPos) else null
            return FramerOutput.Dropped(
                DropReason.TopicTooLong(topicLength, maxTopicLength),
                packetId,
                qos,
            )
        }
        if (end - p < topicLength) throw MQTTException(ReasonCode.MALFORMED_PACKET)
        val topic = try {
            buf.decodeToString(p, p + topicLength, throwOnInvalidSequence = true)
        } catch (e: Exception) {
            throw MQTTException(ReasonCode.MALFORMED_PACKET)
        }
        p += topicLength
        if (topic.indexOf('+') >= 0 || topic.indexOf('#') >= 0 || topic.indexOf('\u0000') >= 0) {
            throw MQTTException(ReasonCode.TOPIC_NAME_INVALID)
        }

        var packetId: Int? = null
        if (qos != Qos.AT_MOST_ONCE) {
            if (end - p < 2) throw MQTTException(ReasonCode.MALFORMED_PACKET)
            packetId = u16(p)
            p += 2
        }

        var topicAlias: Int? = null
        if (version == MQTTVersion.MQTT5) {
            val (propertiesLength, lengthBytes) = readVarInt(p, end)
            p += lengthBytes
            val propertiesEnd = p.toLong() + propertiesLength
            if (propertiesEnd > end) throw MQTTException(ReasonCode.MALFORMED_PACKET)
            val pe = propertiesEnd.toInt()
            while (p < pe) {
                val (id, idBytes) = readVarInt(p, pe)
                p += idBytes
                p = when (id) {
                    PROP_PAYLOAD_FORMAT -> need(p, 1, pe)
                    PROP_MESSAGE_EXPIRY -> need(p, 4, pe)
                    PROP_CONTENT_TYPE, PROP_RESPONSE_TOPIC, PROP_CORRELATION_DATA -> skipBinary(p, pe)
                    PROP_USER_PROPERTY -> skipBinary(skipBinary(p, pe), pe)
                    PROP_SUBSCRIPTION_ID -> p + readVarInt(p, pe).second
                    PROP_TOPIC_ALIAS -> {
                        if (topicAlias != null) throw MQTTException(ReasonCode.PROTOCOL_ERROR)
                        val next = need(p, 2, pe)
                        val alias = u16(p)
                        if (alias == 0) throw MQTTException(ReasonCode.PROTOCOL_ERROR)
                        topicAlias = alias
                        next
                    }
                    else -> throw MQTTException(ReasonCode.PROTOCOL_ERROR)
                }
            }
            if (topicLength == 0 && topicAlias == null) throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        } else if (topicLength == 0) {
            throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        }

        val payload = buf.copyOfRange(p, end)
        return FramerOutput.Publish(IncomingPublish(topic, packetId, qos, retain, dup, topicAlias, payload))
    }

    private fun u16(pos: Int): Int = ((buf[pos].toInt() and 0xFF) shl 8) or (buf[pos + 1].toInt() and 0xFF)

    private fun need(pos: Int, count: Int, limit: Int): Int {
        if (limit - pos < count) throw MQTTException(ReasonCode.MALFORMED_PACKET)
        return pos + count
    }

    /** Пропускает строку/двоичные данные с 2-байтной длиной; возвращает позицию после них. */
    private fun skipBinary(pos: Int, limit: Int): Int {
        need(pos, 2, limit)
        return need(pos + 2, u16(pos), limit)
    }

    /** Variable byte integer внутри уже полного пакета: (значение, число байтов). Неполный или > 4 байт — malformed. */
    private fun readVarInt(pos: Int, limit: Int): Pair<Int, Int> {
        var value = 0
        var multiplier = 1
        var i = 0
        while (true) {
            if (pos + i >= limit || i == 4) throw MQTTException(ReasonCode.MALFORMED_PACKET)
            val b = buf[pos + i].toInt() and 0xFF
            value += (b and 0x7F) * multiplier
            i++
            if (b and 0x80 == 0) return value to i
            multiplier *= 128
        }
    }

    private fun startSkip(type: MQTTControlPacketType, flags: Int, total: Long, remaining: Long) {
        skipRemaining = remaining
        skipSize = if (total > Int.MAX_VALUE) Int.MAX_VALUE else total.toInt()
        skipIsPublish = type == MQTTControlPacketType.PUBLISH
        skipQos = if (skipIsPublish) publishQos(flags) else null
        skipBodyPos = 0
        skipCapture = skipIsPublish && skipQos != Qos.AT_MOST_ONCE
        skipTopicLength = 0
        skipPacketId = 0
        skipPacketIdKnown = false
    }

    /** Пропуск тела; для PUBLISH QoS ≥ 1 по пути выхватывает длину топика и packetId (первые байты тела). */
    private fun observeSkipped(src: ByteArray, off: Int, n: Int) {
        var i = 0
        while (i < n && skipCapture) {
            val pos = skipBodyPos
            val b = src[off + i].toInt() and 0xFF
            when (pos) {
                0L -> skipTopicLength = b shl 8
                1L -> skipTopicLength = skipTopicLength or b
                2L + skipTopicLength -> skipPacketId = b shl 8
                3L + skipTopicLength -> {
                    skipPacketId = skipPacketId or b
                    skipPacketIdKnown = true
                    skipCapture = false
                }
            }
            skipBodyPos++
            i++
            val topicEnd = 2L + skipTopicLength
            if (skipCapture && skipBodyPos >= 2 && skipBodyPos < topicEnd) {
                val jump = minOf(topicEnd - skipBodyPos, (n - i).toLong()).toInt()
                i += jump
                skipBodyPos += jump
            }
        }
        skipBodyPos += n - i
    }

    private fun finishSkip(): FramerOutput {
        val output = FramerOutput.Dropped(
            DropReason.TooLarge(skipSize, maxIncomingPacket),
            if (skipPacketIdKnown) skipPacketId else null,
            skipQos,
        )
        skipCapture = false
        skipIsPublish = false
        skipQos = null
        return output
    }

    /** Буфер заполнен до конца: сдвинуть непрочитанное в начало, а если начало и так свободно — вырасти. */
    private fun makeRoom() {
        if (readPos > 0) {
            compact()
        } else {
            grow(buf.size + 1)
        }
    }

    private fun grow(minCapacity: Int) {
        check(minCapacity <= maxBuffer) { "framer buffer limit" }
        var newSize = if (buf.size > maxBuffer / 2) maxBuffer else buf.size * 2
        if (newSize < minCapacity) newSize = minCapacity
        if (readPos > 0) compact()
        buf = buf.copyOf(newSize)
    }

    private fun compact() {
        val unread = writePos - readPos
        if (unread > 0) buf.copyInto(buf, 0, readPos, writePos)
        readPos = 0
        writePos = unread
    }

    private companion object {
        /** Байт типа + до 4 байтов длины. */
        const val HEADER_MAX = 5

        const val PROP_PAYLOAD_FORMAT = 0x01
        const val PROP_MESSAGE_EXPIRY = 0x02
        const val PROP_CONTENT_TYPE = 0x03
        const val PROP_RESPONSE_TOPIC = 0x08
        const val PROP_CORRELATION_DATA = 0x09
        const val PROP_SUBSCRIPTION_ID = 0x0B
        const val PROP_TOPIC_ALIAS = 0x23
        const val PROP_USER_PROPERTY = 0x26
    }
}
