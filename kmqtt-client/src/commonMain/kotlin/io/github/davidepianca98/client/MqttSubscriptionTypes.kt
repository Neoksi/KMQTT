package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode

// Публичные типы подписок клиента по W6 §4.1/§4.3 (S06b-T03b). `toString()` — без фильтров (H11).

/** Состояние желаемого фильтра в текущей эпохе. */
public sealed interface TopicState {
    /** Нет живой сессии или SUBSCRIBE фильтра отправлен и ждёт SUBACK. */
    public data object Pending : TopicState

    /** Брокер подписал с [qos] (SUBACK 0/1/2). */
    public data class Granted(val qos: Qos) : TopicState

    /** Брокер отказал (SUBACK ≥ 0x80, например 0x87 NOT_AUTHORIZED по ACL). Сессия продолжается (H7). */
    public data class Refused(val code: ReasonCode) : TopicState
}

/**
 * Отказ брокера в подписке на [filter] группы [group] (H7): сессия продолжается, остальные фильтры работают.
 * Фильтр, общий для нескольких групп, даёт событие на каждую группу (журнал считает различные фильтры).
 */
public class SubscribeRefused(public val group: String, public val filter: String, public val code: ReasonCode) {
    override fun equals(other: Any?): Boolean =
        other is SubscribeRefused && other.group == group && other.filter == filter && other.code == code

    override fun hashCode(): Int = (group.hashCode() * 31 + filter.hashCode()) * 31 + code.hashCode()

    override fun toString(): String = "SubscribeRefused(code=$code)"
}
