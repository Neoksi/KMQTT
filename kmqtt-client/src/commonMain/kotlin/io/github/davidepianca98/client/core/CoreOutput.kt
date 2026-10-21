package io.github.davidepianca98.client.core

import io.github.davidepianca98.client.DropReason
import io.github.davidepianca98.client.MqttError
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode

/**
 * События ядра [MqttSessionCore] для движка. `toString()` ни одного события не печатает топики и нагрузки (H11).
 */
@MqttCoreApi
public sealed interface CoreOutput {
    /** CONNACK с успехом: сессия установлена. */
    public data class Connected(public val connack: ConnackInfo) : CoreOutput

    /**
     * Входящая публикация (топик уже восстановлен по алиасу). Для QoS 1 PUBACK уже поставлен в исходящие — в момент
     * выдачи события («хотя бы раз до буфера клиента»).
     */
    public class Message(
        public val topic: String,
        public val payload: ByteArray,
        public val qos: Qos,
        public val retain: Boolean,
    ) : CoreOutput {
        override fun toString(): String = "Message(qos=$qos, retain=$retain, size=${payload.size})"
    }

    /**
     * Завершение исходящей публикации QoS ≥ 1: PUBACK (QoS 1), PUBCOMP (QoS 2) или PUBREC с кодом ≥ 0x80 (QoS 2,
     * брокер отказал — PUBREL не отправляется). [reason] — код MQTT 5 (в 3.1.1 всегда `SUCCESS`); ≥ 0x80 — отказ
     * (например, 0x87 NOT_AUTHORIZED вне ACL).
     */
    public data class PubAck(public val packetId: Int, public val reason: ReasonCode) : CoreOutput

    /**
     * SUBACK — всегда событием, отказ сессию не рвёт. [codes] — по порядку фильтров SUBSCRIBE; 3.1.1 переведён:
     * 0/1/2 → `SUCCESS`/`GRANTED_QOS1`/`GRANTED_QOS2`, 0x80 → `UNSPECIFIED_ERROR`.
     */
    public data class SubAck(public val packetId: Int, public val codes: List<ReasonCode>) : CoreOutput

    /** UNSUBACK. [codes] — коды MQTT 5 по порядку фильтров (в 3.1.1 список пуст). */
    public data class UnsubAck(public val packetId: Int, public val codes: List<ReasonCode>) : CoreOutput

    /**
     * Входящий пакет отброшен без разрыва сессии: больше лимита или топик длиннее лимита. Отброшенный PUBLISH
     * с QoS ≥ 1 ядро уже подтвердило (PUBACK/PUBREC), если фреймер успел прочитать его packetId.
     */
    public data class Dropped(public val reason: DropReason) : CoreOutput

    /**
     * Брокер продолжает расширенную аутентификацию (AUTH 0x18, MQTT 5 §4.12): [data] — его Authentication
     * Data (`null` — без неё), [method] — метод (совпадает с [SessionAuth.method]). Ответ — [MqttSessionCore.authResponse]
     * или отказ [MqttSessionCore.authFailed]; срок — тот же `connackTimeoutMs` от CONNECT. `toString()` без данных (H11).
     */
    public class AuthChallenge(public val method: String, public val data: ByteArray?) : CoreOutput {
        override fun toString(): String = "AuthChallenge(method=$method, size=${data?.size ?: -1})"
    }

    /**
     * Повторная аутентификация посреди сессии завершилась успехом (AUTH 0x00 брокера; обмен начат
     * [MqttSessionCore.reauthenticate]): его Authentication Data уже проверены [SessionAuth.verifyConnack], если та
     * задана. Сессия продолжается.
     */
    public data object Reauthenticated : CoreOutput

    /**
     * Конец подключения, выдаётся ровно один раз. `error == null` — штатное закрытие (`disconnect` + EOF или дедлайн
     * linger). После него в [MqttSessionCore.takeOutbound] может остаться последний пакет (DISCONNECT с кодом ошибки
     * в MQTT 5) — хост пишет его без ожидания и закрывает соединение.
     */
    public data class Closed(public val error: MqttError?) : CoreOutput
}

/** Свойства сервера из CONNACK (для 3.1.1 — значения по умолчанию протокола). */
@MqttCoreApi
public data class ConnackInfo(
    public val sessionPresent: Boolean,
    /** Server Keep Alive (MQTT 5); если задан, ядро применяет его вместо своего keepAlive. */
    public val serverKeepAliveSec: Int?,
    public val receiveMaximum: Int,
    public val maximumQos: Qos,
    public val retainAvailable: Boolean,
    public val maximumPacketSize: Int?,
    public val assignedClientId: String?,
    public val topicAliasMaximum: Int,
)

/** Состояние подключения в ядре. */
@MqttCoreApi
public enum class CoreState {
    /** CONNECT отправлен (или будет отправлен в [MqttSessionCore.start]), ждём CONNACK. */
    Connecting,

    /** CONNACK с успехом получен. */
    Connected,

    /** Отправлен DISCONNECT: входящее читается и отбрасывается до EOF или дедлайна linger. */
    Closing,

    /** Конец подключения; [CoreOutput.Closed] выдан. */
    Closed,
}

/** Исходящая публикация. `toString()` без топика и нагрузки. */
@MqttCoreApi
public class OutgoingPublish(
    public val topic: String,
    public val payload: ByteArray,
    public val qos: Qos = Qos.AT_MOST_ONCE,
    public val retain: Boolean = false,
) {
    override fun toString(): String = "OutgoingPublish(qos=$qos, retain=$retain, size=${payload.size})"
}

/** Фильтр подписки. `toString()` без фильтра (H11: топики вне логов). */
@MqttCoreApi
public data class SubscriptionRequest(
    public val filter: String,
    public val qos: Qos = Qos.AT_MOST_ONCE,
) {
    override fun toString(): String = "SubscriptionRequest(qos=$qos, length=${filter.length})"
}

/** Результат [MqttSessionCore.publish]. */
@MqttCoreApi
public sealed interface PublishAdmission {
    /** PUBLISH поставлен в исходящие; [packetId] — для QoS ≥ 1 (ждать [CoreOutput.PubAck]), null для QoS 0. */
    public data class Accepted(public val packetId: Int?) : PublishAdmission

    /** Публикация невозможна в этом подключении; брокеру ничего не ушло. */
    public data class Rejected(public val reason: PublishRejection) : PublishAdmission

    /**
     * Неподтверждённых QoS ≥ 1 столько, сколько разрешил брокер (Receive Maximum из CONNACK): движок держит
     * публикацию в очереди и повторяет после очередного [CoreOutput.PubAck]. Брокеру ничего не ушло.
     */
    public data object FlowControlled : PublishAdmission
}

/** Почему публикация отклонена ядром. */
@MqttCoreApi
public sealed interface PublishRejection {
    /** Подключение не в состоянии `Connected` (до CONNACK, при закрытии или после конца). */
    public data object NotConnected : PublishRejection

    /** QoS выше Maximum QoS брокера. */
    public data object QosNotSupported : PublishRejection

    /** Retain, а брокер его не поддерживает (Retain Available = 0). */
    public data object RetainNotSupported : PublishRejection

    /** Пакет [size] байт больше Maximum Packet Size брокера [limit]. */
    public data class TooLarge(public val size: Int, public val limit: Int) : PublishRejection

    /** Топик пуст, длиннее 65535 байт UTF-8 или содержит `+`, `#`, `\u0000`. */
    public data object InvalidTopic : PublishRejection
}
