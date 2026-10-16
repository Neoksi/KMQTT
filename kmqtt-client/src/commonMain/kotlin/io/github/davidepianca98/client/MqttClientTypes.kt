package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import kotlin.time.Duration

// Публичные типы клиента по W6 §4.1. Объявлены в S06b-T03a (движок выдаёт их действиями), фасад S06b-T05a дополняет
// этот файл (ConnectResult и др.), а не объявляет их заново. `toString()` — без топиков и нагрузок (H11).

/** Состояние подключения клиента (для UI и журнала). */
public sealed interface ConnectionState {
    /** Клиент создан, но не запущен. */
    public data object Idle : ConnectionState

    /** Идёт подключение (TCP/TLS и ожидание CONNACK); [attempt] — номер попытки с 1 после сброса задержки. */
    public data class Connecting(val attempt: Int) : ConnectionState

    /** Сессия установлена (CONNACK с успехом); [epoch] — номер сессии, растёт в процессе. */
    public data class Connected(val epoch: Long, val sessionPresent: Boolean, val server: ServerInfo) : ConnectionState

    /** Подключение кончилось с [lastError]; следующая попытка номер [attempt] — через [delay]. */
    public data class Waiting(val attempt: Int, val delay: Duration, val lastError: MqttError) : ConnectionState

    /** Конечное: штатное закрытие (`error == null`) или исчерпаны попытки ([ReconnectPolicy.maxAttempts]). */
    public data class Closed(val error: MqttError?) : ConnectionState
}

/** Свойства брокера из CONNACK (в 3.1.1 — значения по умолчанию протокола). */
public data class ServerInfo(
    val maximumQos: Qos,
    val retainAvailable: Boolean,
    val receiveMaximum: Int,
    val maximumPacketSize: Int?,
    val serverKeepAliveSec: Int?,
)

/**
 * Входящие клиента строго по порядку получения вместе с границами сессий. Эпоха растёт глобально в процессе:
 * новое подключение — больше.
 */
public sealed interface MqttIncoming {
    public data class SessionStarted(val epoch: Long) : MqttIncoming

    public class Message(
        public val topic: String,
        public val payload: ByteArray,
        public val qos: Qos,
        public val retain: Boolean,
        public val epoch: Long,
    ) : MqttIncoming {
        override fun toString(): String = "Message(epoch=$epoch, qos=$qos, retain=$retain, size=${payload.size})"
    }

    /** Конец сессии [epoch]: после него сообщений этой эпохи нет. `error == null` — штатное закрытие. */
    public data class SessionEnded(val epoch: Long, val error: MqttError?) : MqttIncoming
}

/** До какой стадии ждать публикацию. */
public enum class PublishStage {
    /** Публикация отдана на запись в транспорт живой сессии. */
    Sent,

    /** PUBACK брокера (QoS 1; для QoS 2 — PUBCOMP). */
    Delivered,
}

/** Исход публикации — ровно один на публикацию. */
public sealed interface PublishResult {
    public data class Sent(val epoch: Long) : PublishResult

    public data class Delivered(val epoch: Long) : PublishResult

    /** Не дождалась соединения за TTL; брокеру не уходила. */
    public data object Expired : PublishResult

    public data class Failed(val reason: PublishFailure) : PublishResult
}

/** Почему публикация не прошла. */
public sealed interface PublishFailure {
    /** Записана, но подключение оборвалось до PUBACK (`until = Delivered`); после переподключения не досылается. */
    public data object ConnectionLost : PublishFailure

    /** Клиент закрыт раньше, чем публикация ушла (или до PUBACK при `until = Delivered`). */
    public data object Closed : PublishFailure

    /**
     * Отказ: клиента до отправки (QoS/retain/размер/топик против CONNACK — код, соответствующий причине) или брокера
     * в PUBACK MQTT 5 (код ≥ 0x80).
     */
    public data class Rejected(val code: ReasonCode?) : PublishFailure

    /** Оффлайн-очередь заполнена. */
    public data object QueueFull : PublishFailure
}
