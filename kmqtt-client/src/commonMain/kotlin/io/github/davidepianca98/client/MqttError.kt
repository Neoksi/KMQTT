package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode

/**
 * Причина конца подключения нового клиента.
 *
 * Безопасность (H11): ни поля, ни `toString()` не содержат адресов, топиков, нагрузок и паролей. У [Io] причина
 * печатается только именем класса исключения: текст исключения ввода-вывода часто содержит `host:port`.
 */
public sealed interface MqttError {
    /** CONNACK с кодом, отличным от успеха (3.1.1 — код возврата, переведённый в [ReasonCode]). */
    public data class Refused(val code: ReasonCode) : MqttError

    /** DISCONNECT от брокера (любой код, в т. ч. `SERVER_MOVED`/`USE_ANOTHER_SERVER` — без перехода, H10). */
    public data class ServerDisconnect(val code: ReasonCode) : MqttError

    /** Нет ответа на PINGREQ за отведённое время. */
    public data object KeepAliveTimeout : MqttError

    /** Нет CONNACK за `connackTimeout`. */
    public data object ConnackTimeout : MqttError

    /** TCP/TLS-подключение не установлено за `connectTimeout`. */
    public data object ConnectTimeout : MqttError

    /** Имя брокера не разрешилось. */
    public data object UnknownHost : MqttError

    /** Нарушение протокола или некорректный пакет со стороны брокера. */
    public data class Protocol(val code: ReasonCode) : MqttError

    /**
     * Ошибка ввода-вывода. [kind] — короткая метка без адресов (например, `"read"`, `"write"`, `"connect"`);
     * [cause] — исходное исключение для отладки, в `toString()` попадает только имя его класса.
     */
    public data class Io(val kind: String, val cause: Throwable?) : MqttError {
        override fun toString(): String = "Io(kind=$kind, cause=${cause?.let { it::class.simpleName }})"
    }

    /** Потребитель входящих не забирал сообщения дольше `consumerStallLimit` (обратное давление, W6 §4.5). */
    public data object ConsumerStalled : MqttError
}

/**
 * Почему входящий пакет отброшен без разрыва сессии (H9). Только числа: ни топика, ни нагрузки.
 */
public sealed interface DropReason {
    /** Пакет целиком ([size] байт с фиксированным заголовком) больше лимита [limit]; тело пропущено потоково. */
    public data class TooLarge(val size: Int, val limit: Int) : DropReason

    /** Топик PUBLISH длиной [length] байт UTF-8 длиннее лимита [limit]. */
    public data class TopicTooLong(val length: Int, val limit: Int) : DropReason
}
