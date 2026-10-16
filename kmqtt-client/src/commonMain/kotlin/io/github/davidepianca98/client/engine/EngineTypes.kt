package io.github.davidepianca98.client.engine

import io.github.davidepianca98.client.ConnectionState
import io.github.davidepianca98.client.DropReason
import io.github.davidepianca98.client.MqttError
import io.github.davidepianca98.client.MqttIncoming
import io.github.davidepianca98.client.PublishResult
import io.github.davidepianca98.client.PublishStage
import io.github.davidepianca98.client.ReconnectPolicy
import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.core.SessionSettings
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode

// Команды, действия и параметры движка (S06b-T03a). События транспорта — `TransportEvent` из MqttHost.kt (T04a).
// Ни один `toString()` не печатает адрес, топики, фильтры и нагрузки (H11).

/** Идентификатор публикации: по нему движок сообщает исход ([EngineAction.Complete]) и снимает её ([EngineCommand.Cancel]). */
@MqttCoreApi
public data class PublishTicket(val id: Long)

/**
 * Параметры движка.
 *
 * @param session параметры каждого подключения (ядро создаётся заново на каждое).
 * @param connectTimeoutMs срок TCP/TLS-подключения для [EngineAction.Connect] (исполняет хост).
 * @param offlineCapacity сколько публикаций ждёт соединения (и окна Receive Maximum); больше — `Failed(QueueFull)`.
 * @param subscribeBatch фильтров в одном SUBSCRIBE (порции, `MQTTConnection.SUBSCRIBE_CHUNK`).
 * @param consumerStallLimitMs пауза чтения ([EngineCommand.SetReadPaused]) дольше — подключение закрывается с
 *   [MqttError.ConsumerStalled] (W6 §4.5); `null` — без предела.
 */
@MqttCoreApi
public class EngineOptions(
    public val host: String,
    public val port: Int,
    public val session: SessionSettings,
    public val connectTimeoutMs: Long = 30_000,
    public val reconnect: ReconnectPolicy = ReconnectPolicy(),
    public val offlineCapacity: Int = 64,
    public val subscribeBatch: Int = 50,
    public val consumerStallLimitMs: Long? = 60_000,
) {
    init {
        require(port in 1..65_535) { "port out of 1..65535" }
        require(connectTimeoutMs > 0) { "connectTimeoutMs <= 0" }
        require(offlineCapacity >= 0) { "offlineCapacity < 0" }
        require(subscribeBatch >= 1) { "subscribeBatch < 1" }
        require(consumerStallLimitMs == null || consumerStallLimitMs > 0) { "consumerStallLimitMs <= 0" }
    }

    override fun toString(): String =
        "EngineOptions(session=$session, connectTimeoutMs=$connectTimeoutMs, reconnect=$reconnect, " +
            "offlineCapacity=$offlineCapacity, subscribeBatch=$subscribeBatch, consumerStallLimitMs=$consumerStallLimitMs)"
}

/** Команда API для движка. Исполняются строго по порядку поступления (одна FIFO, H2). */
@MqttCoreApi
public sealed interface EngineCommand {
    /** Начать подключение (идемпотентно; после закрытия — ничего). */
    public data object Start : EngineCommand

    /**
     * Публикация. Без живой сессии ждёт в оффлайн-очереди не дольше [ttlMs] (затем `Expired`). Исход —
     * [EngineAction.Complete] с [ticket], ровно один (кроме снятой [Cancel]).
     */
    public class Publish(
        public val ticket: PublishTicket,
        public val topic: String,
        public val payload: ByteArray,
        public val qos: Qos,
        public val retain: Boolean,
        public val ttlMs: Long,
        public val until: PublishStage,
    ) : EngineCommand {
        override fun toString(): String =
            "Publish(ticket=${ticket.id}, qos=$qos, retain=$retain, size=${payload.size}, ttlMs=$ttlMs, until=$until)"
    }

    /** Снять публикацию: из оффлайн-очереди — без исхода; уже записанная — исход больше не выдаётся. */
    public data class Cancel(val ticket: PublishTicket) : EngineCommand

    /** Набор фильтров группы (пустой — снять группу). Желаемое = объединение групп. */
    public class SetGroup(public val group: String, public val filters: Set<String>, public val qos: Qos) : EngineCommand {
        override fun toString(): String = "SetGroup(filters=${filters.size}, qos=$qos)"
    }

    /** Повторный SUBSCRIBE подписанных фильтров группы [group] (`null` — всех): брокер заново пришлёт retained. */
    public class Resubscribe(public val group: String?) : EngineCommand {
        override fun toString(): String = "Resubscribe(all=${group == null})"
    }

    /**
     * Обратное давление (W6 §4.5): потребитель входящих не успевает (`true`) или освободился (`false`). Движок
     * передаёт паузу ядру и хосту ([EngineAction.PauseRead]) и повторяет её после каждого нового подключения.
     */
    public data class SetReadPaused(val paused: Boolean) : EngineCommand

    /**
     * Штатное закрытие (H1, H2): после всех команд, поставленных раньше, — DISCONNECT с [reason] (MQTT 5), ожидание
     * EOF брокера (не дольше `closeLingerMs`), закрытие транспорта, `Closed(null)`.
     */
    public data class Close(val reason: ReasonCode = ReasonCode.SUCCESS) : EngineCommand
}

/** Действие движка. Исполняются по порядку; I/O-часть фасад переводит в `HostAction`. */
@MqttCoreApi
public sealed interface EngineAction {
    /** Подключить транспорт (прежний закрывается). Результат — `TransportEvent.Connected`/`Failed`. */
    public class Connect(public val host: String, public val port: Int, public val timeoutMs: Long) : EngineAction {
        override fun toString(): String = "Connect(timeoutMs=$timeoutMs)"
    }

    /** Записать байты в текущий транспорт. */
    public class Write(public val bytes: ByteArray) : EngineAction {
        override fun toString(): String = "Write(${bytes.size})"
    }

    /** Закрыть текущий транспорт сразу. */
    public data object CloseTransport : EngineAction

    /** Приостановить/возобновить чтение текущего транспорта. */
    public data class PauseRead(val paused: Boolean) : EngineAction

    /** Выдать потребителю входящих (по порядку). */
    public data class Emit(val incoming: MqttIncoming) : EngineAction

    /** Новое состояние подключения. */
    public data class State(val state: ConnectionState) : EngineAction

    /** Исход публикации [ticket]. */
    public data class Complete(val ticket: PublishTicket, val result: PublishResult) : EngineAction

    /** Событие для `MqttLogger` (без топиков, нагрузок, адресов). */
    public data class Log(val event: EngineLogEvent) : EngineAction
}

/** События журнала движка (W6 §4.8): только числа и коды. */
@MqttCoreApi
public sealed interface EngineLogEvent {
    public data class Connected(val epoch: Long) : EngineLogEvent

    public data class Disconnected(val error: MqttError?) : EngineLogEvent

    public data class ReconnectIn(val delayMs: Long) : EngineLogEvent

    /** Входящий пакет отброшен без разрыва (H9). */
    public data class Dropped(val reason: DropReason) : EngineLogEvent

    /** SUBSCRIBE отправлен: [count] фильтров. */
    public data class Subscribe(val count: Int) : EngineLogEvent

    /** SUBACK с отказом: [count] фильтров. */
    public data class Refused(val count: Int) : EngineLogEvent

    /** PUBACK MQTT 5 с отказом. */
    public data class PublishRefused(val code: ReasonCode) : EngineLogEvent
}
