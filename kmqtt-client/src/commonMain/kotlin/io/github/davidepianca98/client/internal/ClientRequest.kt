package io.github.davidepianca98.client.internal

import io.github.davidepianca98.client.ConnectResult
import io.github.davidepianca98.client.MqttClientOptions
import io.github.davidepianca98.client.PublishResult
import io.github.davidepianca98.client.SubscribeRefused
import io.github.davidepianca98.client.TopicState
import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.core.SessionAuth
import io.github.davidepianca98.client.core.SessionCredentials
import io.github.davidepianca98.client.core.SessionSettings
import io.github.davidepianca98.client.core.WillSpec
import io.github.davidepianca98.client.engine.EngineCommand
import io.github.davidepianca98.client.engine.EngineOptions
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos
import kotlinx.coroutines.CompletableDeferred

// Вынесено из MqttClientImpl.kt (: файл фасада не раздувать).

/** Запрос API в очередь клиента; исполняется на потоке хоста строго по порядку (FIFO). */
@OptIn(MqttCoreApi::class)
internal sealed interface ClientRequest {
    class Command(val command: EngineCommand) : ClientRequest
    class Publish(val command: EngineCommand.Publish, val result: CompletableDeferred<PublishResult>) : ClientRequest
    class AwaitConnect(val result: CompletableDeferred<ConnectResult>) : ClientRequest
    class AwaitApplied(val group: String, val result: CompletableDeferred<Map<String, TopicState>>) : ClientRequest
    class TemporaryStart(
        val group: String,
        val filters: Set<String>,
        val qos: Qos,
        val refused: CompletableDeferred<SubscribeRefused>,
    ) : ClientRequest
    class TemporaryEnd(val group: String, val qos: Qos) : ClientRequest
    class Reauthenticate(val result: CompletableDeferred<Boolean>) : ClientRequest
    object Close : ClientRequest
}

/**
 * Параметры клиента → параметры движка (сессия, подключение, переподключение, пределы). Окно продолжения сессии
 * ([MqttClientOptions.sessionResumeWindow]) — только при `persistentSession`; параметры подключения «с
 * чистого листа» строятся тем же [sessionSettings], отличаются только Clean Start.
 */
@OptIn(MqttCoreApi::class)
internal fun MqttClientOptions.toEngineOptions(): EngineOptions {
    val resumeWindowMs = if (persistentSession) sessionResumeWindow?.inWholeMilliseconds else null
    return EngineOptions(
        host = host,
        port = port,
        session = sessionSettings(cleanStart = !persistentSession),
        connectTimeoutMs = connectTimeout.inWholeMilliseconds,
        reconnect = reconnect,
        offlineCapacity = offlineQueue.capacity,
        subscribeBatch = limits.subscribeBatch,
        consumerStallLimitMs = limits.consumerStallLimit.inWholeMilliseconds,
        maxFilters = limits.maxFilters,
        maxUnacknowledged = limits.maxUnacknowledged,
        resumeWindowMs = resumeWindowMs,
        cleanSession = if (resumeWindowMs != null) sessionSettings(cleanStart = true) else null,
        closeDrainMs = closeDrain.inWholeMilliseconds,
    )
}

@OptIn(MqttCoreApi::class)
private fun MqttClientOptions.sessionSettings(cleanStart: Boolean): SessionSettings = SessionSettings(
    version = version,
    clientId = clientId,
    credentials = credentials?.let { SessionCredentials(it.username, it.password) },
    keepAliveSec = keepAlive.inWholeSeconds.toInt(),
    will = will?.let { WillSpec(it.topic, it.payload, it.retain, it.qos) },
    connackTimeoutMs = connackTimeout.inWholeMilliseconds,
    closeLingerMs = closeLinger.inWholeMilliseconds,
    maxIncomingPacket = limits.maxIncomingPacket,
    maxTopicLength = limits.maxTopicLength,
    cleanStart = cleanStart,
    sessionExpiryIntervalSec = if (persistentSession && version == MQTTVersion.MQTT5) sessionExpirySeconds() else null,
    auth = enhancedAuth?.let { SessionAuth(it.method, verifyConnack = it.verifyConnack, initialData = it.initialData) },
)

/** Session Expiry Interval в секундах: `INFINITE` и всё больше предела MQTT 5 — «не истекает». */
@OptIn(MqttCoreApi::class)
private fun MqttClientOptions.sessionExpirySeconds(): Long = if (sessionExpiry.isInfinite()) {
    SessionSettings.MAX_SESSION_EXPIRY
} else {
    sessionExpiry.inWholeSeconds.coerceAtMost(SessionSettings.MAX_SESSION_EXPIRY)
}
