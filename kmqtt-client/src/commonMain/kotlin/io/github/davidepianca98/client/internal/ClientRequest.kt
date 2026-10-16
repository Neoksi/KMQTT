package io.github.davidepianca98.client.internal

import io.github.davidepianca98.client.ConnectResult
import io.github.davidepianca98.client.MqttClientOptions
import io.github.davidepianca98.client.PublishResult
import io.github.davidepianca98.client.SubscribeRefused
import io.github.davidepianca98.client.TopicState
import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.core.SessionCredentials
import io.github.davidepianca98.client.core.SessionSettings
import io.github.davidepianca98.client.core.WillSpec
import io.github.davidepianca98.client.engine.EngineCommand
import io.github.davidepianca98.client.engine.EngineOptions
import io.github.davidepianca98.mqtt.packets.Qos
import kotlinx.coroutines.CompletableDeferred

// Вынесено из MqttClientImpl.kt (S06b-T05b, решение оркестратора: файл фасада не раздувать).

/** Запрос API в очередь клиента; исполняется на потоке хоста строго по порядку (FIFO, H2). */
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
    object Close : ClientRequest
}

/** Параметры клиента → параметры движка (сессия, подключение, переподключение, пределы). */
@OptIn(MqttCoreApi::class)
internal fun MqttClientOptions.toEngineOptions(): EngineOptions = EngineOptions(
    host = host,
    port = port,
    session = SessionSettings(
        version = version,
        clientId = clientId,
        credentials = credentials?.let { SessionCredentials(it.username, it.password) },
        keepAliveSec = keepAlive.inWholeSeconds.toInt(),
        will = will?.let { WillSpec(it.topic, it.payload, it.retain, it.qos) },
        connackTimeoutMs = connackTimeout.inWholeMilliseconds,
        closeLingerMs = closeLinger.inWholeMilliseconds,
        maxIncomingPacket = limits.maxIncomingPacket,
        maxTopicLength = limits.maxTopicLength,
    ),
    connectTimeoutMs = connectTimeout.inWholeMilliseconds,
    reconnect = reconnect,
    offlineCapacity = offlineQueue.capacity,
    subscribeBatch = limits.subscribeBatch,
    consumerStallLimitMs = limits.consumerStallLimit.inWholeMilliseconds,
    maxFilters = limits.maxFilters,
)
