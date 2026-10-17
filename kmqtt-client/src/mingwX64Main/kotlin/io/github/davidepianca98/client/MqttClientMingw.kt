package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.internal.MqttClientImpl

/**
 * Creates an [MqttClient] for Windows (mingwX64) that runs on its own I/O thread `kmqtt-io-<n>` ([WinSockHost], clock
 * [MqttClock.Monotonic]): plain TCP over WinSock. This is the main factory on mingwX64.
 *
 * TLS ([MqttClientOptions.tls]) and MQTT over WebSocket ([MqttClientOptions.webSocket]) are not available on mingwX64
 * yet: such options are refused with [IllegalArgumentException] (the client never falls back to plain text).
 *
 * The thread is started by the first request, not by this call. Always call [MqttClient.close] when done.
 */
@OptIn(MqttCoreApi::class)
public fun MqttClient(options: MqttClientOptions): MqttClient {
    require(options.tls == null) { "TLS is not supported on mingwX64 yet" }
    require(options.webSocket == null) { "MQTT over WebSocket is not supported on mingwX64 yet" }
    val clock = MqttClock.Monotonic
    return MqttClientImpl(options, WinSockHost(nextHostThreadName(), clock), clock)
}
