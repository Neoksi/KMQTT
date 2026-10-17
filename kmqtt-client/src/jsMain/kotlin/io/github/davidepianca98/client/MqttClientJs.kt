package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.internal.MqttClientImpl

/**
 * Creates an [MqttClient] for Node.js ([NodeHost] on the Node event loop, clock [MqttClock.Monotonic]): plain TCP
 * (`net`) or, with [MqttClientOptions.tls], TLS (`tls`, TLS 1.2+, host name verification on unless
 * [MqttClientOptions.verifyHostname] is turned off explicitly). This is the main factory on JS (Node only; browsers have
 * no TCP sockets).
 *
 * MQTT over WebSocket ([MqttClientOptions.webSocket]) is not available on JS yet: such options are refused with
 * [IllegalArgumentException]. "Trust all" (`TLSClientSettings.checkServerCertificate = false`) with
 * `verifyHostname = true` is refused as on the JVM: pass `verifyHostname = false` explicitly.
 *
 * Nothing is started by this call. Always call [MqttClient.close] when done.
 */
@OptIn(MqttCoreApi::class)
public fun MqttClient(options: MqttClientOptions): MqttClient {
    require(options.webSocket == null) { "MQTT over WebSocket is not supported on JS yet" }
    val clock = MqttClock.Monotonic
    val tls = options.tls ?: return MqttClientImpl(options, NodeHost(clock), clock)
    val host = NodeHost(clock, NodeTransportFactory.tls(tls, options.verifyHostname))
    return MqttClientImpl(options, host, clock, hostAppliesTls = true)
}
