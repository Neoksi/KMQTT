package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.internal.MqttClientImpl

/**
 * Creates an [MqttClient] for Windows (mingwX64) that runs on its own I/O thread `kmqtt-io-<n>` ([WinSockHost], clock
 * [MqttClock.Monotonic]): plain TCP over WinSock or, with [MqttClientOptions.tls], TLS 1.2+ on OpenSSL (memory BIO
 * over the same socket). This is the main factory on mingwX64.
 *
 * TLS follows the JVM rules: the pinned `serverCertificate` is the only trust anchor (otherwise the Windows `ROOT`
 * store), the host name (and SNI) is verified unless [MqttClientOptions.verifyHostname] is turned off explicitly, and
 * "trust all" (`checkServerCertificate = false`) with `verifyHostname = true` is refused with
 * [IllegalArgumentException]. MQTT over WebSocket ([MqttClientOptions.webSocket]) is not available on mingwX64 yet:
 * such options are refused with [IllegalArgumentException] (the client never falls back to plain text).
 *
 * The thread is started by the first request, not by this call. Always call [MqttClient.close] when done.
 */
@OptIn(MqttCoreApi::class)
public fun MqttClient(options: MqttClientOptions): MqttClient {
    require(options.webSocket == null) { "MQTT over WebSocket is not supported on mingwX64 yet" }
    val clock = MqttClock.Monotonic
    val tls = options.tls ?: return MqttClientImpl(options, WinSockHost(nextHostThreadName(), clock), clock)
    val factory = WinSockTransportFactory.tls(tls, options.verifyHostname)
    val host = WinSockHost(nextHostThreadName(), clock, factory, WinSockHost.DEFAULT_READ_BUFFER)
    return MqttClientImpl(options, host, clock, hostAppliesTls = true)
}
