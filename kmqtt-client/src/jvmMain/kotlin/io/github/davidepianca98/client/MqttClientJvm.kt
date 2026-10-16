package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.internal.MqttClientImpl

/**
 * Creates an [MqttClient] that runs on its own I/O thread `kmqtt-io-<n>` ([NioActorHost], clock
 * [MqttClock.Monotonic]). This is the main factory on the JVM and the only one that supports TLS.
 *
 * Without [MqttClientOptions.tls] the client uses plain TCP. With `tls != null` the host is built with a TLS transport
 * and the host name (and SNI) verification is on unless [MqttClientOptions.verifyHostname] is turned off explicitly.
 * The client never connects in plain text when `tls` is set: the common factory `MqttClient(options, host, clock)`
 * throws for `tls != null`, because it cannot know that a given host applies TLS.
 *
 * The thread is started by the first request, not by this call. Always call [MqttClient.close] when done.
 *
 * Invalid options throw [IllegalArgumentException]; the message contains no addresses or secrets.
 */
@OptIn(MqttCoreApi::class)
public fun MqttClient(options: MqttClientOptions): MqttClient {
    val clock = MqttClock.Monotonic
    val tls = options.tls ?: return MqttClientImpl(options, NioActorHost(nextHostThreadName(), clock), clock)
    val host = NioActorHost(
        nextHostThreadName(),
        clock,
        NioTransportFactory.tls(tls, options.verifyHostname),
        NioActorHost.DEFAULT_READ_BUFFER
    )
    return MqttClientImpl(options, host, clock, hostAppliesTls = true)
}
