package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.internal.MqttClientImpl

/**
 * Creates an [MqttClient] for the Apple targets (macOS, iOS, tvOS, watchOS) that runs on its own I/O thread
 * `kmqtt-io-<n>` ([PosixHost], POSIX `poll` over BSD sockets, clock [MqttClock.Monotonic]): plain TCP or, with
 * [MqttClientOptions.tls], TLS 1.2+ on OpenSSL (memory BIO over the same socket). This is the main factory on Apple.
 *
 * TLS follows the JVM rules: the pinned `serverCertificate` is the only trust anchor, the host name (and SNI) is
 * verified unless [MqttClientOptions.verifyHostname] is turned off explicitly, and "trust all"
 * (`checkServerCertificate = false`) with `verifyHostname = true` is refused with [IllegalArgumentException]. OpenSSL
 * does not see the system trust store of Apple platforms: without a pinned certificate only the file bundle
 * `/etc/ssl/cert.pem` (present on macOS) and `SSL_CERT_FILE`/`SSL_CERT_DIR` are used, so on iOS, tvOS and watchOS pin
 * the server certificate. MQTT over WebSocket ([MqttClientOptions.webSocket]) is not available on Apple yet: such
 * options are refused with [IllegalArgumentException] (the client never falls back to plain text).
 *
 * The thread is started by the first request, not by this call. Always call [MqttClient.close] when done.
 */
@OptIn(MqttCoreApi::class)
public fun MqttClient(options: MqttClientOptions): MqttClient {
    require(options.webSocket == null) { "MQTT over WebSocket is not supported on Apple platforms yet" }
    val clock = MqttClock.Monotonic
    val tls = options.tls ?: return MqttClientImpl(options, PosixHost(nextHostThreadName(), clock), clock)
    val factory = PosixTransportFactory.tls(tls, options.verifyHostname)
    val host = PosixHost(nextHostThreadName(), clock, factory, PosixHost.DEFAULT_READ_BUFFER)
    return MqttClientImpl(options, host, clock, hostAppliesTls = true)
}
