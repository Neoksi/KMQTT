package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.internal.MqttClientImpl

/**
 * Creates an [MqttClient] for JavaScript (clock [MqttClock.Monotonic]). This is the main factory on JS.
 *
 * On Node.js the client runs on [NodeHost] (the Node event loop) and the transport follows
 * [MqttClientOptions.transport]: plain TCP (`net`); with [MqttClientOptions.tls] TLS (`tls`, TLS 1.2+, host name
 * verification on unless [MqttClientOptions.verifyHostname] is turned off explicitly); with
 * [MqttClientOptions.webSocket] MQTT over WebSocket (`ws://`, or `wss://` together with `tls`, with the same TLS checks
 * and the extra upgrade headers) — the same protocol handling as on the JVM.
 *
 * Anywhere else (a browser, or another runtime without Node's `process.versions.node`) this is [BrowserMqttClient]:
 * only WebSocket, through the runtime's `WebSocket` API.
 *
 * "Trust all" (`TLSClientSettings.checkServerCertificate = false`) with `verifyHostname = true` is refused as on the
 * JVM: pass `verifyHostname = false` explicitly. Nothing is started by this call. Always call [MqttClient.close] when
 * done.
 */
@OptIn(MqttCoreApi::class)
public fun MqttClient(options: MqttClientOptions): MqttClient {
    if (!isNodeJs()) return BrowserMqttClient(options)
    val clock = MqttClock.Monotonic
    val tls = options.tls
    val webSocket = options.webSocket
    val factory = when {
        webSocket != null -> NodeTransportFactory.webSocket(webSocket, tls, options.verifyHostname)
        tls != null -> NodeTransportFactory.tls(tls, options.verifyHostname)
        else -> return MqttClientImpl(options, NodeHost(clock), clock)
    }
    return MqttClientImpl(
        options,
        NodeHost(clock, factory),
        clock,
        hostAppliesTls = tls != null,
        hostAppliesWebSocket = webSocket != null
    )
}

/**
 * Creates an [MqttClient] on the WebSocket API of the runtime (WHATWG `WebSocket`: browsers, Node.js 22+, Deno);
 * A browser has no raw sockets, so this client speaks only MQTT over WebSocket.
 *
 * [MqttClientOptions.webSocket] is required (`ws://`, or `wss://` with [MqttClientOptions.tls]). What the browser API
 * cannot do is refused with [IllegalArgumentException] instead of being ignored silently:
 * - extra upgrade headers ([MqttWebSocket.headers]): the browser sets the headers itself (put a token into the path's
 *   query string or into the MQTT password instead);
 * - any TLS setting except the defaults: a pinned or own server certificate, a client certificate, ALPN, SNI,
 *   `checkServerCertificate = false` or `verifyHostname = false`. The browser checks the certificate chain against
 *   its own roots and the host name itself (`TLSClientSettings.version` is ignored).
 *
 * A failed connection is reported as `MqttError.Io` with `kind = "websocket"` (the browser does not tell an unknown
 * host from a refused connection or a TLS failure). Nothing is started by this call. Always call [MqttClient.close].
 */
@OptIn(MqttCoreApi::class)
public fun BrowserMqttClient(options: MqttClientOptions): MqttClient {
    val webSocket = options.webSocket
    require(webSocket != null) { "Browser: only MQTT over WebSocket is available (set MqttClientOptions.webSocket)" }
    require(webSocket.headers.isEmpty()) { "Browser: the WebSocket API cannot send extra upgrade headers" }
    val tls = options.tls
    if (tls != null) {
        require(
            tls.serverCertificate == null && tls.clientCertificate == null && tls.clientCertificateKey == null &&
                tls.clientCertificatePassword == null && tls.alpnProtocols == null && tls.serverNameIndications == null
        ) { "Browser: TLS certificates, ALPN and SNI are managed by the browser and cannot be set" }
        require(tls.checkServerCertificate && options.verifyHostname) {
            "Browser: certificate and host name checks cannot be turned off"
        }
    }
    val clock = MqttClock.Monotonic
    val secure = tls != null
    val host = BrowserHost(clock) { wakeup -> BrowserWebSocketTransport(wakeup, webSocket, secure) }
    return MqttClientImpl(options, host, clock, hostAppliesTls = secure, hostAppliesWebSocket = true)
}

/** Node.js: есть `process.versions.node` (в браузере `process` нет). */
internal fun isNodeJs(): Boolean =
    js("typeof process !== 'undefined' && process.versions != null && process.versions.node != null") as Boolean
