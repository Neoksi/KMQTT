package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode

/**
 * Why a connection of an [MqttClient] ended or could not be established.
 *
 * Security: neither fields nor `toString()` contain addresses, topics, payloads or passwords. For [Io] only the class
 * name of the cause is printed, because the text of an I/O exception often contains `host:port`. Handshake errors of TLS
 * are reported without the address or the names in the certificate.
 */
public sealed interface MqttError {
    /**
     * The broker answered CONNECT with a failure code (with MQTT 3.1.1 the return code translated to [ReasonCode]).
     * With [MqttClientOptions.enhancedAuth] also the refusal of the client itself: the provider threw or did not answer
     * within the CONNACK timeout ([ReasonCode.NOT_AUTHORIZED]; see [MqttEnhancedAuth]).
     */
    public data class Refused(val code: ReasonCode) : MqttError

    /**
     * The broker sent DISCONNECT with the given [code]. A redirect (`SERVER_MOVED`, `USE_ANOTHER_SERVER`) is never
     * followed: the client always reconnects to the configured address.
     */
    public data class ServerDisconnect(val code: ReasonCode) : MqttError

    /** The broker did not answer PINGREQ in time. */
    public data object KeepAliveTimeout : MqttError

    /** No CONNACK arrived within [MqttClientOptions.connackTimeout]; also a broker that did not finish a
     * re-authentication exchange ([MqttClient.reauthenticate]) within that time. */
    public data object ConnackTimeout : MqttError

    /**
     * The TCP connection (with TLS and the WebSocket upgrade, when used) was not established within
     * [MqttClientOptions.connectTimeout].
     */
    public data object ConnectTimeout : MqttError

    /** The host name could not be resolved. */
    public data object UnknownHost : MqttError

    /** The broker violated the protocol or sent a malformed packet. */
    public data class Protocol(val code: ReasonCode) : MqttError

    /**
     * An I/O error. [kind] is a short label without addresses (for example `"read"`, `"write"`, `"connect"`, `"ssl"`
     * for a TLS error, `"ssl-hostname"`/`"ssl-trust"`/`"ssl-expired"` when the JVM TLS handshake was refused because
     * of the server certificate (host name does not match the certificate / the certificate is not trusted / the
     * certificate is expired or not yet valid), `"websocket"` for a refused WebSocket upgrade or a
     * WebSocket protocol error, `"closed"` when the client was closed). [cause] is the original exception for debugging; its
     * text is not part of `toString()`.
     */
    public data class Io(val kind: String, val cause: Throwable?) : MqttError {
        override fun toString(): String = "Io(kind=$kind, cause=${cause?.let { it::class.simpleName }})"
    }

    /**
     * The collector of [MqttClient.incoming] did not take messages for longer than [MqttLimits.consumerStallLimit] while
     * the buffer was full, so the connection was closed.
     */
    public data object ConsumerStalled : MqttError
}

/**
 * Why an incoming packet was dropped without ending the session ([MqttLogger.dropped]). Numbers only: no topic and no
 * payload.
 */
public sealed interface DropReason {
    /** The whole packet of [size] bytes (with the fixed header) exceeds [limit]; its body was skipped without buffering. */
    public data class TooLarge(val size: Int, val limit: Int) : DropReason

    /** The topic of a PUBLISH is [length] UTF-8 bytes long, more than [limit]. */
    public data class TopicTooLong(val length: Int, val limit: Int) : DropReason
}
