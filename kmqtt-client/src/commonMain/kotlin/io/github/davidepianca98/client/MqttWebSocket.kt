package io.github.davidepianca98.client

// MQTT over WebSocket (SEVEN-248). Security: toString() prints neither the path (a query string may carry a token)
// nor the header values (an Authorization header is a secret); validation messages contain no values either.

/**
 * WebSocket settings of an [MqttClient] ([MqttClientOptions.webSocket]): MQTT packets travel in binary WebSocket frames
 * (RFC 6455, subprotocol `mqtt`). With [MqttClientOptions.tls] the connection is `wss://` (WebSocket over TLS with the
 * same certificate and host name checks as plain TLS), without it `ws://`.
 *
 * Invalid values are rejected with [IllegalArgumentException] at construction; the message never contains the path or
 * a header value.
 *
 * @param path request path of the HTTP upgrade, starting with `/`, visible ASCII only (percent-encode anything else);
 *   it may contain a query string. Most brokers listen on `/mqtt` (EMQX, HiveMQ, VerneMQ); Mosquitto accepts any path.
 * @param headers extra headers of the upgrade request, for example `Authorization` for a broker behind a reverse proxy.
 *   Names are HTTP tokens and must be unique ignoring case; values are visible ASCII, space and tab. The headers the
 *   client sets itself cannot be given here: `Host`, `Upgrade`, `Connection`, `Content-Length`, `Transfer-Encoding` and
 *   every `Sec-WebSocket-*`.
 */
public class MqttWebSocket(
    public val path: String = DEFAULT_PATH,
    headers: Map<String, String> = emptyMap(),
) {
    /** Extra headers of the upgrade request, in the order given (a copy: later changes of the caller's map do not apply). */
    public val headers: Map<String, String> = headers.toMap()

    init {
        require(path.length in 1..MAX_PATH && path[0] == '/' && path.all { it in '!'..'~' }) {
            "webSocket.path must start with '/' and contain only visible ASCII (at most $MAX_PATH characters)"
        }
        require(headers.size <= MAX_HEADERS) { "webSocket.headers: more than $MAX_HEADERS headers" }
        val seen = HashSet<String>()
        for ((name, value) in headers) {
            require(name.isNotEmpty() && name.all { it.isTokenChar() }) { "webSocket.headers: a name is not an HTTP token" }
            val lower = name.lowercase()
            require(lower !in RESERVED && !lower.startsWith("sec-websocket-")) {
                "webSocket.headers: Host, Upgrade, Connection, Content-Length, Transfer-Encoding and Sec-WebSocket-* " +
                    "are set by the client"
            }
            require(seen.add(lower)) { "webSocket.headers: a name is repeated (names ignore case)" }
            require(value.length <= MAX_VALUE && value.all { it == '\t' || it in ' '..'~' }) {
                "webSocket.headers: a value contains a control or non-ASCII character or is too long"
            }
        }
    }

    override fun equals(other: Any?): Boolean = other is MqttWebSocket && other.path == path && other.headers == headers

    override fun hashCode(): Int = path.hashCode() * 31 + headers.hashCode()

    override fun toString(): String = "MqttWebSocket(path=***, headers=${headers.size})"

    public companion object {
        /** Default request path. */
        public const val DEFAULT_PATH: String = "/mqtt"

        private const val MAX_PATH = 2048
        private const val MAX_HEADERS = 32
        private const val MAX_VALUE = 4096
        private val RESERVED = setOf("host", "upgrade", "connection", "content-length", "transfer-encoding")
        private const val TOKEN_SYMBOLS = "!#$%&'*+-.^_`|~"

        private fun Char.isTokenChar(): Boolean =
            this in 'a'..'z' || this in 'A'..'Z' || this in '0'..'9' || this in TOKEN_SYMBOLS
    }
}

/** Transport of an [MqttClient], derived from [MqttClientOptions.tls] and [MqttClientOptions.webSocket]. */
public enum class MqttTransportKind {
    /** Plain TCP: no `tls`, no `webSocket`. */
    Tcp,

    /** TLS over TCP: `tls` set, no `webSocket`. */
    Tls,

    /** WebSocket over TCP (`ws://`): `webSocket` set, no `tls`. */
    WebSocket,

    /** WebSocket over TLS (`wss://`): both `webSocket` and `tls` set. */
    SecureWebSocket,
}
