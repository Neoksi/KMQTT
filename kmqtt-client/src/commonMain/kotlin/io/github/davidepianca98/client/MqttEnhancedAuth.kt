package io.github.davidepianca98.client

/**
 * MQTT 5 enhanced authentication (AUTH packets, MQTT 5 §4.12): a challenge/response exchange such as SCRAM or Kerberos
 * that runs between CONNECT and CONNACK. Set it as [MqttClientOptions.enhancedAuth]; it requires MQTT 5.
 *
 * The exchange of every connection (each reconnect too):
 * 1. CONNECT carries the Authentication Method [method] and the Authentication Data returned by [initialData] (none when
 *    it returns `null`). [initialData] is called for every connection, so it can produce a fresh nonce. It runs on the
 *    I/O thread of the client: it must be fast and must not block. If it throws, the connection attempt ends with
 *    [MqttError.Refused] (`NOT_AUTHORIZED`) and nothing is sent.
 * 2. Every AUTH packet of the broker with the reason "continue authentication" calls [respond] with the method and the
 *    Authentication Data of the broker (`null` when it sent none). [respond] runs in a coroutine on
 *    `Dispatchers.Default`, never on the I/O thread; switch to an I/O dispatcher yourself for blocking work. Its result
 *    is sent back in an AUTH packet: the returned bytes as Authentication Data, `null` for an AUTH without data (as the
 *    legacy `MQTTClient.enhancedAuthCallback`). To refuse, throw: the client sends DISCONNECT with `NOT_AUTHORIZED` and
 *    the attempt ends with [MqttError.Refused] (`NOT_AUTHORIZED`). The text of the exception is never stored, logged or
 *    reported.
 * 3. The exchange ends with CONNACK. A success must carry the same Authentication Method (otherwise
 *    [MqttError.Protocol]); a failure code of the broker (for example `NOT_AUTHORIZED` or `BAD_AUTHENTICATION_METHOD`)
 *    ends the attempt with [MqttError.Refused] with that code.
 *
 * The whole exchange is bounded by [MqttClientOptions.connackTimeout], counted from CONNECT: if the broker is silent the
 * attempt ends with [MqttError.ConnackTimeout]; if [respond] has not answered in time it ends with [MqttError.Refused]
 * (`NOT_AUTHORIZED`), and its late answer is discarded. An answer computed for a connection that has meanwhile broken is
 * never sent on the next connection. After each failed attempt the client reconnects according to
 * [MqttClientOptions.reconnect], exactly as after any other refusal.
 *
 * Not supported: re-authentication during a session (AUTH with "re-authenticate", 0x19) — an AUTH packet of the broker
 * after CONNACK ends the connection with [MqttError.Protocol]; the Authentication Data of a successful CONNACK (for
 * example the server signature of SCRAM) is not passed to the application, so the broker is not verified by this
 * exchange (use TLS with certificate checks for that).
 *
 * [toString] prints the method only: authentication data are secrets.
 *
 * @param method Authentication Method, a non-empty name agreed with the broker (for example `"SCRAM-SHA-256"`).
 * @param initialData Authentication Data of CONNECT, computed for every connection; `null` for none.
 * @param respond answer to a challenge of the broker: `(method, data) -> answer`.
 */
public class MqttEnhancedAuth(
    public val method: String,
    public val initialData: () -> ByteArray? = { null },
    public val respond: suspend (method: String, data: ByteArray?) -> ByteArray?,
) {
    init {
        require(method.isNotEmpty()) { "enhancedAuth.method is empty" }
    }

    override fun toString(): String = "MqttEnhancedAuth(method=$method)"
}
