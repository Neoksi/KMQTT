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
 * Authenticity of the broker: the Authentication Data of a successful CONNACK (for example the server
 * signature of SCRAM) is checked by the function given to [verifyingConnack]. Without it the data are ignored and the
 * broker is not verified by this exchange (use TLS with certificate checks for that). If the check fails, the client
 * sends DISCONNECT with `NOT_AUTHORIZED`, the attempt ends with [MqttError.Refused] (`NOT_AUTHORIZED`) and the
 * connection is never reported as established. Note that `Refused(NOT_AUTHORIZED)` therefore does not always mean
 * that the broker refused: the client itself refuses when the provider throws, does not answer in time or the broker's
 * CONNACK data do not verify.
 *
 * Re-authentication during a session (MQTT 5 §4.12.1): [MqttClient.reauthenticate] starts a new exchange on
 * the live connection — an AUTH packet with "re-authenticate" (0x19), the same [method] and fresh Authentication Data
 * of [initialData] (called again, so it can produce a fresh nonce). The broker's challenges call [respond] exactly as
 * during the connection exchange; the broker's final AUTH with "success" (0x00) ends it, and its Authentication Data
 * are checked by the function of [verifyingConnack], exactly as the data of a successful CONNACK. The exchange is
 * bounded by [MqttClientOptions.connackTimeout]; a failure (the provider threw or did not answer in time, the broker
 * stayed silent, the verification failed) ends the connection like a failure of the initial exchange. Only the client
 * starts a re-authentication: an unsolicited AUTH packet of the broker after CONNACK still ends the connection with
 * [MqttError.Protocol]. Without calls to [MqttClient.reauthenticate] nothing changes: no AUTH packet is ever sent
 * after CONNACK.
 *
 * [toString] prints the method only: authentication data are secrets.
 *
 * @param method Authentication Method, a non-empty name agreed with the broker (for example `"SCRAM-SHA-256"`).
 * @param initialData Authentication Data of CONNECT, computed for every connection; `null` for none.
 * @param respond answer to a challenge of the broker: `(method, data) -> answer`.
 */
public class MqttEnhancedAuth private constructor(
    public val method: String,
    public val initialData: () -> ByteArray?,
    public val respond: suspend (method: String, data: ByteArray?) -> ByteArray?,
    internal val verifyConnack: ((data: ByteArray?) -> Boolean)?,
) {
    public constructor(
        method: String,
        initialData: () -> ByteArray? = { null },
        respond: suspend (method: String, data: ByteArray?) -> ByteArray?,
    ) : this(method, initialData, respond, null)

    init {
        require(method.isNotEmpty()) { "enhancedAuth.method is empty" }
    }

    /**
     * A copy that also checks the Authentication Data of a successful CONNACK with [verify] (`data` is `null` when the
     * broker sent none). `false` or an exception means the broker is not genuine: DISCONNECT `NOT_AUTHORIZED` and
     * [MqttError.Refused] (`NOT_AUTHORIZED`). Runs on the I/O thread of the client: it must be fast and must not block.
     * The text of an exception is never stored, logged or reported.
     */
    public fun verifyingConnack(verify: (data: ByteArray?) -> Boolean): MqttEnhancedAuth =
        MqttEnhancedAuth(method, initialData, respond, verify)

    override fun toString(): String = "MqttEnhancedAuth(method=$method)"
}
