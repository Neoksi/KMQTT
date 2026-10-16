package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import kotlin.time.Duration

// Public result and state types of the coroutine client. No toString() here prints topics or payloads.

/** State of the connection of an [MqttClient], for UI and diagnostics ([MqttClient.connection]). */
public sealed interface ConnectionState {
    /** The client was created but not started. */
    public data object Idle : ConnectionState

    /**
     * A connection attempt is in progress (TCP or TLS connect, then waiting for CONNACK). [attempt] counts attempts
     * from 1 after the delay was reset by a stable session.
     */
    public data class Connecting(val attempt: Int) : ConnectionState

    /**
     * The session is established (CONNACK with success).
     *
     * @param epoch session number; it grows over the whole process, so a newer session always has a larger epoch.
     * @param sessionPresent the flag from CONNACK; with the clean start the client always uses it is `false`.
     * @param server properties announced by the broker.
     */
    public data class Connected(val epoch: Long, val sessionPresent: Boolean, val server: ServerInfo) : ConnectionState

    /**
     * The connection ended with [lastError]; the next attempt, number [attempt], starts after [delay]. With
     * [ReconnectPolicy.Never] or exhausted attempts the client goes to [Closed] instead.
     */
    public data class Waiting(val attempt: Int, val delay: Duration, val lastError: MqttError) : ConnectionState

    /**
     * Terminal state: an orderly [MqttClient.close] (`error == null`) or the reconnection attempts are exhausted
     * ([ReconnectPolicy.maxAttempts]), in which case [error] is the last error. An exhausted client still holds its I/O
     * thread until [MqttClient.close] is called.
     */
    public data class Closed(val error: MqttError?) : ConnectionState
}

/**
 * Broker properties from CONNACK. With MQTT 3.1.1, which has no such properties, the values are the protocol defaults.
 *
 * @param maximumQos highest QoS the broker accepts for publications.
 * @param retainAvailable whether the broker supports retained messages.
 * @param receiveMaximum how many unacknowledged QoS 1 and 2 publications the broker accepts at once.
 * @param maximumPacketSize largest packet the broker accepts, or `null` when not limited.
 * @param serverKeepAliveSec keep-alive imposed by the broker, or `null` when it keeps the requested one.
 */
public data class ServerInfo(
    val maximumQos: Qos,
    val retainAvailable: Boolean,
    val receiveMaximum: Int,
    val maximumPacketSize: Int?,
    val serverKeepAliveSec: Int?,
)

/**
 * An item of [MqttClient.incoming]: incoming messages in the order of receipt, together with the boundaries of
 * sessions. Epochs grow over the whole process: a new connection always has a larger epoch than the previous one.
 * Per session the items arrive as [SessionStarted], [Message]..., [SessionEnded]; no message of an epoch follows its
 * [SessionEnded].
 */
public sealed interface MqttIncoming {
    /** A session with the given [epoch] began; the messages of that session follow. */
    public data class SessionStarted(val epoch: Long) : MqttIncoming

    /**
     * An application message.
     *
     * @param topic topic name the message was published to.
     * @param payload message body; the array belongs to the receiver.
     * @param qos QoS of the delivery to this client.
     * @param retain the retain flag as sent by the broker.
     * @param epoch the session the message belongs to.
     */
    public class Message(
        public val topic: String,
        public val payload: ByteArray,
        public val qos: Qos,
        public val retain: Boolean,
        public val epoch: Long,
    ) : MqttIncoming {
        override fun toString(): String = "Message(epoch=$epoch, qos=$qos, retain=$retain, size=${payload.size})"
    }

    /** The session [epoch] ended. [error] is `null` for an orderly close. */
    public data class SessionEnded(val epoch: Long, val error: MqttError?) : MqttIncoming
}

/** The stage of a publication [MqttClient.publish] waits for. */
public enum class PublishStage {
    /** The PUBLISH packet was handed to the transport of a live session. */
    Sent,

    /** The broker acknowledged the publication: PUBACK for QoS 1, PUBCOMP for QoS 2. QoS 0 has none and ends as `Sent`. */
    Delivered,
}

/**
 * Outcome of a publication. There is exactly one outcome per call of [MqttClient.publish].
 *
 * The cases differ in what is known about the broker: [Sent] says only that the packet was handed to the transport,
 * [Delivered] that the broker acknowledged it, [Expired] that nothing was ever sent, and [Failed] why the publication
 * did not complete.
 */
public sealed interface PublishResult {
    /**
     * The packet was handed to the transport of the session [epoch]. The I/O thread does not confirm that the operating
     * system accepted every byte, and the broker has not confirmed anything yet.
     */
    public data class Sent(val epoch: Long) : PublishResult

    /** The broker acknowledged the publication in the session [epoch] (PUBACK, or PUBCOMP for QoS 2). */
    public data class Delivered(val epoch: Long) : PublishResult

    /** No live session appeared within the time-to-live; the publication was never sent to the broker. */
    public data object Expired : PublishResult

    /** The publication did not complete for the given [reason]. */
    public data class Failed(val reason: PublishFailure) : PublishResult
}

/** Why a publication failed. */
public sealed interface PublishFailure {
    /**
     * The packet was written but the connection broke before it was acknowledged (only with
     * `until = PublishStage.Delivered`). It is not re-sent after reconnecting; whether the broker received it is
     * unknown.
     */
    public data object ConnectionLost : PublishFailure

    /** The client was closed before the publication was sent, or before it was acknowledged. */
    public data object Closed : PublishFailure

    /**
     * Refused, either by the client before sending (the QoS, the retain flag, the size or the topic does not fit what
     * the broker announced in CONNACK; the code names the reason) or by the broker in a MQTT 5 PUBACK (code 0x80 or
     * above). [code] may be `null` when no reason code applies.
     */
    public data class Rejected(val code: ReasonCode?) : PublishFailure

    /** The offline queue is full ([OfflineQueue.capacity]). */
    public data object QueueFull : PublishFailure
}

/** Result of [MqttClient.connect]: the first CONNACK, or the end of the client. */
public sealed interface ConnectResult {
    /** The session [epoch] was established; [server] holds the properties announced by the broker. */
    public data class Connected(val epoch: Long, val server: ServerInfo) : ConnectResult

    /**
     * The client closed without connecting: with [ReconnectPolicy.Never] or exhausted attempts [error] is the error of
     * the last attempt (for example [MqttError.Refused], whose code tells whether another protocol version is worth
     * trying); after [MqttClient.close] it is an [MqttError.Io] with `kind = "closed"`.
     */
    public data class Failed(val error: MqttError) : ConnectResult
}
