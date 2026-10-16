package io.github.davidepianca98.client

import io.github.davidepianca98.client.internal.MqttClientImpl
import io.github.davidepianca98.mqtt.packets.Qos
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow
import kotlin.time.Duration

/**
 * Coroutine MQTT client: one connection to a broker with automatic reconnection, declarative subscriptions
 * ([subscriptions]), an offline publish queue with a time-to-live, and an ordered, back-pressured stream of incoming
 * messages ([incoming]).
 *
 * Create it with `MqttClient(options)` on the JVM (this is the only factory that can use TLS) or with
 * `MqttClient(options, host)` on a custom [MqttHost]; see [MqttClientOptions] for the settings.
 *
 * **Threading.** Every method is thread-safe and non-blocking: a call puts a request into an unbounded FIFO queue and
 * wakes the client's own I/O thread (on the JVM a daemon thread named `kmqtt-io-<n>`). The protocol, timers and the
 * socket live on that thread only. Continuations of the `suspend` methods and of the flows resume on the dispatcher of
 * the caller or collector, never on the I/O thread. Publications and subscription changes reach the socket in the order
 * they were requested.
 *
 * **Lifecycle.** [ConnectionState.Idle] after creation, then [start] or [connect] begins connecting, and [close] ends
 * the client in [ConnectionState.Closed]. The I/O thread is started by the first request (any method call, not
 * only [start]; reading a property does not count), so that the time-to-live of a publication requested before
 * [start] already runs. A client is
 * single-use: it cannot be restarted after [close]. A client that closed itself (reconnection attempts exhausted, see
 * [ReconnectPolicy.maxAttempts]) still holds its I/O thread until [close] is called, so always call [close].
 *
 * Every session uses a clean start: unacknowledged publications and subscriptions of a broken connection are never
 * resumed by the broker; subscriptions are re-established by the client after each reconnect.
 */
public interface MqttClient {
    /** The options this client was created with. */
    public val config: MqttClientOptions

    /**
     * Connection state for UI and diagnostics. It is a [StateFlow], so it conflates: a collector may miss
     * intermediate states. Use [incoming] when the order relative to messages matters.
     */
    public val connection: StateFlow<ConnectionState>

    /**
     * Incoming messages in the order they were received, interleaved with session boundaries:
     * [MqttIncoming.SessionStarted], then the [MqttIncoming.Message]s of that session, then
     * [MqttIncoming.SessionEnded].
     *
     * The flow has a single collector for the whole lifetime of the client: a second `collect` (also after the first
     * collector was cancelled) throws [IllegalStateException]. It is buffered by [MqttLimits.incomingBuffer]. When the
     * buffer is full the client stops reading the socket, so the pressure propagates through TCP to the broker and
     * nothing is dropped. If the collector takes nothing for longer than [MqttLimits.consumerStallLimit] the connection
     * is closed with [MqttError.ConsumerStalled]. An incoming QoS 1 message is acknowledged to the broker when it is put
     * into the buffer, not when the collector processes it. The flow completes after [close]; messages already in the
     * buffer are still delivered first.
     */
    public val incoming: Flow<MqttIncoming>

    /** Declarative subscriptions of this client. */
    public val subscriptions: MqttSubscriptions

    /**
     * Starts connecting and keeps reconnecting according to [MqttClientOptions.reconnect]. Does not wait for the
     * connection. Idempotent; has no effect after [close].
     */
    public fun start()

    /**
     * [start] followed by waiting for the first CONNACK (returns at once when already connected).
     *
     * With [ReconnectPolicy.Never] the result is that of the first attempt. With reconnection enabled the call waits
     * until the client connects or is closed, so bound it with `withTimeout`. After [close] the result is
     * [ConnectResult.Failed].
     */
    public suspend fun connect(): ConnectResult

    /**
     * Publishes a message and suspends until the stage [until] is reached. Exactly one [PublishResult] is returned per
     * publication:
     * - [PublishResult.Sent]: the PUBLISH packet was handed to the transport of a live session (the I/O thread does not
     *   confirm that the operating system accepted every byte);
     * - [PublishResult.Delivered]: the broker acknowledged it (PUBACK for QoS 1, PUBCOMP for QoS 2); only when
     *   `until = PublishStage.Delivered`. A QoS 0 message has no acknowledgement, so its result is `Sent` even with
     *   `until = Delivered`;
     * - [PublishResult.Expired]: there was no live session for [ttl]; the message was never sent to the broker;
     * - [PublishResult.Failed]: see [PublishFailure].
     *
     * Without a live session, or while the broker's receive window is full, the publication waits in the offline queue
     * ([MqttClientOptions.offlineQueue]) for at most [ttl]. The queue is FIFO and keeps the order of calls. A
     * publication that was written but not acknowledged when the connection broke is not re-sent after reconnecting
     * (the new session is clean): it fails with [PublishFailure.ConnectionLost] if `until = Delivered`.
     *
     * Cancelling the calling coroutine before the packet was written removes the publication from the queue; after the
     * write it only stops waiting for the outcome.
     *
     * @param topic topic name (not a filter).
     * @param payload message body; the array is not copied, do not modify it after the call.
     * @param qos requested QoS; if it exceeds what the broker announced, the result is
     *   `Failed(Rejected(...))` without sending.
     * @param retain the retain flag.
     * @param ttl how long the publication may wait for a live session before it expires.
     * @param until the stage to wait for.
     */
    public suspend fun publish(
        topic: String,
        payload: ByteArray,
        qos: Qos = Qos.AT_LEAST_ONCE,
        retain: Boolean = false,
        ttl: Duration = config.offlineQueue.ttl,
        until: PublishStage = PublishStage.Sent,
    ): PublishResult

    /**
     * Orderly shutdown. Requests already in the queue (for example publications that clear retained messages) are sent
     * first, then DISCONNECT. After DISCONNECT the client keeps reading and discarding incoming data until the broker
     * closes the connection or [MqttClientOptions.closeLinger] passes, and only then closes the socket and stops the I/O
     * thread. This avoids a TCP reset that would make the broker lose the tail of the data and publish the will.
     * The broker does not publish the will after an orderly close.
     *
     * Publications that could not be sent are completed with [PublishFailure.Closed], including those held back by the
     * broker's receive window. Closing before the CONNACK arrives closes the transport without DISCONNECT, so a broker
     * that has already accepted the CONNECT may publish the will.
     *
     * A second call waits for the same shutdown. The call does not hang: the total time is bounded by
     * `closeLinger` plus a flush allowance of about 3 seconds, after which the I/O thread is stopped forcibly.
     * [incoming] completes and [connection] ends in [ConnectionState.Closed].
     */
    public suspend fun close()
}

/**
 * Creates a client that runs on the given [host].
 *
 * The factory is in common code so that targets without an I/O host still compile; on the JVM use
 * `MqttClient(options)` instead. The [host] is single-use and owned by the client: [MqttClient.close] closes it too.
 * [clock] must be the clock of the host.
 *
 * **Security:** when [MqttClientOptions.tls] is set this factory always throws [IllegalArgumentException] at
 * creation, before the host is started: a host passed from outside is not known to apply TLS (the public
 * `NioActorHost()` speaks plain TCP), and the client never falls back to plain text. Use the JVM factory
 * `MqttClient(options)` for TLS. Invalid options also throw [IllegalArgumentException]; the message contains no
 * addresses or secrets.
 */
public fun MqttClient(options: MqttClientOptions, host: MqttHost, clock: MqttClock = MqttClock.Monotonic): MqttClient =
    MqttClientImpl(options, host, clock)
