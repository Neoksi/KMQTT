package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import io.github.davidepianca98.socket.tls.TLSClientSettings
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

// Configuration of the coroutine client. Security: no toString() in this file prints the broker address, the login,
// the password, TLS keys and passwords, or the topic and payload of the will.

/**
 * Settings of an [MqttClient]. Invalid values are rejected with [IllegalArgumentException] at construction; the message
 * names the parameter but never contains an address or a secret.
 *
 * Do not compare two instances to decide whether a client must be recreated: [ReconnectPolicy.random] and [logger] are
 * lambdas and objects, which are compared by reference.
 *
 * [toString] hides the broker address, credentials, TLS settings and the will, so the options can be logged. The
 * [TLSClientSettings] you pass in are not protected this way: their own `toString()` prints the client key and
 * password, so never log them.
 *
 * @param host broker host name or IP address. Name resolution is done by the host implementation (on the JVM on
 *   `Dispatchers.IO`).
 * @param port broker port, 1..65535.
 * @param version MQTT protocol version.
 * @param credentials user name and password sent in CONNECT; `null` for anonymous access (pass `null`, not an empty
 *   user name).
 * @param tls TLS settings; `null` means plain TCP. TLS is applied by the host the client is created with: the JVM
 *   factory `MqttClient(options)` builds a TLS host; the common factory `MqttClient(options, host, clock)` refuses
 *   options with `tls != null` ([IllegalArgumentException]), so a client with `tls` never connects in plain text.
 *   ALPN protocols and an explicit SNI name come from [TLSClientSettings.alpnProtocols] and
 *   [TLSClientSettings.serverNameIndications].
 * @param verifyHostname check that the certificate of the broker matches the host name (and SNI). **Enabled by
 *   default**, also for a pinned certificate and a connection by IP address; it is turned off only explicitly. It has no
 *   effect without [tls]. Note that [TLSClientSettings.checkServerCertificate] set to `false` trusts any certificate
 *   and disables the host name check regardless of this flag.
 * @param clientId MQTT client identifier; `null` generates a random one. It must be unique on the broker, because a
 *   second connection with the same identifier makes the broker drop the first.
 * @param keepAlive MQTT keep-alive interval, 0..65535 seconds; zero disables keep-alive.
 * @param connectTimeout limit for the TCP (and TLS handshake) connection, including name resolution.
 * @param connackTimeout how long to wait for CONNACK after CONNECT.
 * @param closeLinger how long [MqttClient.close] waits after DISCONNECT for the broker to close the connection. With
 *   a positive value the socket is closed after the broker's FIN, which avoids a TCP reset (a reset can make the broker
 *   lose queued data and publish the will). With zero the client does not wait: all queued PUBLISH packets and the
 *   DISCONNECT are still written first (within about a second), but the connection may end with a reset if the broker
 *   still has data in flight.
 * @param will last will and testament of the session: the broker publishes it when the connection is lost, but not
 *   after an orderly [MqttClient.close].
 * @param reconnect reconnection policy.
 * @param offlineQueue limits of the queue of publications waiting for a connection.
 * @param limits memory limits.
 * @param logger sink for diagnostic events without topics or payloads.
 */
public data class MqttClientOptions(
    val host: String,
    val port: Int,
    val version: MQTTVersion = MQTTVersion.MQTT5,
    val credentials: MqttCredentials? = null,
    val tls: TLSClientSettings? = null,
    val verifyHostname: Boolean = true,
    val clientId: String? = null,
    val keepAlive: Duration = 30.seconds,
    val connectTimeout: Duration = 30.seconds,
    val connackTimeout: Duration = 30.seconds,
    val closeLinger: Duration = 3.seconds,
    val will: MqttWill? = null,
    val reconnect: ReconnectPolicy = ReconnectPolicy(),
    val offlineQueue: OfflineQueue = OfflineQueue(),
    val limits: MqttLimits = MqttLimits(),
    val logger: MqttLogger = MqttLogger.None,
) {
    init {
        require(host.isNotBlank()) { "host is blank" }
        require(port in 1..65_535) { "port out of 1..65535" }
        require(!keepAlive.isNegative() && keepAlive.inWholeSeconds <= 65_535) { "keepAlive out of 0..65535 s" }
        require(connectTimeout.isPositive()) { "connectTimeout <= 0" }
        require(connackTimeout.isPositive()) { "connackTimeout <= 0" }
        require(!closeLinger.isNegative()) { "closeLinger < 0" }
    }

    override fun toString(): String =
        "MqttClientOptions(version=$version, credentials=$credentials, tls=${if (tls == null) "off" else "on"}, " +
            "verifyHostname=$verifyHostname, clientId=${if (clientId == null) "random" else "set"}, keepAlive=$keepAlive, " +
            "connectTimeout=$connectTimeout, connackTimeout=$connackTimeout, closeLinger=$closeLinger, will=$will, " +
            "reconnect=$reconnect, offlineQueue=$offlineQueue, limits=$limits)"
}

/**
 * User name and password for CONNECT. [toString] prints neither; equality compares the password by content.
 *
 * @param username the user name.
 * @param password the password bytes, or `null` for none.
 */
public class MqttCredentials(public val username: String, public val password: ByteArray? = null) {
    override fun equals(other: Any?): Boolean =
        other is MqttCredentials && other.username == username && other.password.contentEquals(password)

    override fun hashCode(): Int = username.hashCode() * 31 + (password?.contentHashCode() ?: 0)

    override fun toString(): String = "MqttCredentials(username=***, password=${if (password == null) "null" else "***"})"
}

/**
 * Last will of a session, published by the broker when the connection is lost (not after an orderly
 * [MqttClient.close]). With MQTT 5 the client adds the empty will properties that brokers require. The QoS of the will
 * must not exceed the maximum QoS of the broker; the caller checks this. [toString] hides the topic and the payload.
 *
 * @param topic topic the broker publishes the will to.
 * @param payload will body.
 * @param retain whether the broker retains the will.
 * @param qos QoS of the will.
 */
public class MqttWill(
    public val topic: String,
    public val payload: ByteArray,
    public val retain: Boolean = false,
    public val qos: Qos = Qos.AT_MOST_ONCE,
) {
    override fun equals(other: Any?): Boolean =
        other is MqttWill && other.topic == topic && other.payload.contentEquals(payload) &&
            other.retain == retain && other.qos == qos

    override fun hashCode(): Int = ((topic.hashCode() * 31 + payload.contentHashCode()) * 31 + retain.hashCode()) * 31 + qos.hashCode()

    override fun toString(): String = "MqttWill(retain=$retain, qos=$qos, size=${payload.size})"
}

/**
 * Offline queue of publications. Without a live session a publication waits for at most [ttl] and then completes as
 * [PublishResult.Expired] (it is never sent to the broker afterwards). With more than [capacity] waiting publications
 * the new one fails with [PublishFailure.QueueFull]; a capacity of zero disables waiting.
 *
 * @param ttl default time-to-live; a single publication can override it with the `ttl` argument of
 *   [MqttClient.publish].
 * @param capacity maximum number of waiting publications.
 */
public data class OfflineQueue(val ttl: Duration = 10.seconds, val capacity: Int = 64) {
    init {
        require(!ttl.isNegative()) { "ttl < 0" }
        require(capacity >= 0) { "capacity < 0" }
    }
}

/**
 * Memory limits of the client. A violation produces a typed outcome (a dropped packet, a refused subscription set, a
 * failed publication), never an `OutOfMemoryError`.
 *
 * @param maxIncomingPacket largest accepted incoming packet in bytes, including the fixed header. With MQTT 5 the
 *   limit is announced to the broker; with MQTT 3.1.1 a larger packet is skipped without buffering
 *   ([MqttLogger.dropped] with [DropReason.TooLarge]) and the session stays alive.
 * @param maxTopicLength largest accepted topic of an incoming PUBLISH, in UTF-8 bytes; longer ones are dropped
 *   ([DropReason.TopicTooLong]).
 * @param incomingBuffer number of messages [MqttClient.incoming] buffers before the client stops reading the socket.
 * @param consumerStallLimit how long reading may stay paused because the buffer is full before the connection is closed
 *   with [MqttError.ConsumerStalled].
 * @param subscribeBatch number of filters in one SUBSCRIBE or UNSUBSCRIBE packet.
 * @param maxFilters number of distinct filters over all groups; a set that would exceed it is refused (the group stays
 *   unchanged, see [MqttSubscriptions.set]).
 * @param commandQueue reserved. The request queue of the client is currently unbounded; the value is validated but not
 *   applied.
 */
public data class MqttLimits(
    val maxIncomingPacket: Int = 256 * 1024,
    val maxTopicLength: Int = 1024,
    val incomingBuffer: Int = 1024,
    val consumerStallLimit: Duration = 60.seconds,
    val subscribeBatch: Int = 50,
    val maxFilters: Int = 4096,
    val commandQueue: Int = 1024,
) {
    init {
        require(maxIncomingPacket >= 2) { "maxIncomingPacket < 2" }
        require(maxTopicLength >= 1) { "maxTopicLength < 1" }
        require(incomingBuffer >= 1) { "incomingBuffer < 1" }
        require(consumerStallLimit.isPositive()) { "consumerStallLimit <= 0" }
        require(subscribeBatch >= 1) { "subscribeBatch < 1" }
        require(maxFilters >= 1) { "maxFilters < 1" }
        require(commandQueue >= 1) { "commandQueue < 1" }
    }
}

/**
 * Diagnostic events of the client. Events never contain topics, filters, payloads, addresses or passwords: only
 * numbers, codes and [MqttError] values. Callbacks run on the I/O thread of the client, so an implementation must be
 * fast and must not block; exceptions thrown by an implementation are swallowed by the client. All methods are
 * optional.
 */
public interface MqttLogger {
    /** A session with the given [epoch] was established (CONNACK received). */
    public fun connected(epoch: Long) {}

    /** The connection ended; [error] is `null` for an orderly close. */
    public fun disconnected(error: MqttError?) {}

    /** The next connection attempt is scheduled in [delayMs] milliseconds. */
    public fun reconnectIn(delayMs: Long) {}

    /** A SUBSCRIBE packet for [count] filters was sent. */
    public fun subscribe(count: Int) {}

    /** The broker refused [count] filters in a SUBACK; the session continues. */
    public fun refused(count: Int) {}

    /** An incoming packet was dropped without ending the session. */
    public fun dropped(reason: DropReason) {}

    /** A PUBACK of MQTT 5 carried the failure code [code]. */
    public fun publishRefused(code: ReasonCode) {}

    /** [MqttSubscriptions.set] was refused: the groups would hold [filters] distinct filters with a limit of [limit]. */
    public fun groupRejected(filters: Int, limit: Int) {}

    public companion object {
        /** A logger that records nothing. */
        public val None: MqttLogger = object : MqttLogger {
            override fun toString(): String = "MqttLogger.None"
        }
    }
}
