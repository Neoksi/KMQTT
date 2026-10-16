# KMQTT

**KMQTT** is a Kotlin Multiplatform MQTT 3.1.1/5.0 Client and Broker, with the objective of targeting the most possible build targets.


## Client features
:x: = To Do  
:white_check_mark: = Supported  
:heavy_plus_sign: = Work In Progress

|        Platform         |     MQTT 3.1.1     |      MQTT 5.0      |        TCP         |        TLS         |     Websocket      |
|:-----------------------:|:------------------:|:------------------:|:------------------:|:------------------:|:------------------:|
|           JVM           | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|       Windows X64       | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|        Linux X64        | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|       Linux ARM64       | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|         Node.js         | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|         iOS X64         | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|        iOS ARM64        | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|   iOS Simulator ARM64   | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|        macOS X64        | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|       macOS ARM64       | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|        tvOS X64         | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|       tvOS ARM64        | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|  tvOS Simulator ARM64   | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|       watchOS X64       | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|      watchOS ARM32      | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|      watchOS ARM64      | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| watchOS Simulator ARM64 | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |

## Broker features
:x: = To Do  
:white_check_mark: = Supported  
:heavy_plus_sign: = Work In Progress

|  Platform   |     MQTT 3.1.1     |      MQTT 5.0       |         TCP         |        TLS         |     Websocket      |    Clustering     |
|:-----------:|:------------------:|:-------------------:|:-------------------:|:------------------:|:------------------:|:-----------------:|
|     JVM     | :white_check_mark: | :white_check_mark:  | :white_check_mark:  | :white_check_mark: | :white_check_mark: | :heavy_plus_sign: |
| Windows X64 | :white_check_mark: | :white_check_mark:  | :white_check_mark:  | :white_check_mark: | :white_check_mark: |        :x:        |
|  Linux X64  | :white_check_mark: | :white_check_mark:  | :white_check_mark:  | :white_check_mark: | :white_check_mark: |        :x:        |
| Linux ARM64 | :white_check_mark: | :white_check_mark:  | :white_check_mark:  | :white_check_mark: | :white_check_mark: |        :x:        |
|   Node.js   | :white_check_mark: | :white_check_mark:  | :white_check_mark:  | :white_check_mark: | :white_check_mark: |        :x:        |
|  macOS X64  | :white_check_mark: | :white_check_mark:  | :white_check_mark:  | :white_check_mark: | :white_check_mark: |        :x:        |
| macOS ARM64 | :white_check_mark: | :white_check_mark:  | :white_check_mark:  | :white_check_mark: | :white_check_mark: |        :x:        |




## Getting Started with the client

> This section describes the blocking `MQTTClient`, which is kept for compatibility. New code on the JVM should use the
> [coroutine client](#coroutine-client-iogithubdavidepianca98clientmqttclient) described below (fork only).

### Library

#### Gradle

If you are getting an error saying that OpenSSL hasn't been found, please copy the correct file from https://github.com/davidepianca98/KMQTT/tree/master/kmqtt-common/src/nativeInterop in your project's main directory.

##### Kotlin Multiplatform plugin
On the Kotlin Multiplatform plugin you only need to require the dependency on the common source set and the platform specific parts will be automatically imported.
```gradle
repositories {
    mavenCentral()
}

kotlin {
    jvm()
    mingwX64()
    macosX64()

    sourceSets {
        commonMain {
            dependencies {
                implementation("io.github.davidepianca98:kmqtt-common:1.0.0")
                implementation("io.github.davidepianca98:kmqtt-client:1.0.0")
            }
        }
    }
}
```

##### Single platform project
```gradle
repositories {
    mavenCentral()
}
dependencies {
    implementation("io.github.davidepianca98:kmqtt-common-jvm:1.0.0")
    implementation("io.github.davidepianca98:kmqtt-client-jvm:1.0.0")
}
```

Replace jvm with js, linuxx64, linuxarm64, mingwx64, macosx64, macosarm64, iosarm64, iosX64, iossimulatorarm64, tvossimulatorarm64, tvosx64, tvosarm64, watchosarm32, watchosarm64, watchosx64, watchossimulatorarm64 based on the desired target.

#### Quick start code example
This code starts the MQTT client on port 1883 without TLS encryption. You can play with MQTTClient() constructor parameters to set the various settings
```kotlin
fun main() {
    val client = MQTTClient(
        MQTTVersion.MQTT5,
        "test.mosquitto.org",
        1883,
        null
    ) {
        println(it.payload?.toByteArray()?.decodeToString())
    }
    client.subscribe(listOf(Subscription("/randomTopic", SubscriptionOptions(Qos.EXACTLY_ONCE))))
    client.publish(false, Qos.EXACTLY_ONCE, "/randomTopic", "hello".encodeToByteArray().toUByteArray())
    client.run() // Blocking method, use step() if you don't want to block the thread
}
```

#### TLS code example
The certificates and key must be in PEM format, and they can be either a path to the PEM file or the actual PEM string. The password can be null if the private key has no protection.
```kotlin
fun main() {
    val client = MQTTClient(
        MQTTVersion.MQTT5,
        "test.mosquitto.org",
        8883,
        TLSClientSettings(
            serverCertificate = "mosquitto.org.crt",
        )
    ) {
        println(it.payload?.toByteArray()?.decodeToString())
    }
    client.subscribe(listOf(Subscription("/randomTopic", SubscriptionOptions(Qos.EXACTLY_ONCE))))
    client.publish(false, Qos.EXACTLY_ONCE, "/randomTopic", "hello".encodeToByteArray().toUByteArray())
    client.run() // Blocking method, use step() if you don't want to block the thread
}
```



## Coroutine client (`io.github.davidepianca98.client.MqttClient`)

A coroutine-friendly MQTT 3.1.1/5.0 client that sits next to the blocking `MQTTClient` above. It handles reconnection,
declarative subscriptions, publication results and back pressure for you, so application code does not need its own
connection-management layer.

**Where it exists.** This client exists only in this fork (`Neoksi/KMQTT`, branch `new_mqtt_client`), not in upstream
and not in the Maven `1.0.0` artifacts mentioned above. Build the fork from source, for example with a Gradle
`includeBuild` that substitutes `io.github.davidepianca98:kmqtt-client` (and `kmqtt-common`) by the fork's projects. The
public API uses `kotlinx.coroutines` types (`Flow`, `StateFlow`, `Deferred`, `Job`) and `kmqtt-common` types (`Qos`,
`ReasonCode`, `MQTTVersion`, `TLSClientSettings`); `kmqtt-client` exposes both modules as `api` dependencies, so they
come with it.

**Targets.** The API and the protocol engine are in `commonMain` and compile for every target, but the only I/O host
that exists is the JVM one (`NioActorHost`, a `java.nio` selector on its own thread). On JavaScript and native targets
the code compiles but there is no host to run it, so use the blocking `MQTTClient` there (or implement `MqttHost`, see
[Limitations](#limitations-of-the-first-version)).

What it gives you:

- one connection with automatic reconnection (`ReconnectPolicy`: exponential delay, jitter, optional attempt limit);
- subscriptions declared as named groups; they are re-established after every reconnect and refusals do not break the
  session;
- `publish` returns a result (`Sent`, `Delivered`, `Expired`, `Failed`) and has an offline queue with a time-to-live, so
  a stale command is never sent late;
- one ordered `Flow` of incoming messages with session boundaries and real back pressure (no message is dropped);
- orderly close that avoids a TCP reset;
- TLS with host name verification on by default (JVM);
- MQTT over WebSocket, `ws://` and `wss://` (JVM);
- no blocking call on your threads: a single daemon thread `kmqtt-io-<n>` per client does all the I/O.

### Quick start

```kotlin
import io.github.davidepianca98.client.*
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos
import kotlinx.coroutines.*
import kotlin.time.Duration.Companion.seconds

fun main() = runBlocking {
    val client = MqttClient( // the JVM factory: MqttClient(options)
        MqttClientOptions(
            host = "broker.example.org",
            port = 1883,
            version = MQTTVersion.MQTT5,
            credentials = MqttCredentials("user", "secret".encodeToByteArray()),
        )
    )

    // incoming: one collector for the whole life of the client
    val reader = launch {
        client.incoming.collect { item ->
            when (item) {
                is MqttIncoming.SessionStarted -> println("session ${item.epoch} started")
                is MqttIncoming.Message -> println("${item.topic}: ${item.payload.decodeToString()}")
                is MqttIncoming.SessionEnded -> println("session ${item.epoch} ended: ${item.error}")
            }
        }
    }

    // Declarative subscriptions: take effect after every CONNACK
    client.subscriptions.set("sensors", setOf("sensors/#"), Qos.AT_LEAST_ONCE)

    // connect() starts the client and waits for the first CONNACK
    when (val connected = withTimeout(15.seconds) { client.connect() }) {
        is ConnectResult.Connected -> println("connected, epoch ${connected.epoch}")
        is ConnectResult.Failed -> println("not connected: ${connected.error}")
    }
    println(client.subscriptions.awaitApplied("sensors")) // filter -> Granted/Refused

    // Publish and wait for the broker's acknowledgement
    val result = client.publish(
        topic = "lights/kitchen/set",
        payload = "on".encodeToByteArray(),
        qos = Qos.AT_LEAST_ONCE,
        ttl = 5.seconds, // if there is no connection for 5 s, the message expires and is never sent
        until = PublishStage.Delivered,
    )
    when (result) {
        is PublishResult.Sent -> println("handed to the socket in session ${result.epoch}")
        is PublishResult.Delivered -> println("acknowledged by the broker")
        PublishResult.Expired -> println("no connection in time, nothing was sent")
        is PublishResult.Failed -> println("failed: ${result.reason}")
    }

    client.close() // always call it, even after errors
    reader.join() // incoming completes after close
}
```

The default values of `MqttClientOptions` are: MQTT 5, keep-alive 30 s, connect and CONNACK timeouts 30 s, `closeLinger`
3 s, offline queue of 64 publications with a 10 s time-to-live, host name verification on, no logger. `toString()` of the
options hides the address, credentials, TLS settings and the will.

### Publishing

`publish(topic, payload, qos = AT_LEAST_ONCE, retain = false, ttl, until = Sent)` suspends until the requested stage and
returns exactly one result per call:

| Result | Meaning |
|---|---|
| `Sent(epoch)` | The PUBLISH was handed to the transport of a live session. The I/O thread does not confirm that the operating system took every byte, and the broker has confirmed nothing yet. |
| `Delivered(epoch)` | The broker acknowledged it: PUBACK for QoS 1, PUBCOMP for QoS 2. Only with `until = PublishStage.Delivered`. QoS 0 has no acknowledgement and ends as `Sent`. |
| `Expired` | There was no live session within `ttl`; the message was never sent. |
| `Failed(reason)` | `ConnectionLost` (written, but the connection broke before the acknowledgement), `SessionLost` (persistent session only: written in an earlier connection, and the broker no longer has the session), `Closed`, `Rejected(code)` (refused by the client against what the broker announced in CONNACK, or by the broker with an MQTT 5 PUBACK code of 0x80 or more), `QueueFull`. |

Details worth knowing:

- Publications and subscription changes reach the socket in the order of the calls.
- Without a connection a publication waits in the offline queue (`MqttClientOptions.offlineQueue`) for at most `ttl`.
  Anything that expired is never sent afterwards, which is the point: do not turn a light on ten minutes late.
- `QueueFull` comes from the offline queue (`OfflineQueue.capacity`) or, at once and without blocking, when
  `MqttLimits.commandQueue` publications (1024) are already waiting for the I/O thread to take them.
- By default every session uses a clean start. A publication written but not acknowledged when the connection breaks is
  not re-sent after reconnecting; with `until = Delivered` you get `Failed(ConnectionLost)` and decide yourself whether
  to retry. With `persistentSession = true` it is re-sent instead, see [Persistent session](#persistent-session).
- With MQTT 3.1.1 there is no failure code in PUBACK, so `Delivered` does not prove that the broker's access control let
  the publication through.
- Cancelling the calling coroutine before the packet was written removes the publication from the queue; after the write
  it only stops waiting.
- The payload array is not copied; do not modify it after the call.

### Persistent session

Off by default. `MqttClientOptions(persistentSession = true, clientId = "...", sessionExpiry = 10.minutes)` connects with
Clean Start = 0 (Clean Session = 0 in MQTT 3.1.1) and resumes the broker session after a reconnect:

- The client keeps every unacknowledged QoS 1 and QoS 2 publication (packet identifier, payload, time-to-live) across
  reconnects, at most `MqttLimits.maxUnacknowledged` (64); beyond that new QoS 1/2 publications wait in the offline queue.
- CONNACK with **Session Present = 1**: they are re-sent with the DUP flag and their original packet identifiers (QoS 2
  after PUBREC: PUBREL), in the original order and before anything published later. A waiting `until = Delivered` call
  then completes with `Delivered` as usual; one that already got `Sent` is re-sent silently.
- The time-to-live still rules: it is counted from the `publish` call, and a publication whose `ttl` passed before the
  resend is not re-sent (`Expired` while offline). A command is never executed late just because the session survived.
- CONNACK with **Session Present = 0** (session expired, broker restarted): they fail with `Failed(SessionLost)`, nothing is
  re-sent.
- `publish(..., resend = false)` marks a publication that must never be written twice, for example a dangerous command
  that needed a confirmation: if the connection breaks before the acknowledgement it fails with `ConnectionLost` at once,
  exactly as with a clean session.
- Rule of thumb: a publication that has received `Failed` or `Expired` is never written again; cancelling the calling
  coroutine also drops it.
- `clientId` is required (the broker finds the session by it). `sessionExpiry` (MQTT 5 Session Expiry Interval, at least
  1 s, `Duration.INFINITE` = never) is how long the broker keeps the session; MQTT 3.1.1 has no such field and the broker
  decides.
- Subscriptions are still subscribed again after every CONNACK; filters removed while offline are unsubscribed on resume.
  While offline the broker may queue QoS 1/2 messages of the session's subscriptions and deliver them after the reconnect.
- The state lives in the memory of the client instance: a new `MqttClient` or a restarted process starts without it (the
  broker may still report Session Present = 1, then there is simply nothing to re-send).
- QoS 1 is "at least once": a re-sent publication may reach subscribers twice when the first copy had arrived.

### Subscriptions

Subscriptions are declared in named **groups**; the desired set is the union of all groups and the QoS of a filter is the
maximum over its groups.

```kotlin
val subs = client.subscriptions

// replace the set of a group (an empty set removes the group)
subs.set("sensors", setOf("sensors/+/temperature"), Qos.AT_LEAST_ONCE)
val applied: Map<String, TopicState> = subs.awaitApplied("sensors") // waits for the SUBACKs

// follow a flow of filter sets; the group is removed when the job ends or the client closes
val job = subs.bind("devices", filterSets, Qos.AT_MOST_ONCE, scope) // filterSets: Flow<Set<String>>

// a short-lived subscription, for example while discovering devices
val verdict = subs.withTemporary(setOf("homeassistant/+/+/config")) { group ->
    val refusal = withTimeoutOrNull(3.seconds) { group.refused.await() }
    if (refusal != null) "not allowed: ${refusal.code}" else "window is over"
}

subs.state.value // filter -> Pending | Granted(qos) | Refused(code)
subs.refusals.collect { println("refused ${it.code}") } // events, one per group with a refused filter
subs.resubscribe("sensors") // subscribe again so that the broker re-sends retained messages
```

- A change in a live session sends only the difference: UNSUBSCRIBE of the removed filters, then SUBSCRIBE of the added
  ones. After every CONNACK the whole set is subscribed again in batches of `MqttLimits.subscribeBatch` (50).
- A refused filter (SUBACK 0x80 or above) does not end the session. It becomes `TopicState.Refused(code)` and an event is
  emitted on `refusals`. The refusal is remembered for the session, so a group that adds the same filter gets the answer
  at once.
- `set` that would take the total number of distinct filters over `MqttLimits.maxFilters` (4096) is refused: the group
  stays as it was, the logger is told, and `awaitApplied` of that group throws `IllegalStateException` until a later `set`
  of the group succeeds.
- `awaitApplied` throws `IllegalStateException` if the client is closed (by you or by itself).

### Incoming messages and sessions

`client.incoming` is a `Flow<MqttIncoming>` that delivers messages strictly in order of receipt, with boundaries:
`SessionStarted(epoch)`, then the `Message`s of that session, then `SessionEnded(epoch, error)`.

- Epochs grow over the whole process: a newer connection always has a larger epoch. Use them to tell "this value came
  from the current connection" from "this is a leftover of a dead one".
- `incoming` has **one collector for the whole life of the client**. A second `collect`, also after the first collector
  was cancelled, throws `IllegalStateException`. Collect it once, in a long-lived scope, and fan out yourself.
- `connection: StateFlow<ConnectionState>` (`Idle`, `Connecting`, `Connected`, `Waiting`, `Closed`) is for UI and
  diagnostics. It conflates, so a collector may miss states; if you need the order relative to messages, use the session
  markers of `incoming`.
- A QoS 1 message is acknowledged to the broker when the client puts it into the buffer, not when your code processes it.
- The flow completes after `close()`; everything already received is delivered first, in order: the buffer, the
  messages held back beyond it while reading was paused, and the final `SessionEnded`.

### Back pressure and `ConsumerStalled`

`incoming` is buffered (`MqttLimits.incomingBuffer`, 1024 messages). When the buffer is full the client **stops reading the
socket**, so the pressure travels through TCP to the broker and nothing is dropped. Keep-alive pings keep going while
reading is paused, so the broker does not drop a healthy but slow client.

Memory of the incoming path is bounded by the buffer plus one socket read burst: the messages decoded in the step that
found the buffer full are held in order beyond it, and the client reads nothing more until the collector has made room.
On the JVM host a burst is at most 4 × 16 KiB = 64 KiB of wire bytes (with TLS the plaintext of that, plus at most one
TLS record); a packet started in earlier reads (up to `maxIncomingPacket`) may complete in the same step.

If the collector takes nothing for longer than `MqttLimits.consumerStallLimit` (60 s) the client assumes the consumer is
dead and closes the connection with `MqttError.ConsumerStalled`. After that the client stays in
`Waiting(lastError = ConsumerStalled)` without any network activity until the consumer has drained the buffer, and only
then reconnects (never before the backoff delay has passed). A consumer that never returns leaves the client in
`Waiting` until `close()`, which closes it at once. Note that a broker's own queue may drop QoS 0 messages when
a client does not read for long (for example Mosquitto's `max_queued_messages`), so make the consumer fast or buffer on
your side.

### Reconnection

`ReconnectPolicy(initial = 1 s, max = 10 s, jitter = 0.2, stableAfter = 30 s, maxAttempts = null)`:

- The delay doubles after each failed attempt up to `max` (1, 2, 4, 8, 10, 10... seconds). Jitter only shortens the delay.
- The attempt counter and the delay are reset only after a **stable session**: all SUBSCRIBEs of the connection were
  acknowledged without refusals, or the connection lived at least `stableAfter`. A broker that accepts CONNECT but denies
  subscriptions therefore cannot keep the client in a one-connection-per-second loop.
- `maxAttempts = null` retries forever; a number limits the reconnection attempts in a row, after which the client goes
  to `Closed(lastError)`. `ReconnectPolicy.Never` (`maxAttempts = 0`) makes the first end of a connection final, which is
  handy for a one-shot connection check: `connect()` returns the result of that single attempt (for example
  `Failed(Refused(code))`, whose code helps to choose another protocol version).
- A client that closed itself still holds its I/O thread until you call `close()`.
- A broker's redirect (`SERVER_MOVED`, `USE_ANOTHER_SERVER`) is never followed.
- Every event is also reported to `MqttLogger` (`connected`, `disconnected`, `reconnectIn`, ...) without topics, payloads
  or addresses. `MqttLogger.packet(direction, type, size)` reports every MQTT packet (direction, control packet type and
  size in bytes, nothing else): an outgoing one when it is handed to the I/O host for writing, an incoming one when its
  last byte has been read (also a packet that is then dropped as too large).

### TLS (JVM)

```kotlin
import io.github.davidepianca98.socket.tls.TLSClientSettings

val client = MqttClient(
    MqttClientOptions(
        host = "broker.example.org",
        port = 8883,
        tls = TLSClientSettings(
            serverCertificate = "broker.pem", // pin one certificate (a path or a PEM string); null = system trust store
            alpnProtocols = listOf("mqtt"), // optional ALPN
            serverNameIndications = null, // optional explicit SNI name; default is the connection host
        ),
    )
)
```

- TLS is available **only** through the JVM factory `MqttClient(options)`. The common factory
  `MqttClient(options, host, clock)` throws `IllegalArgumentException` when `tls != null`: a host given from outside is not
  known to apply TLS, and the client never falls back to plain text.
- **Host name verification is on by default** (`verifyHostname = true`), also for a pinned certificate and also when you
  connect by IP address (the certificate must then contain that IP in its subject alternative names). Turn it off only
  explicitly with `verifyHostname = false`.
- Combinations of `TLSClientSettings.checkServerCertificate` and `verifyHostname`:

  | `checkServerCertificate` | `verifyHostname` | Behaviour |
  |---|---|---|
  | `true` | `true` | The default: the certificate chain and the host name are checked. |
  | `true` | `false` | The chain is checked, the host name is not. Safe with a pinned `serverCertificate`: the pinned certificate itself defines the identity of the broker (use it for a broker reached by an IP address that is not in the certificate's SAN). Weak with the system trust store: any valid certificate of any site signed by a trusted CA is accepted, so pin the certificate instead. |
  | `false` | `true` | **Refused at creation**: the factories throw `IllegalArgumentException` ("trust-all TLS ... needs an explicit verifyHostname = false"). Without a trusted chain there is nothing to check the name against, and the client never turns the check off silently. |
  | `false` | `false` | Full "trust all": the traffic is encrypted, but the broker is not authenticated. Only for debugging and a trusted network. |
- Only TLS 1.2 and 1.3 are used. A client certificate can be set with `clientCertificate`, `clientCertificateKey` and
  `clientCertificatePassword`.
- Handshake failures are reported as `MqttError.Io(kind = "ssl")` with a generic message: the address and the names from
  the certificate are not exposed.
- `TLSClientSettings.toString()` prints the client key and password. Never log it; `MqttClientOptions.toString()` is safe.

### WebSocket (JVM)

```kotlin
val client = MqttClient(
    MqttClientOptions(
        host = "broker.example.org",
        port = 8084, // the broker's WebSocket listener
        webSocket = MqttWebSocket(
            path = "/mqtt", // the default; Mosquitto accepts any path
            headers = mapOf("Authorization" to "Bearer ..."), // optional extra headers of the upgrade request
        ),
        tls = TLSClientSettings(serverCertificate = "broker.pem"), // with tls: wss://, without: ws://
    )
)
client.config.transport // MqttTransportKind.SecureWebSocket
```

- The transport is chosen by two options: `tls` and `webSocket` (`MqttClientOptions.transport` shows the result: `Tcp`,
  `Tls`, `WebSocket` or `SecureWebSocket`). The default stays plain TCP.
- MQTT packets travel in binary frames of the `mqtt` subprotocol (RFC 6455). The client sends one masked frame per write;
  incoming frames may be fragmented and are streamed to the MQTT parser as they arrive, so a large frame is never
  buffered whole. WebSocket pings are answered by the client itself and have nothing to do with MQTT keep-alive
  (PINGREQ/PINGRESP still travel inside binary frames).
- `wss://` uses the same `TLSClientSettings` and the same rules as plain TLS: a pinned certificate or the system trust
  store, **host name verification on by default**, ALPN and SNI from the settings. Certificate and host name errors are
  reported as `MqttError.Io(kind = "ssl")`.
- Like TLS, WebSocket is available **only** through the JVM factory `MqttClient(options)`; the common factory
  `MqttClient(options, host, clock)` throws `IllegalArgumentException` when `webSocket != null`.
- A refused upgrade or a protocol violation of the server (a masked or text frame, reserved bits, a broken fragment
  sequence, ...) is `MqttError.Io(kind = "websocket")`. The message shows the HTTP status code at most, never the reason
  phrase, headers, body or the address.
- `MqttWebSocket.toString()` and `MqttClientOptions.toString()` show neither the path (a query string may carry a
  token) nor the header values. The client sets `Host`, `Upgrade`, `Connection` and the `Sec-WebSocket-*` headers itself;
  they cannot be given in `headers`. The response must select the `mqtt` subprotocol or none, and no extension.
- Closing: the client sends a WebSocket close frame after the last data (after DISCONNECT) and waits for the server's
  close frame or end of stream (bounded by the same flush allowance of about a second) before closing the socket. A close
  frame of the server is answered with the same code and ends the stream like a TCP FIN.

### Closing

`close()` shuts the client down in an orderly way:

1. requests already in the queue (for example publications that clear retained messages) are sent first, in order;
2. DISCONNECT is sent;
3. the client keeps reading and discarding incoming data until the broker closes the connection (it must, after a client
   DISCONNECT) or `MqttClientOptions.closeLinger` (default 3 s) passes;
4. only then the socket is closed and the I/O thread stops.

This ordering avoids a TCP reset: closing a socket that still has unread data makes the operating system send RST instead
of FIN, and a broker that sees RST may drop the tail of the queued data, including the DISCONNECT, and publish the will.
The broker never publishes the will after an orderly close.

- The write of the remaining bytes (including a TLS `close_notify`) is completed before the transport is closed, within
  about a second. With `closeLinger = 0` the client does not wait for the broker's FIN: all queued PUBLISH packets and the
  DISCONNECT are still written, but the connection may then end with RST if the broker still has data in flight. The
  no-reset guarantee holds for `closeLinger > 0`.
- Publications that could not be sent get `Failed(Closed)`, including those held back by the broker's receive window.
- Closing before CONNACK arrives closes the transport without DISCONNECT, so a broker that has already accepted CONNECT
  may publish the will.
- `close()` cannot hang: the total time is bounded by `closeLinger` plus about 3 seconds, then the thread is stopped
  forcibly. A second call waits for the same shutdown.
- Error paths without anything to send (broken connection, keep-alive timeout, stalled consumer) close the socket at
  once. A protocol error found by the client on MQTT 5 is answered with `DISCONNECT` carrying the reason code: that
  packet is written first (within about a second, like the tail of an orderly close), then the socket is closed without
  waiting for the broker.

### Limitations of the first version

- **No MQTT 5 enhanced authentication** (AUTH packets). Use the blocking `MQTTClient` for it. (WebSocket was added
  later, see [WebSocket](#websocket-jvm).)
- **Persistent session is opt-in** (`persistentSession`, see [Persistent session](#persistent-session)); its state is kept
  in memory only. A QoS 2 publication dropped on resume (time-to-live passed, `resend = false`) may leave its packet
  identifier held by a broker that already sent PUBREC. Topic aliases are not used.
- **QoS 2** (publishing and receiving) is implemented on the main path only and has no dedicated tests; the API and the
  tests concentrate on QoS 0 and 1.
- **I/O host on the JVM only.** There is no host for JavaScript or native targets. A custom host implements `MqttHost`
  (plus `HostDriver`, `MqttTransport`, ...), which is experimental API marked `@MqttCoreApi` and may change.
- The `clock` parameter of the common factory is accepted but the time comes from the host loop.
- A client that closes itself keeps its thread until `close()` is called.

### Security

- Passwords, the payload and topic of the will, topics, filters and addresses never appear in `toString()` of the public
  types, in `MqttError` or in `MqttLogger` events. `MqttError.Io` prints only the class name of its cause.
- Memory is bounded by `MqttLimits` (packet size, topic length, incoming buffer, number of filters, publications in the
  request queue) and `OfflineQueue.capacity`; a violation gives a typed outcome, not an `OutOfMemoryError`.
- The client never falls back from TLS to plain text, and verifies the host name unless you turn that off explicitly.
- See [TLS](#tls-jvm) for `checkServerCertificate` and `TLSClientSettings.toString()`.

### Testing your code

- The engine and the facade run on virtual time in this module's tests with an in-memory transport and host
  (`FakeTransport`, `VirtualHost` in `kmqtt-client/src/jvmTest`, driven by `kotlinx-coroutines-test`). They are `internal`
  to the module's tests today and are not published; copy them or use a real broker.
- For integration tests start the fork's own broker in a thread (`io.github.davidepianca98.mqtt.broker.Broker`, module
  `kmqtt-broker`) and connect to `127.0.0.1`; see `EmbeddedBrokerSupport.kt` and `MqttClientBrokerTest.kt` in
  `kmqtt-client/src/jvmTest`. TLS tests use the same broker with `TLSSettings`, WebSocket tests with
  `webSocketPort` (with `TLSSettings` that port speaks `wss`), see `WebSocketBrokerTest.kt`.
- Run the module's tests from the fork's root with `./gradlew :kmqtt-client:jvmTest` (when the fork is included in a larger
  build, the path is prefixed by the included build, for example `:kmqtt:kmqtt-client:jvmTest`).

### Differences from the blocking `MQTTClient`

| | `MQTTClient` | `MqttClient` |
|---|---|---|
| Threading | you call `run()`/`step()` and block a thread | own I/O thread, `suspend` API |
| Payload | `UByteArray` | `ByteArray` |
| Reconnection | manual | built in, with a policy |
| Subscriptions | `subscribe`/`unsubscribe` calls | declarative groups, restored after reconnect |
| Publication result | none | `Sent`/`Delivered`/`Expired`/`Failed` |
| Incoming | callback | ordered `Flow` with back pressure and session epochs |
| WebSocket | yes | yes (JVM) |
| Enhanced auth | yes | no |

## Getting Started with the broker

### Executables
You can download the executables for your platform under the release tab

#### Program Arguments

|     Argument      | Default Value |                                                       Description                                                       |
|:-----------------:|:-------------:|:-----------------------------------------------------------------------------------------------------------------------:|
|        -h         |   127.0.0.1   |                                         Interface address to bind the server to                                         |
|        -p         |     1883      |                                                Server port to listen to                                                 |
| --max-connections |      128      |                                    The maximum number of TCP connections to support                                     |
|    --key-store    |     null      |     The path to the PKCS12 keystore containing the private key and the certificate for TLS, if null TLS is disabled     |
|  --key-store-psw  |     null      | The password of the PKCS12 keystore indicated in --key-store, if the keystore has no password set, don't set the option |
|       --wsp       |     null      |                                             The WebSocket port to listen to                                             |

### Library

#### Gradle

If you are getting an error saying that OpenSSL hasn't been found, please copy the correct file from https://github.com/davidepianca98/KMQTT/tree/master/kmqtt-common/src/nativeInterop in your project's main directory.

##### Kotlin Multiplatform plugin
On the Kotlin Multiplatform plugin you only need to require the dependency on the common source set and the platform specific parts will be automatically imported.
```gradle
repositories {
    mavenCentral()
}

kotlin {
    jvm()
    mingwX64()
    macosX64()
    sourceSets {
        commonMain {
            dependencies {
                implementation("io.github.davidepianca98:kmqtt-common:1.0.0")
                implementation("io.github.davidepianca98:kmqtt-broker:1.0.0")
            }
        }
    }
}
```

##### Single platform project
```gradle
repositories {
    mavenCentral()
}
dependencies {
    implementation("io.github.davidepianca98:kmqtt-common-jvm:1.0.0")
    implementation("io.github.davidepianca98:kmqtt-broker-jvm:1.0.0")
}
```

Replace jvm with js, linuxx64, linuxarm64, macosarm64, macosx64, mingwx64 based on the desired target.

#### Quick start code example
This code starts the MQTT broker on port 1883 without TLS encryption. You can play with Broker() constructor parameters to set the various settings
```kotlin
fun main() {
    Broker().listen()
}
```

#### TLS code example
The keystore must be in PKCS12 format, the keystore password can be null
```kotlin
fun main() {
    val broker = Broker(
        tlsSettings = TLSSettings(keyStoreFilePath = "keyStore.p12", keyStorePassword = "password"),
        port = 8883
    ).listen()
    broker.listen()
}
```

#### Authentication code example
```kotlin
fun main() {
    val broker = Broker(authentication = object : Authentication {
        override fun authenticate(clientId: String, username: String?, password: UByteArray?): Boolean {
            // TODO Implement your authentication method    
            return username == "user" && password?.toByteArray()?.decodeToString() == "pass"
        }
    })
    broker.listen() // Blocking method, use step() if you don't want to block the thread
}
```

#### Authorization code example

```kotlin
fun main() {
    val broker = Broker(authorization = object : Authorization {
        override fun authorize(
            clientId: String,
            username: String?,
            password: UByteArray?, // != null only if savePassword set to true in the broker constructor
            topicName: String,
            isSubscription: Boolean,
            payload: UByteArray?
        ): Boolean {
            // TODO Implement your authorization method    
            return topicName == "$clientId/topic"
        }
    })
    broker.listen() // Blocking method, use step() if you don't want to block the thread
}
```

#### Message interceptor code example

```kotlin
fun main() {
    val broker = Broker(packetInterceptor = object : PacketInterceptor {
        override fun packetReceived(clientId: String, username: String?, password: UByteArray?, packet: MQTTPacket) {
            when (packet) {
                is MQTTConnect -> println(packet.protocolName)
                is MQTTPublish -> println(packet.topicName)
            }
        }
    })
    broker.listen() // Blocking method, use step() if you don't want to block the thread
}
```

#### Internal publish code example
```kotlin
fun main() {
    val broker = Broker()
    broker.publish(
        retain = false,
        topicName = "test/",
        qos = Qos.AT_MOST_ONCE,
        properties = MQTTProperties(),
        "testPayload".toByteArray().toUByteArray()
    )
    broker.listen() // Blocking method, use step() if you don't want to block the thread
}
```

#### Other advanced functionality

MQTT5 Enhanced Authentication: set the `enhancedAuthenticationProviders` Broker constructor parameter, implementing the
provider interface `EnhancedAuthenticationProvider`.

Session persistence: set the `persistence` Broker constructor parameter, implementing `Persistence` interface.

Bytes metrics: set the `bytesMetrics` Broker constructor parameter, implementing `BytesMetrics` interface.  
