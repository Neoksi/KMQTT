# KMQTT

English | [Русский](Readme.ru.md)

**KMQTT** is a Kotlin Multiplatform MQTT 3.1.1/5.0 Client and Broker, with the objective of targeting the most possible build targets.

This repository is the fork `Neoksi/KMQTT` (branch `new_mqtt_client`) of `davidepianca98/KMQTT`. It has two MQTT clients
and one broker:

| What | Where | Use it for |
|---|---|---|
| **Coroutine client** `io.github.davidepianca98.client.MqttClient` | [section below](#coroutine-client-iogithubdavidepianca98clientmqttclient) (fork only) | New code: JVM, Android, Node.js, browsers (WebSocket only), Windows (mingwX64), Linux (linuxX64, linuxArm64), Apple (macOS, iOS, tvOS, watchOS). `suspend` API, reconnection, declarative subscriptions, back pressure. |
| **Blocking client** `MQTTClient` (legacy) | [Legacy blocking client](#legacy-blocking-client-mqttclient) | Old code. Kept as it is, not extended. |
| **Broker** `Broker` | [Getting Started with the broker](#getting-started-with-the-broker) | Embedded broker for apps and tests, also a stand-alone executable. |

## Client features (legacy blocking `MQTTClient`)

The coroutine client has its own table of targets, see [Targets](#coroutine-client-iogithubdavidepianca98clientmqttclient) below.

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




## Coroutine client (`io.github.davidepianca98.client.MqttClient`)

A coroutine-friendly MQTT 3.1.1/5.0 client that sits next to the [legacy blocking `MQTTClient`](#legacy-blocking-client-mqttclient).
It handles reconnection, declarative subscriptions, publication results and back pressure for you, so application code
does not need its own connection-management layer.

**Where it exists.** This client exists only in this fork (`Neoksi/KMQTT`, branch `new_mqtt_client`), not in upstream
and not in the Maven `1.0.0` artifacts mentioned in the legacy section. Build the fork from source, see
[Getting started](#getting-started-with-the-coroutine-client). The
public API uses `kotlinx.coroutines` types (`Flow`, `StateFlow`, `Deferred`, `Job`) and `kmqtt-common` types (`Qos`,
`ReasonCode`, `MQTTVersion`, `TLSClientSettings`); `kmqtt-client` exposes both modules as `api` dependencies, so they
come with it.

**Targets.** The API and the protocol engine are in `commonMain` and compile for every target. An I/O host (`MqttHost`)
exists for:

| Target | Host (factory `MqttClient(options)`) | TCP | TLS | WebSocket |
|---|---|---|---|---|
| JVM (incl. Android) | `NioActorHost`: a `java.nio` selector on its own thread `kmqtt-io-<n>` | yes | yes | yes |
| JS, Node.js | `NodeHost`: turns of the Node event loop (`net`, `tls`), no thread of its own | yes | yes | yes |
| JS, browser (any runtime without Node's `process`) | browser host: turns of the event loop (`MessageChannel`), the runtime's `WebSocket` API; `BrowserMqttClient(options)` | no | `wss://` only | yes |
| mingwX64 (Windows) | `WinSockHost`: WinSock `select` on its own Kotlin/Native worker `kmqtt-io-<n>` | yes | yes (OpenSSL) | no |
| linuxX64, linuxArm64 | `PosixHost`: POSIX `poll` on its own Kotlin/Native worker `kmqtt-io-<n>` (wake-up through a `pipe`) | yes | yes (OpenSSL) | no |
| Apple: macosX64, macosArm64, iosX64, iosArm64, iosSimulatorArm64, tvosX64, tvosArm64, tvosSimulatorArm64, watchosX64, watchosArm32, watchosArm64, watchosSimulatorArm64 | the same `PosixHost` over BSD sockets (`SO_NOSIGPIPE` on each socket instead of `MSG_NOSIGNAL`) | yes | yes (OpenSSL, pinned certificate on iOS/tvOS/watchOS) | no |

The Apple host is the Linux code (one source set `posixHostMain`); only the `SIGPIPE` protection differs. The Apple
targets compile on any OS (klibs), but the host runs only on Apple hardware: its tests run as `macosArm64Test` and
`macosX64Test` on a Mac and, with Xcode, on the iOS/tvOS/watchOS simulators (`iosSimulatorArm64Test` and the like);
the device targets (iosArm64, tvosArm64, watchosArm32, watchosArm64) are compiled only (see [Limitations](#limitations)). Each host
passes the same contract test suite
(`kmqtt-client/src/commonTest/.../hostcontract`; over WebSocket on JS for the Node and browser hosts). Options a
platform factory cannot honour (WebSocket on mingwX64, Linux and Apple, anything but WebSocket and default TLS in a browser,
see [WebSocket on JS](#websocket-on-js-nodejs-and-browsers)) are refused with `IllegalArgumentException`; the client
never falls back to plain text. Android has
no host of its own: it uses the JVM artifact (the `jvm` target is compiled for JVM 1.8) and the same `NioActorHost`.

What it gives you:

- one connection with automatic reconnection (`ReconnectPolicy`: exponential delay, jitter, optional attempt limit);
- subscriptions declared as named groups; they are re-established after every reconnect and refusals do not break the
  session; a filter that is not valid MQTT is refused locally and never sent;
- `publish` returns a result (`Sent`, `Delivered`, `Expired`, `Failed`) and has an offline queue with a time-to-live, so
  a stale command is never sent late;
- optional persistent session: unacknowledged QoS 1/2 publications are re-sent after a reconnect, except those marked
  `resend = false`;
- one ordered `Flow` of incoming messages with session boundaries and real back pressure (no message is dropped);
- orderly close that avoids a TCP reset;
- TLS with host name verification on by default (JVM, Node.js, mingwX64 and Linux on OpenSSL; in a browser `wss://`
  with the browser's own checks);
- MQTT over WebSocket, `ws://` and `wss://` (JVM, Node.js, browsers);
- MQTT 5 enhanced authentication (AUTH challenge/response, for example SCRAM), the same over every transport;
- no blocking call on your threads: a single daemon thread `kmqtt-io-<n>` per client does all the I/O (on Node.js the
  event loop does, without blocking calls).

### Getting started with the coroutine client

The fork is not published to Maven. Add it to your project as a git submodule and build it from source together with
your code, with a Gradle `includeBuild` that replaces the Maven coordinates by the fork's projects:

```bash
git submodule add -b new_mqtt_client https://github.com/Neoksi/KMQTT libs/kmqtt
git submodule update --init
```

```kotlin
// settings.gradle.kts of your project
includeBuild("libs/kmqtt") {
    dependencySubstitution {
        substitute(module("io.github.davidepianca98:kmqtt-common")).using(project(":kmqtt-common"))
        substitute(module("io.github.davidepianca98:kmqtt-client")).using(project(":kmqtt-client"))
        substitute(module("io.github.davidepianca98:kmqtt-broker")).using(project(":kmqtt-broker")) // only for tests with an embedded broker
    }
}
```

```kotlin
// build.gradle.kts of a Kotlin Multiplatform module: the dependency goes to commonMain, as in "Gradle" of the legacy client
kotlin {
    jvm()
    // an Android target (androidLibrary or androidTarget) takes the JVM variant of the same artifact
    sourceSets {
        commonMain {
            dependencies {
                // the version is not used for resolution: the substitution above wins
                implementation("io.github.davidepianca98:kmqtt-common:1.0.0")
                implementation("io.github.davidepianca98:kmqtt-client:1.0.0")
            }
        }
    }
}
```

The version of the Kotlin Gradle plugin in `gradle/libs.versions.toml` of the fork must match the one of your project
(an older plugin breaks Gradle Sync in the IDE on Gradle 9, console builds still pass). The public KMQTT `1.0.0` from
Maven Central has a different API (no `autoInit`, `onUnsubscribed`, `init`) and has no coroutine client.

### Quick examples

All examples need these imports (adjust the list to what you use). The calls are checked by
`kmqtt-client/src/jvmTest/kotlin/io/github/davidepianca98/client/ReadmeSnippetsTest.kt`, which runs the same code
against the embedded broker of the fork.

```kotlin
import io.github.davidepianca98.client.*
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.socket.tls.TLSClientSettings
import kotlinx.coroutines.*
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
```

#### Minimal: connect, subscribe, receive, publish, close

```kotlin
fun main() = runBlocking {
    val client = MqttClient(MqttClientOptions(host = "broker.example.org", port = 1883)) // MQTT 5, plain TCP
    val received = CompletableDeferred<String>()

    // incoming has ONE collector for the whole life of the client: start it before connect()
    val reader = launch {
        client.incoming.collect { item ->
            if (item is MqttIncoming.Message) received.complete("${item.topic}: ${item.payload.decodeToString()}")
        }
    }
    client.subscriptions.set("demo", setOf("demo/#"), Qos.AT_LEAST_ONCE) // declared once, kept across reconnects

    when (val connected = withTimeout(15.seconds) { client.connect() }) { // waits for the first CONNACK
        is ConnectResult.Connected -> println("connected, epoch ${connected.epoch}")
        is ConnectResult.Failed -> println("not connected: ${connected.error}")
    }
    client.subscriptions.awaitApplied("demo") // SUBACK of every filter: Granted or Refused

    client.publish("demo/hello", "hi".encodeToByteArray(), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
    println(withTimeout(5.seconds) { received.await() }) // our own message comes back

    client.close() // always call it, also after errors: it stops the I/O thread
    reader.join() // incoming completes after close
}
```

#### Everything at once: groups, results, time-to-live

```kotlin
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
3 s, `closeDrain` 3 s, offline queue of 64 publications with a 10 s time-to-live, host name verification on, no logger. `toString()` of the
options hides the address, credentials, TLS settings and the will.

#### TLS with a pinned certificate

```kotlin
val client = MqttClient(
    MqttClientOptions(
        host = "broker.example.org",
        port = 8883,
        tls = TLSClientSettings(
            serverCertificate = "broker.pem", // pin one certificate: a path to a PEM file or the PEM text itself
        ),
    )
)
```

A string with `-----BEGIN ` in it is always PEM text and is never opened as a file; any other string is a path. The
host name is checked by default, also for the pinned certificate: the certificate must contain the host (or the IP
address) of `host` in its subject alternative names. Without `serverCertificate` the system trust store is used. More in
[TLS (JVM)](#tls-jvm) and [TLS on mingwX64, Linux and Apple](#tls-on-mingwx64-linux-and-apple-openssl).

#### Automatic reconnection and watching the state

```kotlin
fun CoroutineScope.watch(client: MqttClient) {
    // for UI: StateFlow, so it conflates; do not rely on seeing every state
    launch {
        client.connection.collect { state ->
            when (state) {
                ConnectionState.Idle -> println("not started")
                is ConnectionState.Connecting -> println("connecting, attempt ${state.attempt}")
                is ConnectionState.Connected -> println("online, epoch ${state.epoch}, session present ${state.sessionPresent}")
                is ConnectionState.Waiting -> println("offline (${state.lastError}), next attempt in ${state.delay}")
                is ConnectionState.Closed -> println("closed: ${state.error}")
            }
        }
    }
    // for data: strictly ordered, with the boundaries of every connection
    launch {
        client.incoming.collect { item ->
            when (item) {
                is MqttIncoming.SessionStarted -> println("new connection ${item.epoch}: values of older epochs are stale")
                is MqttIncoming.Message -> println("[${item.epoch}] ${item.topic}")
                is MqttIncoming.SessionEnded -> println("connection ${item.epoch} lost: ${item.error}")
            }
        }
    }
}

fun CoroutineScope.startClient(): MqttClient {
    val client = MqttClient(
        MqttClientOptions(
            host = "broker.example.org",
            port = 1883,
            reconnect = ReconnectPolicy(initial = 1.seconds, max = 30.seconds, jitter = 0.2), // maxAttempts = null: forever
            logger = object : MqttLogger { // numbers and codes only, no topics, no addresses
                override fun reconnectIn(delayMs: Long) = println("retry in $delayMs ms")
            },
        )
    )
    watch(client)
    client.subscriptions.set("devices", setOf("devices/+/state"), Qos.AT_LEAST_ONCE) // restored after every reconnect
    client.start() // does not wait; connect() would wait for the first CONNACK
    return client // call client.close() when you are done
}
```

Details: [Reconnection](#reconnection), [Incoming messages and sessions](#incoming-messages-and-sessions).

#### Persistent session and `resend`

```kotlin
val client = MqttClient(
    MqttClientOptions(
        host = "broker.example.org",
        port = 1883,
        clientId = "kitchen-panel-1", // required: the broker finds the session by it
        persistentSession = true, // Clean Start = 0, unacknowledged QoS 1/2 survive a reconnect
        sessionExpiry = 10.minutes, // MQTT 5 Session Expiry Interval
    )
)
client.start()

// An ordinary value: if the connection breaks before the PUBACK it is re-sent (DUP = 1) after the reconnect,
// but only while the ttl (counted from this call) has not passed
val state = client.publish(
    "home/kitchen/panel", "online".encodeToByteArray(), Qos.AT_LEAST_ONCE,
    retain = true, ttl = 1.minutes, until = PublishStage.Delivered,
)

// A command with a side effect: never written twice. If the connection breaks before the PUBACK, you get
// Failed(ConnectionLost) at once and decide yourself (is it still wanted? ask the user again?)
val command = client.publish(
    "home/boiler/set", "on".encodeToByteArray(), Qos.AT_LEAST_ONCE,
    ttl = 5.seconds, until = PublishStage.Delivered, resend = false,
)
```

`Failed(SessionLost)` tells that the broker no longer has the session (it expired or the broker restarted): nothing is
re-sent. The state is kept in the memory of the client instance only. More in [Persistent session](#persistent-session).

#### WebSocket

```kotlin
val client = MqttClient(
    MqttClientOptions(
        host = "broker.example.org",
        port = 8083, // the WebSocket listener of the broker
        webSocket = MqttWebSocket(path = "/mqtt"), // ws://. Add tls = TLSClientSettings(...) for wss://
    )
)
```

Everything else (subscriptions, publishing, reconnection) works as over TCP. More in [WebSocket (JVM)](#websocket-jvm)
and [WebSocket on JS](#websocket-on-js-nodejs-and-browsers).

#### Android and Kotlin Multiplatform

On Android the client is the JVM one: the dependency goes to `commonMain` (or to the Android module) exactly as in
[Getting started](#getting-started-with-the-coroutine-client), and the app needs `android.permission.INTERNET`. Only
the platform factory `MqttClient(options)` can use TLS and WebSocket, and it is **not** an `expect`/`actual` function:
it is a separate function in `jvmMain`, `jsMain`, `mingwX64Main`, `linuxMain` and `appleTlsMain` (all Apple targets), so common code cannot call it. The common function
`MqttClient(options, host)` needs a host and refuses `tls` and `webSocket`. Wrap the platform factory yourself:

```kotlin
// commonMain
expect fun createMqttClient(options: MqttClientOptions): MqttClient

// jvmMain and androidMain
actual fun createMqttClient(options: MqttClientOptions): MqttClient = MqttClient(options)

// mingwX64Main, linuxMain and the Apple source sets (TCP and TLS) and jsMain (Node.js; WebSocket only in a browser) look the same
```

Create one client per broker for the whole life of the app (an application scope or a service), collect `incoming`
once in a long-lived scope, and call `close()` when the scope ends. The TLS code of the client does not use
`java.util.Base64`, which Android has only from API 26 (`minSdk` 24 is supported).

### Publishing

`publish(topic, payload, qos = AT_LEAST_ONCE, retain = false, ttl, until = Sent, resend = true)` suspends until the requested stage and
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
- `sessionResumeWindow` (optional, for MQTT 3.1.1): resume only if the previous connection of this client ended at most
  this long ago; otherwise, and for the first connection of a client instance, CONNECT goes with a clean start and the
  broker drops what it queued (Mosquitto keeps 3.1.1 sessions forever by default). Failed attempts without a CONNACK do
  not move the clock.
- Subscriptions are still subscribed again after every CONNACK; filters removed while offline are unsubscribed on resume.
  While offline the broker may queue QoS 1/2 messages of the session's subscriptions and deliver them after the reconnect.
- The state lives in the memory of the client instance: a new `MqttClient` or a restarted process starts without it (the
  broker may still report Session Present = 1, then there is simply nothing to re-send).
- QoS 1 is "at least once": a re-sent publication may reach subscribers twice when the first copy had arrived.
- Incoming QoS 2 messages are delivered exactly once across a resumed session too: the client remembers the packet
  identifiers it answered with PUBREC and applies them while parsing the CONNACK, so a PUBLISH (DUP = 1) or PUBREL that
  the broker re-sends right behind the CONNACK (Mosquitto does, often in the same read) is recognised.
- How to check: `PersistentSessionBrokerTest` (embedded broker, a proxy cuts the connection), `EnginePersistentSessionTest` and
  `MqttSessionCoreResumeTest` (virtual time) in `kmqtt-client/src/jvmTest`.

### Subscriptions

Subscriptions are declared in named **groups**; the desired set is the union of all groups and the QoS of a filter is the
maximum over its groups.

```kotlin
val subs = client.subscriptions

// replace the set of a group (an empty set removes the group)
subs.set("sensors", setOf("sensors/+/temperature"), Qos.AT_LEAST_ONCE) // qos defaults to AT_MOST_ONCE
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
subs.set("sensors", setOf("sensors/+/temperature"), Qos.AT_LEAST_ONCE, refreshShared = true) // same, for filters another group already holds
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
- **Filters are checked before they are sent**. A filter that is not a valid MQTT topic filter is never sent
  to the broker: empty, containing U+0000, longer than 65 535 bytes in UTF-8, `#` that is not a whole last level, `+`
  that is not a whole level, a malformed shared subscription `$share/<name>/<filter>` (the name must not be empty or
  contain `/`, `+`, `#`, and a valid filter must follow). Such a filter at once gets
  `TopicState.Refused(ReasonCode.TOPIC_FILTER_INVALID)` and one event on `refusals` for its group, while the other
  filters of the group are subscribed as usual. It does not count towards `MqttLimits.maxFilters`, and the check does
  not depend on the protocol version. Checked by `EngineSubscriptionsTest` and `TopicFilterTest`.
- `set(..., refreshShared = true)` subscribes the filters of the group once more even when another group already holds
  them, so that the broker sends their retained messages again. It is the group-level form of `resubscribe`.

### Incoming messages and sessions

`client.incoming` is a `Flow<MqttIncoming>` that delivers messages strictly in order of receipt, with boundaries:
`SessionStarted(epoch)`, then the `Message`s of that session, then `SessionEnded(epoch, error)`.

- Epochs grow over the whole process: a newer connection always has a larger epoch. Use them to tell "this value came
  from the current connection" from "this is a leftover of a dead one".
- By default epochs come from a counter shared by the process. `MqttClientOptions(epochSource = { ... })` takes them
  from your own counter instead (called once per CONNACK, on the client's I/O thread; return growing values above zero
  and do not throw), so the epoch can be your application's session number.
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
- Every event is also reported to `MqttLogger`, see below.

#### `MqttLogger`

`MqttClientOptions.logger` (default `MqttLogger.None`) receives diagnostic events. The events carry numbers, codes and
`MqttError` values only: never topics, filters, payloads, addresses or passwords. The methods are called on the I/O
thread of the client, so an implementation must be fast and must not block; an exception thrown by it is swallowed.
Every method is optional.

| Method | When |
|---|---|
| `connected(epoch)` | CONNACK received, a session began. |
| `disconnected(error)` | The connection ended (`error == null`: orderly close). |
| `reconnectIn(delayMs)` | The next attempt is scheduled. |
| `subscribe(count)` | A SUBSCRIBE for `count` filters was sent. |
| `refused(count)` | The broker refused `count` filters in a SUBACK (a filter refused locally as invalid is not counted here; it shows in `refusals` and `TopicState`). |
| `dropped(reason)` | An incoming packet was dropped without ending the session: `DropReason.TooLarge(size, limit)` or `TopicTooLong(length, limit)`. |
| `publishRefused(code)` | A PUBACK of MQTT 5 carried the failure code. |
| `resent(count)` | A resumed persistent session re-sent `count` unacknowledged publications. |
| `groupRejected(filters, limit)` | `subscriptions.set` was refused for `maxFilters`. |
| `packet(direction, type, size)` | Every MQTT packet: direction, control packet type, size in bytes, nothing else. An outgoing one is reported when it is handed to the I/O host for writing, an incoming one when its last byte has been read (also a packet that is then dropped as too large). |
| `incomingBuffer(depth, capacity)` | Back pressure: the number of messages waiting for the `incoming` collector reached 50, 75 or 100 % of `MqttLimits.incomingBuffer` (once per climb), and once more when it fell back to 25 % or less. `depth` can exceed `capacity` while reading is paused. A fall is reported at the next I/O step, not at the moment the collector took the message. |
| `readPaused(depth, capacity)` | Reading from the broker was paused because the buffer is full. Always followed by `readResumed`, unless the connection ends during the pause. |
| `readResumed(pausedMs)` | Reading went on after a pause of `pausedMs` milliseconds. |

The back pressure events show a slow collector before it becomes `ConsumerStalled`: a growing `incomingBuffer` level and
long pauses mean that the consumer should be faster. Checked by `MqttClientFacadeTest` (back pressure events) and
`PacketLogTest` (`packet`).

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

- TLS is available **only** through the platform factory `MqttClient(options)`: of the JVM (this section), of Node.js,
  mingwX64 and Linux (see below). The common factory `MqttClient(options, host, clock)` throws
  `IllegalArgumentException` when `tls != null`: a host given from outside is not known to apply TLS, and the client
  never falls back to plain text.
- **PEM text or a path**. `serverCertificate`, `clientCertificate` and `clientCertificateKey` take either
  the PEM text or a path to a PEM file. A string that contains `-----BEGIN ` is **always** PEM text and is never opened
  as a file, even if it looks like a path; any other string is a path. Indents, CRLF and line wrapping of any width
  (64, 76, ...) are accepted. A string that is not valid PEM (text before `BEGIN` or after `END`, no matching `END`, a
  second block, characters outside base64, an empty body) is refused without touching the file system. The client key
  must be PKCS#8 (`BEGIN PRIVATE KEY`, RSA or EC). Checked by `PemInputTest` and `TlsTransportTest`.
- **One `SSLContext` per client**. Certificates and keys are parsed and the system trust store is loaded once,
  at the first connection, and every reconnect reuses the result; this matters on slow devices that reconnect in a
  loop. A settings error is not remembered: the next attempt tries again (for example when the certificate file has
  appeared). Checked by `TlsTransportTest`.
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
  the certificate are not exposed. **On the JVM** a refusal because of the server certificate is told apart by `kind` only
  (never by text): `"ssl-hostname"` (the host name does not match the certificate), `"ssl-trust"` (no trusted
  path to the pinned certificate or the system trust store), `"ssl-expired"` (the certificate is expired or not yet
  valid; a pinned certificate is a trust anchor, its validity is not checked). When several apply, the order is: expired,
  host name, trust. Other handshake failures stay `"ssl"`. Wrong TLS settings (a file that cannot be read, a PEM that
  does not parse, a key that cannot be read) are `"ssl-config"` and no socket is opened. **On Node.js** only `"ssl"` and
  `"ssl-config"` exist; mingwX64 and Linux have all the kinds, as on the JVM (see below). Checked by `TlsTransportTest` (kinds, the expired chain
  `expired-chain.p12` / `expired-ca.pem` in `src/jvmTest/resources/tls`) and `TlsBrokerTest`.
- `TLSClientSettings.toString()` prints the client key and password. Never log it; `MqttClientOptions.toString()` is safe.
- **TLS on Node.js.** The factory of the `js` target builds the transport on the Node `tls` module with the same
  `TLSClientSettings`: the chain is checked against the system roots or, with `serverCertificate`, against that
  certificate only; the host name is checked unless `verifyHostname = false`; TLS 1.2 and above; SNI (the host name,
  not an IP address, or `serverNameIndications`), ALPN and a client certificate work. Other connection errors
  carry the Node error code in `Io.kind` (`ECONNREFUSED`, `ECONNRESET`, ...). Checked by `NodeTlsTransportTest`
  (`./gradlew :kmqtt-client:jsNodeTest`).

### TLS on mingwX64, Linux and Apple (OpenSSL)

With `tls != null` the `MqttClient(options)` factories of mingwX64, linuxX64, linuxArm64 and the Apple targets build the host with a TLS
transport (`WinSockTransportFactory.tls`, `PosixTransportFactory.tls`): an OpenSSL session works over memory
(memory BIO) and the ciphertext goes through the same non-blocking socket of the host. The same `TLSClientSettings` and
the same rules as on the JVM:

- TLS 1.2 and 1.3 only; SNI is the host name (not sent for an IP address) or `serverNameIndications`; ALPN is
  `alpnProtocols`;
- `serverCertificate` (PEM text or a path, as above) is the **only** trust anchor; the system store is not used then. A
  self-signed certificate, a CA or a leaf under a CA can be pinned (a PKIX anchor as on the JVM; OpenSSL flag
  `X509_V_FLAG_PARTIAL_CHAIN`). The validity of the anchor is not checked (an expired pinned certificate is accepted);
  the validity of a leaf under a pinned CA is;
- without a pin, the system roots: the Windows `ROOT` store (CryptoAPI); on Linux the distribution bundle
  (`/etc/ssl/certs/ca-certificates.crt`, `/etc/pki/tls/certs/ca-bundle.crt` and others, the directory `/etc/ssl/certs`);
  on macOS the bundle `/etc/ssl/cert.pem`; plus `SSL_CERT_FILE`/`SSL_CERT_DIR`. OpenSSL does not see the system trust
  store of Apple platforms (Keychain), and iOS, tvOS and watchOS have no file bundle, so there pin `serverCertificate`
  (otherwise the connection fails with `"ssl-trust"`);
- the host name is verified when `checkServerCertificate && verifyHostname` (the default): an IP literal against the IP
  SANs, a name against the DNS SANs (no partial wildcards like `a*.example`); with `serverNameIndications` that name is
  checked. "Trust all" with `verifyHostname = true` is refused by the factory, as on the JVM;
- the client certificate and key are PEM text or paths; the key may be encrypted with `clientCertificatePassword` (the
  password is wiped from C memory after parsing). OpenSSL also accepts PEM key formats other than PKCS#8;
- the `MqttError.Io.kind` values are `"ssl-hostname"`, `"ssl-trust"`, `"ssl-expired"`, `"ssl"` and `"ssl-config"` (from
  the OpenSSL verification code; OpenSSL error texts are never passed on). Unlike the JVM, OpenSSL checks the name before
  validity, so a certificate that is both expired and issued for another name gives `"ssl-hostname"`;
- the OpenSSL context (`SSL_CTX`, parsing the certificates and the system roots) is built per connection, not once per
  client as the JVM `SSLContext`; a settings error is not remembered;
- OpenSSL is the static **3.5 LTS** build (3.5.9, sha256 `603f5602e2eef00d77fbd429d34dcd5822bb301757a1bc9cdb24c670f1eb859a`
  of the tarball is pinned in the build script) from the klibs in `kmqtt-common/src/nativeInterop` (the legacy client uses it
  too). To rebuild the klibs see `native-build/`: linuxX64/linuxArm64/mingwX64 — Docker (`build-linux-mingw.sh`), Apple targets —
  `build-apple.sh` on a Mac with full Xcode. All klibs in the repo are 3.5.9; the Apple ones were built on
  2026-10-21 (Apple clang 17, Xcode 26.3, Kotlin/Native 2.4.20) with the build date, home directory, user name and
  archive uid/gid removed. The cinterop description is
  `kmqtt-common/src/nativeInterop/openssl.def` (paths point to the build tree, no machine-specific absolute paths).

Checked by `NativeTlsSessionTest` and `NativeTlsTransportTest` (an in-memory OpenSSL server, no network: trust, host
name, validity, SNI, client key, TLS below 1.2, close_notify, EOF, back pressure, handshake deadline) and
`NativeTlsTestbedTest` (the facade against a Mosquitto TLS broker; skipped without `KMQTT_TLS_A`, `KMQTT_TLS_A_BADHOST`,
`KMQTT_TLS_B` and `KMQTT_TLS_CERTS`, described in the KDoc of the test). Run with `./gradlew :kmqtt-client:mingwX64Test` on
Windows, `linuxX64Test` on Linux and `macosArm64Test`/`macosX64Test` on macOS. The same TLS layer is compiled into all
Apple targets (with the Apple OpenSSL klibs, 3.5.9); it runs on Apple hardware only and is checked by the macOS
tests, not on Windows or Linux.

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
  reported as `MqttError.Io` with the same `kind` values as plain TLS (`"ssl"`, `"ssl-hostname"`, `"ssl-trust"`, `"ssl-expired"`).
- Like TLS, WebSocket is available **only** through the platform factories `MqttClient(options)` of the JVM and JS
  (see [WebSocket on JS](#websocket-on-js-nodejs-and-browsers)); the common factory `MqttClient(options, host, clock)`
  throws `IllegalArgumentException` when `webSocket != null`.
- A refused upgrade or a protocol violation of the server (a masked or text frame, reserved bits, a broken fragment
  sequence, ...) is `MqttError.Io(kind = "websocket")`. The message shows the HTTP status code at most, never the reason
  phrase, headers, body or the address.
- `MqttWebSocket.toString()` and `MqttClientOptions.toString()` show neither the path (a query string may carry a
  token) nor the header values. The client sets `Host`, `Upgrade`, `Connection` and the `Sec-WebSocket-*` headers itself;
  they cannot be given in `headers`. The response must select the `mqtt` subprotocol or none, and no extension.
- Closing: the client sends a WebSocket close frame after the last data (after DISCONNECT) and waits for the server's
  close frame or end of stream (bounded by the same flush allowance of about a second) before closing the socket. A close
  frame of the server is answered with the same code and ends the stream like a TCP FIN.

### WebSocket on JS (Node.js and browsers)

The JS factory `MqttClient(options)` takes the same `webSocket` option:

- **Node.js** (`NodeHost`): the same RFC 6455 handling as on the JVM, over the `net` socket (`ws://`) or the `tls`
  socket (`wss://`, the same `TLSClientSettings` and checks as [TLS on Node.js](#tls-jvm)): extra upgrade headers,
  one masked binary frame per write, streamed incoming frames, pings answered, the close frame after DISCONNECT, errors
  `MqttError.Io(kind = "websocket")` with the HTTP status code at most. Errors of the socket below keep their kinds
  (`UnknownHost`, `"ssl"`, `"ECONNREFUSED"`, ...). For a custom host: `NodeTransportFactory.webSocket(...)`.
- **Browser**: where Node's `process.versions.node` is missing, `MqttClient(options)` is `BrowserMqttClient(options)`:
  the browser host runs on the event loop and the transport uses the runtime's `WebSocket` API (browsers, Node.js 22+,
  Deno; `BrowserMqttClient` can be called directly in any of them). A browser has no raw sockets, so `webSocket` is
  required. What the API cannot do is refused with `IllegalArgumentException`, never ignored: extra headers (put a
  token into the path's query string or the MQTT password), and any TLS setting except the defaults (a pinned or own
  certificate, a client certificate, ALPN, SNI, `checkServerCertificate = false`, `verifyHostname = false`); the
  browser checks the certificate chain and the host name itself. A failed connection is `MqttError.Io(kind =
  "websocket")` with the close code: the browser does not tell an unknown host from a refused connection or a TLS
  failure. There is no read pause (incoming messages wait in the transport while the client pauses reading) and no
  half-close (after the server's close frame nothing more is written); an unclean close (code 1006 and others) is
  `MqttError.Io(kind = "websocket")`, a clean one ends the stream.

```kotlin
// jsMain, browser
val client = MqttClient(
    MqttClientOptions(
        host = "broker.example.org",
        port = 8084,
        webSocket = MqttWebSocket(path = "/mqtt"), // no headers in a browser
        tls = TLSClientSettings(), // wss:// with the browser's checks; omit for ws://
    )
)
```

Checked on Node.js only (`jsNodeTest`: `NodeWebSocketHostContractTest`, `BrowserHostContractTest` — the browser host on
Node's own WHATWG `WebSocket` —, `JsWebSocketTransportTest`); the fork has no browser test runner (JS target builds
Node.js only, Karma is not wired), so a real-browser test is a documented skip. The legacy code of `kmqtt-common` and
`kmqtt-client` imports `node:*` modules at load time, so a browser bundle has to stub them; ready-made stubs and a
webpack 5 `resolve.fallback` config ship in [`js/browser-stubs`](js/browser-stubs/README.md), and
`js/browser-stubs/smoke-node-loader.js` checks the load browser-style (stubbed `node:*`, no
`process.versions.node`) without a browser. The new browser host itself calls no Node API.

### Enhanced authentication (MQTT 5)

```kotlin
val client = MqttClient(
    MqttClientOptions(
        host = "broker.example.org",
        port = 8883,
        tls = TLSClientSettings(serverCertificate = "broker.pem"),
        enhancedAuth = MqttEnhancedAuth(
            method = "SCRAM-SHA-256",
            initialData = { scram.clientFirst() }, // called for every connection: a fresh nonce each time
            respond = { method, data -> scram.answer(data) }, // suspend; throw to refuse
        ).verifyingConnack { data -> scram.verifyServerFinal(data) }, // optional: check the broker's signature in CONNACK
    )
)
```

- CONNECT carries the Authentication Method and the data of `initialData` (none when it returns `null`). Each AUTH of
  the broker with "continue authentication" (0x18) calls `respond(method, data)`; its result goes back in AUTH 0x18 (the
  bytes as Authentication Data, `null` for an AUTH without data). The exchange may take any number of steps and ends
  with CONNACK; it runs again on every reconnect.
- `initialData` runs on the I/O thread, so it must be fast. `respond` runs in a coroutine on `Dispatchers.Default`,
  never on the I/O thread; switch the dispatcher yourself for blocking work.
- Outcomes: a refusal of the broker (CONNACK `NOT_AUTHORIZED`, `BAD_AUTHENTICATION_METHOD`, ...) is
  `MqttError.Refused(code)`. When `respond` throws, the client sends DISCONNECT `NOT_AUTHORIZED` and the attempt ends
  with `MqttError.Refused(NOT_AUTHORIZED)`; when `initialData` throws, nothing is sent and the outcome is the same. The
  text of the exception is never stored or reported.
  The whole exchange is bounded by `connackTimeout` from CONNECT: a silent broker gives `MqttError.ConnackTimeout`, a
  `respond` that has not answered in time gives `Refused(NOT_AUTHORIZED)` and its late answer is dropped. An answer
  computed for a connection that broke meanwhile is never sent on the next one. The client then reconnects by its
  `ReconnectPolicy`, as after any refusal.
- A successful CONNACK must carry the same Authentication Method, otherwise the connection ends with
  `MqttError.Protocol(PROTOCOL_ERROR)`; so do an AUTH with another method or reason code, and an AUTH after CONNACK.
- MQTT 3.1.1 has no AUTH packet: `MqttClientOptions(version = MQTT3_1_1, enhancedAuth = ...)` throws
  `IllegalArgumentException`.
- Authenticity of the broker: `MqttEnhancedAuth(...).verifyingConnack { data -> scram.verifyServerFinal(data) }` checks
  the Authentication Data of a successful CONNACK (for example the SCRAM server signature; `data` is `null` when the
  broker sent none). On `false` or an exception the client sends DISCONNECT `NOT_AUTHORIZED`, the attempt ends with
  `MqttError.Refused(NOT_AUTHORIZED)` and the connection is never reported as established. So `Refused(NOT_AUTHORIZED)`
  can also mean a refusal of the client itself (the provider threw, did not answer in time, or the broker did not
  verify). Without `verifyingConnack` the data are ignored and the exchange does not authenticate the broker; rely on
  TLS for that.
- Re-authentication during a session (MQTT 5 §4.12.1): `client.reauthenticate()` starts a new exchange on the live
  connection — AUTH with "re-authenticate" (0x19), the same method and fresh data of `initialData`. The broker's
  challenges call `respond` as usual; its final AUTH with "success" (0x00) ends the exchange (its Authentication Data
  are checked by `verifyingConnack`, like the CONNACK data) and the call returns `true`. It returns `false` when there
  is no live session, another re-authentication is still running, or the connection ended first. The exchange is
  bounded by `connackTimeout`. Only the client starts a re-authentication: an unsolicited AUTH of the broker after
  CONNACK ends the connection with `MqttError.Protocol(PROTOCOL_ERROR)`. Without calls nothing is sent.
- `MqttEnhancedAuth.toString()` and `MqttClientOptions.toString()` show the method only, never the data.

### Closing

`close()` shuts the client down in an orderly way:

1. requests already in the queue (for example publications that clear retained messages) are sent first, in order.
   Publications held back by the window (the broker's Receive Maximum, or `MqttLimits.maxUnacknowledged`, 64 by default,
   with `persistentSession`) are written as the broker acknowledges the earlier ones, for at most
   `MqttClientOptions.closeDrain` (default 3 s); with nothing queued this step takes no time;
2. DISCONNECT is sent as soon as nothing is left to write (or `closeDrain` has run out);
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
- Publications that could not be sent get `Failed(Closed)`, including those still held back by the window when
  `closeDrain` ran out (`closeDrain = 0`: everything beyond the window). Publications made after `close()` fail with
  `Failed(Closed)` at once, and no SUBSCRIBE or UNSUBSCRIBE is written after it.
- Closing before CONNACK arrives closes the transport without DISCONNECT, so a broker that has already accepted CONNECT
  may publish the will.
- `close()` cannot hang: the total time is bounded by `closeDrain` plus `closeLinger` plus about 3 seconds, then the
  thread is stopped forcibly. A second call waits for the same shutdown.
- Error paths without anything to send (broken connection, keep-alive timeout, stalled consumer) close the socket at
  once. A protocol error found by the client on MQTT 5 is answered with `DISCONNECT` carrying the reason code: that
  packet is written first (within about a second, like the tail of an orderly close), then the socket is closed without
  waiting for the broker.

### Limitations

- **Re-authentication is client-initiated** (`client.reauthenticate()`, MQTT 5 §4.12.1): the client sends AUTH 0x19,
  an unsolicited AUTH of the broker after CONNACK is a protocol error, see
  [Enhanced authentication](#enhanced-authentication-mqtt-5). The Authentication Data of the final CONNACK and of the
  broker's success AUTH can be checked with `verifyingConnack`.
- **Persistent session is opt-in** (`persistentSession`, see [Persistent session](#persistent-session)); its state is kept
  in memory only. A QoS 2 publication dropped on resume (time-to-live passed, `resend = false`) may leave its packet
  identifier held by a broker that already sent PUBREC (Mosquitto 2.1.2 accepts a new publication with that identifier
  as a new one). Topic aliases are not used.
- **QoS 2** (publishing and receiving, including the resumed session) is implemented and tested: the protocol core and
  the engine on virtual time (`MqttSessionCoreTest`, `MqttSessionCoreResumeTest`, `EnginePersistentSessionTest`), and
  against a real Mosquitto 2.1.2 by an application that uses this fork (the full PUBLISH-PUBREC-PUBREL-PUBCOMP exchange and duplicates when each of the four
  packets is lost, a re-sent PUBLISH or PUBREL arriving in one read with the CONNACK, PUBREL of an unknown packet
  identifier, a busy packet identifier; MQTT 5 and 3.1.1). The tests of
  the fork itself with the embedded broker mostly use QoS 0 and 1.
- **I/O hosts for the JVM, Node.js, browsers (WebSocket only), mingwX64, Linux and Apple** (see **Targets** above).
  The Apple host is the POSIX host of Linux (`PosixHost`, BSD sockets and `poll`, `SO_NOSIGPIPE`); it is not
  Network.framework, so the cellular radio and VPN on demand are not brought up by it. It runs only on Apple hardware:
  its tests (`macosArm64Test`, `macosX64Test` and, with Xcode, simulator tests (`iosSimulatorArm64Test`, `iosX64Test`, `tvosSimulatorArm64Test`, `tvosX64Test`, `watchosSimulatorArm64Test`, `watchosX64Test`)) must be run on a Mac,
  on other machines the Apple targets are compiled only, and the device targets are compiled only everywhere. TLS exists on the JVM, Node.js, mingwX64, Linux
  and Apple (OpenSSL 3.5 LTS; the Apple
  klibs are 3.5.9 as well); in a
  browser only `wss://` with the browser's own checks. WebSocket exists on the JVM, Node.js and in browsers, not on
  mingwX64, Linux and Apple. The browser host is tested on Node's WHATWG `WebSocket`, not in a real browser. On Node.js under
  Windows the operating system accepts a write of any size at
  once (libuv writes through IOCP), so the pending-write figure stays 0 and `closeLinger`/flush time is spent by the
  kernel, not by the host. A custom host implements `MqttHost` (plus `HostDriver`, `MqttTransport`, ...), which is
  experimental API marked `@MqttCoreApi` and may change; the contract suite `MqttHostContractTest` in `commonTest` is
  the yardstick for a new one.
- **Addresses on native** (mingwX64, Linux and the other native targets): names are resolved with
  `AF_UNSPEC`, an IPv4 address is preferred, IPv6 is used when the name has no IPv4 address or is an IPv6 literal
  (`::1`, also in brackets, `[::1]`); the socket family follows the address. This holds for the `WinSockHost` and
  `PosixHost` hosts and for the legacy `MQTTClient` sockets (`ClientSocket`, `TLSClientSocket`). The native broker
  now listens on `Broker.host` (default `0.0.0.0`, all IPv4 addresses as before; `::` means all IPv6 and IPv4
  addresses, `IPV6_V6ONLY` is cleared explicitly). UDP (MQTT-SN) and the outgoing cluster connections of the native
  broker stay IPv4 only. There is no fallback to the next address of a name: the first preferred one is used.
- The `clock` parameter of the common factory is accepted but the time comes from the host loop.
- A client that closes itself keeps its thread until `close()` is called.

### Security

- Passwords, authentication data, the payload and topic of the will, topics, filters and addresses never appear in `toString()` of the public
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
  `webSocketPort` (with `TLSSettings` that port speaks `wss`), see `WebSocketBrokerTest.kt`. Enhanced authentication
  tests give the broker `enhancedAuthenticationProviders`, see `EnhancedAuthBrokerTest.kt`.
- Run the module's tests from the fork's root with `./gradlew :kmqtt-client:jvmTest` (one class: `--tests '*ReadmeSnippetsTest'`) (when the fork is included in a larger
  build, the path is prefixed by the included build, for example `:kmqtt:kmqtt-client:jvmTest`). The host contract and
  the Node/browser/WinSock/POSIX hosts are tested by `jsNodeTest` (Gradle downloads Node.js), `mingwX64Test` (on Windows),
  `linuxX64Test` (on Linux x64; elsewhere it only compiles, `linuxArm64` is compiled only) and `macosArm64Test` /
  `macosX64Test` (on a Mac of that architecture) and the simulator tests (`iosSimulatorArm64Test`, `iosX64Test`, `tvosSimulatorArm64Test`, `tvosX64Test`, `watchosSimulatorArm64Test`, `watchosX64Test`) on a Mac with Xcode;
  elsewhere they only compile (for example `compileTestKotlinIosSimulatorArm64`), the device targets are compiled only.
- **Linux tests in Docker** (from Windows or macOS): start a throw-away `eclipse-temurin:21-jdk-jammy` container, copy the fork
  (mounted read-only) into it and run the fork's own `gradlew` (Kotlin/Native and the Gradle cache can stay in named volumes), for
  example:

  ```bash
  docker run --rm -v "$PWD:/src:ro" -v kn-gradle:/root/.gradle -v kn-konan:/root/.konan eclipse-temurin:21-jdk-jammy \
    bash -c 'apt-get update -qq && apt-get install -y -qq libncurses5 >/dev/null && mkdir /work && cp -r /src/. /work &&
      cd /work && sed -i "s/\r$//" gradlew && chmod +x gradlew &&
      ./gradlew --no-daemon --console=plain :kmqtt-common:linuxX64Test :kmqtt-client:linuxX64Test'
  ```

#### How to check what this document says

| Feature | Test (all in `kmqtt-client/src/jvmTest/kotlin/io/github/davidepianca98/client`, unless noted) |
|---|---|
| The examples of this document | `ReadmeSnippetsTest` |
| Publishing, results, offline queue | `MqttClientBrokerTest`, `MqttClientFacadeTest`, `core/MqttSessionCoreTest` |
| Subscription groups, refusals, invalid filters | `engine/EngineSubscriptionsTest`, `engine/TopicFilterTest` |
| Reconnection | `engine/EngineReconnectTest`, `MqttClientVirtualTest` |
| Back pressure, `ConsumerStalled`, `MqttLogger` events | `internal/IncomingGateTest`, `internal/IncomingGateStressTest`, `MqttClientFacadeTest` |
| Persistent session, `resend` | `PersistentSessionBrokerTest`, `engine/EnginePersistentSessionTest`, `core/MqttSessionCoreResumeTest` |
| QoS 2 | `core/MqttSessionCoreTest`, `core/MqttSessionCoreResumeTest` |
| TLS, pinned certificate, failure kinds, `SSLContext` cache | `TlsBrokerTest`, `TlsTransportTest` |
| PEM text or path | `commonTest/.../client/PemInputTest`, `TlsTransportTest` |
| WebSocket | `WebSocketBrokerTest`, `WebSocketTransportTest`; JS: `JsWebSocketTransportTest`, `NodeWebSocketHostContractTest`, `BrowserHostContractTest` |
| Enhanced authentication, `verifyingConnack`, re-authentication | `EnhancedAuthBrokerTest`, `core/MqttSessionCoreAuthTest`, `engine/EngineAuthTest`, `MqttClientReauthTest` |
| Orderly close | `GracefulCloseTest` |
| Hosts (JVM, Node.js, browser, mingwX64, Linux, Apple) | `NioActorHostContractTest`, `NioActorHostTest`; `jsTest`: `NodeHostContractTest`, `NodeHostTest`, `NodeTlsTransportTest`, `NodeWebSocketHostContractTest`, `BrowserHostContractTest`; `mingwX64Test`: `WinSockHostContractTest`, `WinSockHostTest`; `posixHostTest` (runs as `linuxX64Test` on Linux and `macosArm64Test`/`macosX64Test` on macOS): `PosixHostContractTest`, `PosixHostTest`, `Ipv6LoopbackTest`; TLS on mingwX64, Linux and Apple: `NativeTlsSessionTest`, `NativeTlsTransportTest`, `NativeTlsTestbedTest` |
| WebSocket of the embedded broker | `kmqtt-broker/src/jvmTest/.../integration/BrokerSocketFixesTest` |

### Differences from the blocking `MQTTClient`

| | `MQTTClient` | `MqttClient` |
|---|---|---|
| Threading | you call `run()`/`step()` and block a thread | own I/O thread, `suspend` API |
| Payload | `UByteArray` | `ByteArray` |
| Reconnection | manual | built in, with a policy |
| Subscriptions | `subscribe`/`unsubscribe` calls | declarative groups, restored after reconnect |
| Publication result | none | `Sent`/`Delivered`/`Expired`/`Failed` |
| Incoming | callback | ordered `Flow` with back pressure and session epochs |
| Hosts | every target | JVM (incl. Android), Node.js, browsers (WebSocket only), mingwX64, Linux, Apple |
| WebSocket | yes | yes (JVM, Node.js, browsers) |
| QoS 2, persistent session | session state in the client object | opt-in `persistentSession`, in memory |
| Enhanced auth | yes (with re-authentication) | yes (MQTT 5, re-authentication via `reauthenticate()`) |

## Legacy blocking client (`MQTTClient`)

> **Legacy.** This section describes the blocking `MQTTClient` (`io.github.davidepianca98.MQTTClient`). It is kept as it
> is (the broker's own tests still use it) and is not extended. New code on the JVM, Android, Node.js, Windows, Linux
> and Apple should use the [coroutine client](#coroutine-client-iogithubdavidepianca98clientmqttclient) described above
> (fork only).
>
> The Maven coordinates below are those of the upstream `1.0.0` release. In this fork's builds they are replaced by the
> fork's own projects, see [Getting started with the coroutine client](#getting-started-with-the-coroutine-client);
> the upstream `1.0.0` has a different API (no `autoInit`, `onUnsubscribed`, `init`).

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
The certificates and key must be in PEM format, and they can be either a path to the PEM file or the actual PEM string (on JVM and JS a string with `-----BEGIN ` in it is always taken as PEM text, never as a path; the native targets keep the older check). The password can be null if the private key has no protection.
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



## Getting Started with the broker

### Executables
You can download the executables for your platform under the release tab

#### Program Arguments

|     Argument      | Default Value |                                                       Description                                                       |
|:-----------------:|:-------------:|:-----------------------------------------------------------------------------------------------------------------------:|
|        -h         |    0.0.0.0    |                                         Interface address to bind the server to                                         |
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
    )
    broker.listen() // Blocking method, use step() if you don't want to block the thread
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
        properties = MQTT5Properties(),
        payload = "testPayload".encodeToByteArray().toUByteArray()
    )
    broker.listen() // Blocking method, use step() if you don't want to block the thread
}
```

#### WebSocket

Set the `webSocketPort` constructor parameter (the command line option `--wsp`) to accept MQTT over WebSocket on a
second port, next to the normal one. With `tlsSettings` that port speaks `wss`, without it `ws`.

```kotlin
fun main() {
    val broker = Broker(port = 1883, webSocketPort = 8083)
    broker.listen() // ws://host:8083: the client must offer the subprotocol mqtt; the request path is not checked
}
```

The WebSocket layer follows RFC 6455: the FIN bit and continuation frames are handled (a fragmented
message is joined into the MQTT byte stream), a pong is ignored, a close frame is answered with the same status code
and has the correct two-byte form `0x88 0x00` when the broker closes. A non-zero RSV bit, a continuation frame without a
started message, a data frame inside a fragmented message, and a fragmented or longer than 125 bytes control frame
close the connection with an error. Checked by `BrokerSocketFixesTest` (raw WebSocket and TCP clients) and, with the
coroutine client, by `WebSocketBrokerTest`.

#### Refused logins

When `authentication` (or the broker for another reason) refuses a CONNECT of an MQTT 3.1.1 client, the client gets a
CONNACK with the return code (not authorized, bad user name or password, unacceptable protocol version, identifier
rejected, server unavailable) instead of a dropped socket. MQTT 5 clients get CONNACK with the reason code
as before. Other 3.1.1 protocol errors still close the socket without CONNACK, as the specification says.

#### Stopping

`broker.stop(serverReference = null, temporarilyMoved = false)` sends DISCONNECT to the connected MQTT 5 clients
(`SERVER_SHUTTING_DOWN`; with `serverReference` set `SERVER_MOVED`, or `USE_ANOTHER_SERVER` when `temporarilyMoved` is
`true`) and closes the listeners. The stand-alone executable calls it from a shutdown hook. When you run the broker with
`step()` in your own thread (as the tests of the client do), call `stop()` from that thread too.

#### Other advanced functionality

MQTT5 Enhanced Authentication: set the `enhancedAuthenticationProviders` Broker constructor parameter, implementing the
provider interface `EnhancedAuthenticationProvider`.

Session persistence: set the `persistence` Broker constructor parameter, implementing `Persistence` interface. Without it the
sessions live in memory only: after a restart of the broker a client that resumes a persistent session gets Session Present = 0.

Bytes metrics: set the `bytesMetrics` Broker constructor parameter, implementing `BytesMetrics` interface.  
