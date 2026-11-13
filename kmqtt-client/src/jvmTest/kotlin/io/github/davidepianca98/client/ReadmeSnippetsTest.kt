package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import io.github.davidepianca98.socket.tls.TLSClientSettings
import io.github.davidepianca98.socket.tls.TLSSettings
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.File
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Примеры из `Readme.md` («Quick examples» нового клиента) выполняются против встроенного брокера форка.
 * Тело каждого теста повторяет пример из README; заменены только адрес и порт брокера (в README `broker.example.org`),
 * проверки добавлены после примера. Пример, который перестал компилироваться или работать, ломает этот класс, а не
 * читателя README. Сеть наружу не нужна: брокер на 127.0.0.1.
 */
internal class ReadmeSnippetsTest : BrokerTestBase() {

    private fun timed(limitMs: Long = 40_000, block: suspend CoroutineScope.() -> Unit) {
        runBlocking { withTimeout(limitMs, block) }
    }

    private val serverPem: String by lazy {
        (javaClass.classLoader.getResourceAsStream("tls/server.pem") ?: fail("нет ресурса tls/server.pem"))
            .use { it.readBytes().decodeToString() }.replace("\r\n", "\n")
    }

    private fun brokerTlsSettings(): TLSSettings {
        val file = File.createTempFile("kmqtt-test-server", ".p12")
        file.deleteOnExit()
        (javaClass.classLoader.getResourceAsStream("tls/server.p12") ?: fail("нет ресурса tls/server.p12"))
            .use { input -> file.outputStream().use { input.copyTo(it) } }
        return TLSSettings(keyStoreFilePath = file.absolutePath, keyStorePassword = "kmqtt-test")
    }

    /** «Minimal: connect, subscribe, receive, publish, close». */
    @Test
    fun minimalExample() = timed {
        val broker = broker()
        val printed = CopyOnWriteArrayList<String>()
        fun println(text: Any?) {
            printed += text.toString()
        }

        // ---- пример из README (адрес и порт заменены) ----
        val client = trackClient(MqttClient(MqttClientOptions(host = TestBroker.HOST, port = broker.port))) // MQTT 5, plain TCP
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
        // ---- конец примера ----

        assertTrue(printed[0].startsWith("connected, epoch"), printed.toString())
        assertEquals("demo/hello: hi", printed[1])
    }

    /** «Everything at once: groups, results, time-to-live». */
    @Test
    fun everythingAtOnceExample() = timed {
        val broker = broker()
        val printed = CopyOnWriteArrayList<String>()
        fun println(text: Any?) {
            printed += text.toString()
        }

        // ---- пример из README (адрес и порт заменены) ----
        val client = trackClient(
            MqttClient( // the JVM factory: MqttClient(options)
                MqttClientOptions(
                    host = TestBroker.HOST,
                    port = broker.port,
                    version = MQTTVersion.MQTT5,
                    credentials = MqttCredentials("user", "secret".encodeToByteArray()),
                )
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
        // ---- конец примера ----

        assertTrue(printed.any { it.startsWith("connected, epoch") }, printed.toString())
        assertTrue("acknowledged by the broker" in printed, printed.toString())
        assertTrue(printed.any { it.startsWith("session ") && it.endsWith("started") }, printed.toString())
    }

    /** «TLS with a pinned certificate» (в README путь к файлу; здесь тот же параметр, но текстом PEM). */
    @Test
    fun tlsPinnedCertificateExample() = timed {
        val broker = broker(tlsSettings = brokerTlsSettings())

        // ---- пример из README (адрес, порт и сертификат заменены; SAN сертификата теста — только localhost) ----
        val client = MqttClient(
            MqttClientOptions(
                host = "localhost",
                port = broker.port,
                tls = TLSClientSettings(
                    serverCertificate = serverPem, // pin one certificate: a path to a PEM file or the PEM text itself
                ),
            )
        )
        // ---- конец примера ----

        trackClient(client)
        assertEquals(MqttTransportKind.Tls, client.config.transport)
        assertIs<ConnectResult.Connected>(withTimeout(15_000) { client.connect() })
    }

    /** «Automatic reconnection and watching the state». */
    @Test
    fun reconnectAndWatchExample() = timed {
        val broker = broker()
        val printed = CopyOnWriteArrayList<String>()
        fun println(text: Any?) {
            printed += text.toString()
        }

        // ---- пример из README ----
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
                    host = TestBroker.HOST,
                    port = broker.port,
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
        // ---- конец примера ----

        val watcherScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val client = trackClient(watcherScope.startClient())
        BrokerTestKit.waitUntil(10_000, "клиент подключился") { client.connection.value is ConnectionState.Connected }
        val other = trackClient(MqttClient(options(broker.port)))
        assertIs<ConnectResult.Connected>(withTimeout(15_000) { other.connect() })
        client.subscriptions.awaitApplied("devices")
        other.publish("devices/lamp/state", "on".encodeToByteArray(), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        BrokerTestKit.waitUntil(10_000, { "сообщение в наблюдателе: $printed" }) {
            printed.any { it.endsWith("] devices/lamp/state") }
        }
        assertTrue(printed.any { it.startsWith("online, epoch") }, printed.toString())
        watcherScope.cancel()
    }

    /** «Persistent session and resend». */
    @Test
    fun persistentSessionAndResendExample() = timed {
        val broker = broker()

        // ---- пример из README (адрес и порт заменены) ----
        val client = MqttClient(
            MqttClientOptions(
                host = TestBroker.HOST,
                port = broker.port,
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
        // ---- конец примера ----

        trackClient(client)
        assertIs<PublishResult.Delivered>(state)
        assertIs<PublishResult.Delivered>(command)
        assertTrue(client.config.persistentSession)
    }

    /** «WebSocket». */
    @Test
    fun webSocketExample() = timed {
        val broker = broker(webSocket = true)

        // ---- пример из README (адрес и порт заменены) ----
        val client = MqttClient(
            MqttClientOptions(
                host = TestBroker.HOST,
                port = broker.webSocketPort!!, // the WebSocket listener of the broker
                webSocket = MqttWebSocket(path = "/mqtt"), // ws://. Add tls = TLSClientSettings(...) for wss://
            )
        )
        // ---- конец примера ----

        trackClient(client)
        assertEquals(MqttTransportKind.WebSocket, client.config.transport)
        assertIs<ConnectResult.Connected>(withTimeout(15_000) { client.connect() })
        assertEquals(
            PublishResult.Delivered((client.connection.value as ConnectionState.Connected).epoch),
            client.publish("ws/x", byteArrayOf(1), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        )
    }

    /** «Subscriptions»: недопустимый фильтр отвергается локально, допустимые подписываются. */
    @Test
    fun invalidFilterIsRefusedLocally() = timed {
        val broker = broker()
        val client = trackClient(MqttClient(options(broker.port)))
        val refusals = CopyOnWriteArrayList<SubscribeRefused>()
        // refusals — SharedFlow без повтора: сборщик должен работать до set
        val watcher = launch(start = CoroutineStart.UNDISPATCHED) { client.subscriptions.refusals.collect { refusals += it } }
        client.subscriptions.set("g", setOf("ok/#", "a/#/b"), Qos.AT_LEAST_ONCE)
        assertIs<ConnectResult.Connected>(withTimeout(15_000) { client.connect() })
        val applied = withTimeout(10_000) { client.subscriptions.awaitApplied("g") }
        assertEquals(TopicState.Granted(Qos.AT_LEAST_ONCE), applied["ok/#"])
        assertEquals(TopicState.Refused(ReasonCode.TOPIC_FILTER_INVALID), applied["a/#/b"])
        BrokerTestKit.waitUntil(5_000, "событие отказа") { refusals.isNotEmpty() }
        assertEquals(ReasonCode.TOPIC_FILTER_INVALID, refusals.single().code)
        watcher.cancel()
    }

    /** «Enhanced authentication»: объект собирается как в README (проверка с брокером — в `EnhancedAuthBrokerTest`). */
    @Test
    fun enhancedAuthWithVerifyingConnackExampleBuilds() {
        val scram = object {
            fun clientFirst(): ByteArray = "n,,n=user,r=nonce".encodeToByteArray()
            fun answer(data: ByteArray?): ByteArray = data ?: ByteArray(0)
            fun verifyServerFinal(data: ByteArray?): Boolean = data != null
        }

        // ---- фрагмент из README (адрес заменён) ----
        val options = MqttClientOptions(
            host = "broker.example.org",
            port = 8883,
            tls = TLSClientSettings(serverCertificate = serverPem),
            enhancedAuth = MqttEnhancedAuth(
                method = "SCRAM-SHA-256",
                initialData = { scram.clientFirst() }, // called for every connection: a fresh nonce each time
                respond = { _, data -> scram.answer(data) }, // suspend; throw to refuse
            ).verifyingConnack { data -> scram.verifyServerFinal(data) }, // optional: check the broker's signature in CONNACK
        )
        // ---- конец фрагмента ----

        assertEquals("MqttEnhancedAuth(method=SCRAM-SHA-256)", options.enhancedAuth.toString())
    }
}
