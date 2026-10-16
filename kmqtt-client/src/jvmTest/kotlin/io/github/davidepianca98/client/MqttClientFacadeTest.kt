package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.BrokerScript
import io.github.davidepianca98.client.internal.MqttClientImpl
import io.github.davidepianca98.mqtt.MQTTCurrentPacket
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.broker.Broker
import io.github.davidepianca98.mqtt.packets.MQTTPacket
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Connect
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Disconnect
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Publish
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import io.github.davidepianca98.socket.tls.TLSClientSettings
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Минимальные тесты фасада [MqttClient] (S06b-T05a): актор-клей на ручном хосте (шаги `runOnce` вызывает тест) и
 * сквозной сценарий через `NioActorHost` против брокера форка. Полный набор — `MqttClientVirtualTest` (T05b) и
 * `MqttClientBrokerTest`/`GracefulCloseTest` (T05c).
 */
class MqttClientFacadeTest {
    private val toClose = ArrayList<() -> Unit>()

    @AfterTest
    fun tearDown() {
        toClose.asReversed().forEach { runCatching { it() } }
    }

    /** Хост без потока: шаги драйвера вызывает тест. */
    private class ManualHost : MqttHost {
        var driver: HostDriver? = null
        val wakeups = AtomicInteger()

        @Volatile
        var closed = false

        override fun start(driver: HostDriver) {
            check(this.driver == null)
            this.driver = driver
        }

        override fun wakeup() {
            wakeups.incrementAndGet()
        }

        override suspend fun close(timeoutMs: Long): Boolean {
            if (!closed) {
                closed = true
                driver?.onStopped(null)
            }
            return true
        }

        fun step(now: Long, vararg events: TransportEvent): HostStep = driver!!.runOnce(now, events.toList())
    }

    private val script = BrokerScript(MQTTVersion.MQTT5)
    private val parser = MQTTCurrentPacket(16u * 1024u * 1024u, MQTTVersion.MQTT5)

    private fun written(step: HostStep): List<MQTTPacket> =
        step.actions.filterIsInstance<HostAction.Write>().flatMap { parser.addData(it.bytes.asUByteArray()) }

    private fun options(
        closeLinger: kotlin.time.Duration = 3.seconds,
        incomingBuffer: Int = 1024,
    ) = MqttClientOptions(
        host = "broker.local",
        port = 1883,
        clientId = "facade",
        keepAlive = 0.seconds,
        closeLinger = closeLinger,
        reconnect = ReconnectPolicy.Never,
        limits = MqttLimits(incomingBuffer = incomingBuffer),
    )

    /** Клиент на ручном хосте, подключённый (CONNACK получен) к моменту [connectedAt]. */
    private fun connected(options: MqttClientOptions, connectedAt: Long = 2): Pair<MqttClient, ManualHost> {
        val host = ManualHost()
        val client = MqttClient(options, host)
        client.start()
        assertIs<HostAction.Connect>(host.step(0).actions.single())
        written(host.step(1, TransportEvent.Connected)) // CONNECT
        host.step(connectedAt, TransportEvent.Bytes(script.connack()))
        assertIs<ConnectionState.Connected>(client.connection.value)
        return client to host
    }

    private fun awaitWakeup(host: ManualHost, before: Int) {
        val deadline = System.currentTimeMillis() + 5_000
        while (host.wakeups.get() == before) {
            check(System.currentTimeMillis() < deadline) { "no wakeup" }
            Thread.sleep(1)
        }
    }

    @Test
    fun disconnectTailIsNotDroppedWhenCloseFollowsWriteInOneStep() = runBlocking<Unit> {
        val (client, host) = connected(options(closeLinger = 0.seconds))
        val before = host.wakeups.get()
        val closing = async(Dispatchers.Default) { client.close() }
        awaitWakeup(host, before)

        // S06b-T05f: Write(DISCONNECT), затем «дописать и закрыть» в том же шаге; без придержания и срока 500 мс
        val disconnectStep = host.step(3)
        assertTrue(written(disconnectStep).single() is MQTT5Disconnect)
        assertIs<HostAction.Write>(disconnectStep.actions.first())
        assertEquals(HostAction.FlushAndClose(MqttClientImpl.CLOSE_FLUSH_MS), disconnectStep.actions.last())
        assertFalse(HostAction.CloseTransport in disconnectStep.actions, "CloseTransport would drop the write tail")
        assertFalse(HostAction.PauseRead(false) in disconnectStep.actions)

        host.step(4) // хост исполнил FlushAndClose — close() отпускается без ожидания EOF или срока
        withTimeout(5_000) { closing.await() }
        assertEquals(ConnectionState.Closed(null), client.connection.value)
        assertTrue(host.closed)
    }

    @Test
    fun errorPathWithoutWriteClosesTransportImmediately() = runBlocking<Unit> {
        // DISCONNECT брокера: ответной записи нет — остаток не дописывается, CloseTransport сразу
        val (client, host) = connected(options())
        val step = host.step(3, TransportEvent.Bytes(script.disconnect5(ReasonCode.SERVER_SHUTTING_DOWN)))
        assertTrue(written(step).isEmpty())
        assertEquals(listOf<HostAction>(HostAction.CloseTransport), step.actions)
        assertIs<ConnectionState.Closed>(client.connection.value)
        client.close()
    }

    @Test
    fun publishResultsSentDeliveredAndFailedClosedAfterClose() = runBlocking<Unit> {
        val (client, host) = connected(options())
        // Sent: PUBLISH отдан на запись
        val sent = async(Dispatchers.Default, CoroutineStart.UNDISPATCHED) { client.publish("a", byteArrayOf(1)) }
        val sentStep = host.step(3)
        assertIs<MQTT5Publish>(written(sentStep).single())
        assertIs<PublishResult.Sent>(withTimeout(5_000) { sent.await() })
        // Delivered: PUBACK
        val delivered = async(Dispatchers.Default, CoroutineStart.UNDISPATCHED) {
            client.publish("a", byteArrayOf(2), until = PublishStage.Delivered)
        }
        val packetId = (written(host.step(4)).single() as MQTT5Publish).packetId!!.toInt()
        host.step(5, TransportEvent.Bytes(script.puback(packetId)))
        assertIs<PublishResult.Delivered>(withTimeout(5_000) { delivered.await() })
        closeDriven(client, host, from = 6)
        assertEquals(PublishResult.Failed(PublishFailure.Closed), client.publish("a", byteArrayOf(3)))
    }

    /** Штатное закрытие на ручном хосте: DISCONNECT, EOF брокера, шаг после закрытия транспорта. */
    private suspend fun closeDriven(client: MqttClient, host: ManualHost, from: Long) = coroutineScope {
        val before = host.wakeups.get()
        val closing = async(Dispatchers.Default) { client.close() }
        awaitWakeup(host, before)
        val disconnect = host.step(from)
        assertTrue(written(disconnect).single() is MQTT5Disconnect)
        assertFalse(HostAction.CloseTransport in disconnect.actions, "linger: wait for broker EOF")
        assertTrue(disconnect.actions.none { it is HostAction.FlushAndClose }, "linger: wait for broker EOF")
        // Штатное закрытие — всегда «дописать и закрыть» (после EOF очередь пуста, хост закроет сразу)
        assertEquals(HostAction.FlushAndClose(MqttClientImpl.CLOSE_FLUSH_MS), host.step(from + 1, TransportEvent.Eof).actions.single())
        host.step(from + 2)
        withTimeout(5_000) { closing.await() }
        assertEquals(ConnectionState.Closed(null), client.connection.value)
    }

    @Test
    fun offlinePublishExpiresAndCancelledOneNeverReachesBroker() = runBlocking<Unit> {
        val host = ManualHost()
        val client = MqttClient(options(), host)
        val expiring = async(Dispatchers.Default, CoroutineStart.UNDISPATCHED) { client.publish("x", byteArrayOf(1), ttl = 100.milliseconds) }
        val cancelled = async(Dispatchers.Default, CoroutineStart.UNDISPATCHED) { client.publish("y", byteArrayOf(2), ttl = 10.seconds) }
        host.step(0) // хост запущен первой командой, публикации в оффлайн-очереди
        cancelled.cancelAndJoin()
        host.step(150)
        assertEquals(PublishResult.Expired, withTimeout(5_000) { expiring.await() })
        client.start()
        host.step(200)
        val connectStep = host.step(201, TransportEvent.Connected)
        written(connectStep)
        val afterConnack = host.step(202, TransportEvent.Bytes(script.connack()))
        assertTrue(written(afterConnack).none { it is MQTT5Publish }, "cancelled publish must not be sent")
        closeDriven(client, host, from = 203)
    }

    @Test
    fun fullIncomingBufferPausesReadingAndResumesAfterConsumerCatchesUp() = runBlocking<Unit> {
        val (client, host) = connected(options(incomingBuffer = 2))
        val burst = (0 until 5).map { script.publish("t", byteArrayOf(it.toByte())) }.reduce { a, b -> a + b }
        val burstStep = host.step(3, TransportEvent.Bytes(burst))
        assertTrue(HostAction.PauseRead(true) in burstStep.actions)

        val consumer = async(Dispatchers.Default) { client.incoming.take(6).toList() }
        val seen = ArrayList<HostAction>()
        var now = 4L
        val deadline = System.currentTimeMillis() + 5_000
        while (!consumer.isCompleted) {
            check(System.currentTimeMillis() < deadline) { "consumer stuck" }
            seen += host.step(now++).actions
            Thread.sleep(2)
        }
        val items = consumer.await()
        assertIs<MqttIncoming.SessionStarted>(items.first())
        assertEquals((0 until 5).toList(), items.drop(1).map { (it as MqttIncoming.Message).payload.single().toInt() })
        assertTrue(HostAction.PauseRead(false) in seen)
        assertFailsWith<IllegalStateException> { client.incoming.take(1).toList() }
    }

    @Test
    fun optionsToStringHasNoSecrets() {
        val options = MqttClientOptions(
            host = "secret-host.example",
            port = 8883,
            credentials = MqttCredentials("user-login", "pass-word".encodeToByteArray()),
            tls = TLSClientSettings(clientCertificateKey = "KEY-MATERIAL", clientCertificatePassword = "tls-pass"),
            will = MqttWill("will/topic", "will-payload".encodeToByteArray(), retain = true),
        )
        val text = options.toString()
        for (secret in listOf("secret-host", "8883", "user-login", "pass-word", "KEY-MATERIAL", "tls-pass", "will/topic", "will-payload")) {
            assertFalse(secret in text, "toString leaks $secret: $text")
        }
        assertTrue(options.verifyHostname)
    }

    @Test
    fun endToEndThroughNioHostAndForkBroker() = runBlocking<Unit> {
        val port = ServerSocket(0).use { it.localPort }
        val broker = Broker(port = port, host = "127.0.0.1")
        val stop = java.util.concurrent.atomic.AtomicBoolean(false)
        val worker = thread(name = "facade-test-broker", isDaemon = true) {
            while (!stop.get()) broker.step()
            broker.stop()
        }
        toClose += {
            stop.set(true)
            worker.join(5_000)
        }

        val client = MqttClient(MqttClientOptions(host = "127.0.0.1", port = port, keepAlive = 10.seconds), NioActorHost())
        toClose += { runBlocking { withTimeout(10_000) { client.close() } } }
        val incoming = async(Dispatchers.Default) { client.incoming.take(2).toList() }

        val connected = withTimeout(10_000) { client.connect() }
        assertIs<ConnectResult.Connected>(connected)
        client.subscriptions.set("g", setOf("facade/#"), Qos.AT_LEAST_ONCE)
        val applied = withTimeout(10_000) { client.subscriptions.awaitApplied("g") }
        assertEquals(mapOf<String, TopicState>("facade/#" to TopicState.Granted(Qos.AT_LEAST_ONCE)), applied)

        val result = withTimeout(10_000) {
            client.publish("facade/x", "hello".encodeToByteArray(), until = PublishStage.Delivered)
        }
        assertEquals(PublishResult.Delivered(connected.epoch), result)
        val items = withTimeout(10_000) { incoming.await() }
        assertEquals(MqttIncoming.SessionStarted(connected.epoch), items[0])
        val message = items[1] as MqttIncoming.Message
        assertEquals("facade/x", message.topic)
        assertEquals("hello", message.payload.decodeToString())

        val started = System.currentTimeMillis()
        withTimeout(10_000) { client.close() }
        assertTrue(System.currentTimeMillis() - started < 4_000, "close took too long")
        assertEquals(ConnectionState.Closed(null), client.connection.value)
        assertEquals(PublishResult.Failed(PublishFailure.Closed), client.publish("facade/x", byteArrayOf()))
        assertIs<ConnectResult.Failed>(client.connect())
        assertFailsWith<IllegalStateException> { client.subscriptions.awaitApplied("g") }
    }

    /**
     * S06b-T05f сквозь `NioActorHost`: сервер с `SO_RCVBUF` 4 КиБ не читает 4 МиБ публикации, `close()` при
     * `closeLinger = 0` — запись частичная. Сервер начинает читать через 550 мс (позже прежнего придержания 500 мс,
     * раньше срока дописывания) и получает PUBLISH и DISCONNECT целиком, затем EOF (не RST).
     */
    @Test
    fun zeroLingerCloseDeliversWholeDisconnectAfterPartialWriteThroughNioHost() = runBlocking<Unit> {
        val server = ServerSocket()
        server.receiveBufferSize = 4 * 1024
        server.bind(InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
        toClose += { server.close() }
        val packets = Collections.synchronizedList(ArrayList<String>())
        val ended = CompletableDeferred<String>()
        val gate = CountDownLatch(1)
        thread(isDaemon = true, name = "facade-test-raw-server") {
            try {
                server.accept().use { socket ->
                    socket.soTimeout = 10_000
                    val input = socket.getInputStream()
                    val serverParser = MQTTCurrentPacket(16u * 1024u * 1024u, MQTTVersion.MQTT5)
                    val buffer = ByteArray(64 * 1024)
                    var connackSent = false
                    var opened = false
                    while (true) {
                        if (connackSent && !opened) {
                            gate.await(10, TimeUnit.SECONDS)
                            socket.receiveBufferSize = 1 shl 20 // дочитать быстро, до срока дописывания
                            opened = true
                        }
                        val n = input.read(buffer)
                        if (n < 0) {
                            ended.complete("eof")
                            break
                        }
                        for (packet in serverParser.addData(buffer.copyOf(n).asUByteArray())) {
                            packets += packet::class.simpleName!!
                            if (packet is MQTT5Connect && !connackSent) {
                                socket.getOutputStream().write(script.connack())
                                connackSent = true
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                ended.complete(e.javaClass.simpleName)
            }
        }
        val options = MqttClientOptions(
            host = "127.0.0.1",
            port = server.localPort,
            keepAlive = 0.seconds,
            closeLinger = 0.seconds,
            reconnect = ReconnectPolicy.Never,
        )
        val client = MqttClient(options, NioActorHost())
        toClose += { runBlocking { withTimeout(10_000) { client.close() } } }
        val connected = assertIs<ConnectResult.Connected>(withTimeout(10_000) { client.connect() })
        val payload = ByteArray(4 * 1024 * 1024) { (it * 31 + 7).toByte() }
        assertEquals(
            PublishResult.Sent(connected.epoch),
            withTimeout(10_000) { client.publish("big", payload, qos = Qos.AT_MOST_ONCE) }
        )
        val started = System.currentTimeMillis()
        val closing = async(Dispatchers.Default) { client.close() }
        Thread.sleep(550)
        assertFalse(closing.isCompleted, "close() ждёт дописывания остатка")
        val opened = System.currentTimeMillis()
        gate.countDown()
        withTimeout(10_000) { closing.await() }
        val elapsed = System.currentTimeMillis() - started
        println("MqttClientFacadeTest: дочитано и закрыто через ${System.currentTimeMillis() - opened} мс после открытия, close() $elapsed мс")
        assertEquals("eof", withTimeout(10_000) { ended.await() }, "сервер видит штатный EOF, не RST")
        assertEquals(listOf("MQTT5Connect", "MQTT5Publish", "MQTT5Disconnect"), packets.toList())
        assertTrue(elapsed < MqttClientImpl.CLOSE_FLUSH_MS + 2_000, "close() $elapsed мс")
        assertEquals(ConnectionState.Closed(null), client.connection.value)
    }

    @Test
    fun awaitAppliedFailsWhenClientClosesItself() = runBlocking<Unit> {
        val (client, host) = connected(options())
        client.subscriptions.set("g", setOf("a/#"))
        val waiting = async(Dispatchers.Default, CoroutineStart.UNDISPATCHED) {
            runCatching { client.subscriptions.awaitApplied("g") }
        }
        assertTrue(written(host.step(3)).isNotEmpty()) // SUBSCRIBE
        host.step(4, TransportEvent.Bytes(script.disconnect5(ReasonCode.SERVER_SHUTTING_DOWN)))
        assertIs<ConnectionState.Closed>(client.connection.value)
        assertIs<IllegalStateException>(withTimeout(5_000) { waiting.await() }.exceptionOrNull())
        val late = async(Dispatchers.Default, CoroutineStart.UNDISPATCHED) {
            runCatching { client.subscriptions.awaitApplied("g") }
        }
        host.step(5)
        assertIs<IllegalStateException>(withTimeout(5_000) { late.await() }.exceptionOrNull())
    }

    @Test
    fun closeOfNeverStartedClientDoesNotHang() = runBlocking<Unit> {
        val host = ManualHost()
        val client = MqttClient(options(), host)
        withTimeout(1_000) { client.close() }
        assertEquals(ConnectionState.Closed(null), client.connection.value)
        assertEquals(null, host.driver)
    }
}
