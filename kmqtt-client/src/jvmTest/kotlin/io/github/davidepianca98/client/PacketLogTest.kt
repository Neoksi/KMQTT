package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.BrokerScript
import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.MQTTControlPacketType
import io.github.davidepianca98.mqtt.packets.MQTTControlPacketType.CONNACK
import io.github.davidepianca98.mqtt.packets.MQTTControlPacketType.CONNECT
import io.github.davidepianca98.mqtt.packets.MQTTControlPacketType.PUBACK
import io.github.davidepianca98.mqtt.packets.MQTTControlPacketType.PUBLISH
import io.github.davidepianca98.mqtt.packets.Qos
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * `MqttLogger.packet` (SEVEN-258, W6 §4.8): фасад шагается вручную ([HostDriver.runOnce] с заданными событиями
 * транспорта), чтобы порции байтов были ровно такими, как нужно тесту: несколько пакетов в одной порции, пакет по
 * байту, пакет больше лимита, разрез заголовка на обрыве соединения, несколько пакетов в одном `Write`.
 */
@OptIn(MqttCoreApi::class)
class PacketLogTest {
    private class StubHost : MqttHost {
        var driver: HostDriver? = null

        override fun start(driver: HostDriver) {
            this.driver = driver
        }

        override fun wakeup() {}

        override suspend fun close(timeoutMs: Long): Boolean = true
    }

    private data class Seen(val direction: PacketDirection, val type: MQTTControlPacketType, val size: Int)

    private class Recorder(private val fail: Boolean = false) : MqttLogger {
        val packets = ArrayList<Seen>()

        override fun packet(direction: PacketDirection, type: MQTTControlPacketType, size: Int) {
            packets += Seen(direction, type, size)
            if (fail) throw IllegalStateException("logger failure")
        }

        fun take(): List<Seen> = packets.toList().also { packets.clear() }
    }

    private val script = BrokerScript(MQTTVersion.MQTT5)

    private inner class Rig(
        val logger: Recorder = Recorder(),
        limits: MqttLimits = MqttLimits(),
        reconnect: ReconnectPolicy = ReconnectPolicy.Never,
    ) {
        private val host = StubHost()
        val client = MqttClient(
            MqttClientOptions(
                host = "broker.test",
                port = 1883,
                clientId = "packets",
                keepAlive = 0.seconds,
                reconnect = reconnect,
                limits = limits,
                logger = logger,
            ),
            host,
        )
        var now = 1_000L
        val writes = ArrayList<ByteArray>()

        init {
            client.start()
        }

        fun step(vararg events: TransportEvent): List<HostAction> {
            val actions = host.driver!!.runOnce(now, events.toList()).actions
            writes += actions.filterIsInstance<HostAction.Write>().map { it.bytes }
            return actions
        }

        fun bytes(data: ByteArray): List<HostAction> = step(TransportEvent.Bytes(data))

        /** До Connected: Connect → Connected (CONNECT записан) → CONNACK одной порцией. */
        fun connect() {
            assertTrue(step().any { it is HostAction.Connect }, "Connect")
            step(TransportEvent.Connected)
            bytes(script.connack())
            assertIs<ConnectionState.Connected>(client.connection.value)
        }
    }

    @Test
    fun connectAndConnackAreLoggedWithTheirSizes() {
        val rig = Rig()
        rig.connect()
        val connect = rig.writes.single()
        assertEquals(
            listOf(
                Seen(PacketDirection.Outgoing, CONNECT, connect.size),
                Seen(PacketDirection.Incoming, CONNACK, script.connack().size),
            ),
            rig.logger.take(),
        )
    }

    @Test
    fun severalIncomingPacketsInOneChunkAndSeveralPacketsInOneWrite() {
        val rig = Rig()
        rig.connect()
        rig.logger.take()
        rig.writes.clear()
        val first = script.publish("a/b", ByteArray(10), Qos.AT_LEAST_ONCE, packetId = 1)
        val second = script.publish("a/c", ByteArray(300), Qos.AT_LEAST_ONCE, packetId = 2)
        rig.bytes(first + second)
        // Оба PUBACK — одной записью: ядро склеило их в takeOutbound
        val write = rig.writes.single()
        val logged = rig.logger.take()
        assertEquals(
            listOf(
                Seen(PacketDirection.Incoming, PUBLISH, first.size),
                Seen(PacketDirection.Incoming, PUBLISH, second.size),
            ),
            logged.filter { it.direction == PacketDirection.Incoming },
        )
        val outgoing = logged.filter { it.direction == PacketDirection.Outgoing }
        assertEquals(listOf(PUBACK, PUBACK), outgoing.map { it.type })
        assertEquals(write.size, outgoing.sumOf { it.size })
        assertTrue(second.size > 128 + 3, "длина остатка второго PUBLISH — два байта varint")
    }

    @Test
    fun packetFedByteByByteIsLoggedOnceAfterItsLastByte() {
        val rig = Rig()
        rig.step()
        rig.step(TransportEvent.Connected)
        rig.logger.take()
        val connack = script.connack()
        for (i in connack.indices) {
            assertTrue(rig.logger.packets.isEmpty(), "до последнего байта пакет не сообщается")
            rig.bytes(byteArrayOf(connack[i]))
        }
        assertEquals(listOf(Seen(PacketDirection.Incoming, CONNACK, connack.size)), rig.logger.take())
        assertIs<ConnectionState.Connected>(rig.client.connection.value)
    }

    @Test
    fun tooLargePacketIsLoggedWithItsFullSizeAndTheSessionLives() {
        val rig = Rig(limits = MqttLimits(maxIncomingPacket = 64))
        rig.connect()
        rig.logger.take()
        val big = script.publish("a/b", ByteArray(500))
        val next = script.publish("a/c", ByteArray(5))
        rig.bytes(big.copyOfRange(0, 100))
        rig.bytes(big.copyOfRange(100, big.size) + next)
        assertEquals(
            listOf(
                Seen(PacketDirection.Incoming, PUBLISH, big.size),
                Seen(PacketDirection.Incoming, PUBLISH, next.size),
            ),
            rig.logger.take(),
        )
        assertIs<ConnectionState.Connected>(rig.client.connection.value)
    }

    @Test
    fun halfAHeaderOfALostConnectionDoesNotShiftTheNextConnection() {
        val rig = Rig(reconnect = ReconnectPolicy(initial = 1.seconds, max = 1.seconds, jitter = 0.0))
        rig.connect()
        rig.logger.take()
        // Обрыв посреди длины (varint продолжается): без сброса CONNACK нового соединения ушёл бы в «тело»
        val cut = script.publish("a/b", ByteArray(300)).copyOfRange(0, 2)
        rig.step(TransportEvent.Bytes(cut), TransportEvent.Eof)
        assertTrue(rig.logger.take().isEmpty())
        var connect = false
        repeat(10) {
            if (!connect) {
                rig.now += 2_000
                connect = rig.step().any { it is HostAction.Connect }
            }
        }
        assertTrue(connect, "переподключение")
        rig.writes.clear()
        rig.step(TransportEvent.Connected)
        rig.bytes(script.connack())
        assertEquals(
            listOf(
                Seen(PacketDirection.Outgoing, CONNECT, rig.writes.single().size),
                Seen(PacketDirection.Incoming, CONNACK, script.connack().size),
            ),
            rig.logger.take(),
        )
        assertIs<ConnectionState.Connected>(rig.client.connection.value)
    }

    @Test
    fun aThrowingLoggerDoesNotStopTheClient() {
        val rig = Rig(logger = Recorder(fail = true))
        rig.connect()
        val first = script.publish("a/b", ByteArray(3), Qos.AT_LEAST_ONCE, packetId = 1)
        rig.bytes(first)
        assertEquals(listOf(CONNECT, CONNACK, PUBLISH, PUBACK), rig.logger.take().map { it.type })
        assertIs<ConnectionState.Connected>(rig.client.connection.value)
    }
}
