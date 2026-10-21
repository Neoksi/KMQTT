package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.packets.Qos
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assume.assumeTrue
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration.Companion.seconds

/**
 * Жизненный цикл фасада вместе с потоком хоста: 1 000 циклов `start`/`close` без роста потоков
 * `kmqtt-io-*` и дескрипторов, закрытие в каждом состоянии (подключение, ожидание CONNACK, пауза перед реконнектом).
 */
internal class HostLifecycleTest : BrokerTestBase() {

    private fun timed(limitMs: Long = 30_000, block: suspend CoroutineScope.() -> Unit) {
        runBlocking { withTimeout(limitMs, block) }
    }

    private fun awaitIoThreadsAtMost(expected: Int, what: String) =
        BrokerTestKit.waitUntil(5_000, "$what: kmqtt-io ${BrokerTestKit.ioThreads()} > $expected") {
            BrokerTestKit.ioThreads() <= expected
        }

    /**
     * 1 000 циклов `MqttClient(...).start()` + `close()` против брокера (закрытие в момент подключения, без DISCONNECT)
     * 1 000 циклов `connect()` + `close()` (полный цикл с CONNACK и DISCONNECT): потоки `kmqtt-io-*` по имени и открытые
     * дескрипторы (где есть `UnixOperatingSystemMXBean`) возвращаются к исходным, всё вместе укладывается в 60 с, брокер
     * после этого исправен.
     */
    @Test
    fun thousandStartCloseCyclesLeaveNoThreadsOrFds() = timed(60_000) {
        val broker = broker()
        val threadsBefore = BrokerTestKit.ioThreads()
        val fdsBefore = BrokerTestKit.openFds()
        val started = System.nanoTime()

        repeat(START_CLOSE_CYCLES) {
            val client = MqttClient(options(broker.port), NioActorHost())
            client.start()
            withTimeout(10_000) { client.close() }
        }
        val startCloseMs = (System.nanoTime() - started) / 1_000_000

        repeat(CONNECTED_CYCLES) {
            val client = MqttClient(options(broker.port), NioActorHost())
            assertIs<ConnectResult.Connected>(withTimeout(10_000) { client.connect() })
            withTimeout(10_000) { client.close() }
        }
        val totalMs = (System.nanoTime() - started) / 1_000_000

        awaitIoThreadsAtMost(threadsBefore, "после циклов")
        val fdsAfter = BrokerTestKit.openFds()
        println(
            "HostLifecycleTest: $START_CLOSE_CYCLES start/close за $startCloseMs мс, +$CONNECTED_CYCLES connect/close, " +
                "всего $totalMs мс; потоки kmqtt-io $threadsBefore -> ${BrokerTestKit.ioThreads()}; " +
                "fd ${fdsBefore ?: "недоступно (нет UnixOperatingSystemMXBean)"} -> ${fdsAfter ?: "недоступно"}"
        )
        assertTrue(totalMs <= 60_000, "циклы заняли $totalMs мс")
        if (fdsBefore != null && fdsAfter != null) {
            assertTrue(fdsAfter <= fdsBefore + FD_SLACK, "дескрипторы выросли: $fdsBefore -> $fdsAfter")
        }

        val probe = client(broker.port)
        assertIs<ConnectResult.Connected>(withTimeout(10_000) { probe.connect() })
        assertIs<PublishResult.Delivered>(
            probe.publish("life/probe", byteArrayOf(1), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        )
    }

    /** Подключения к порту без слушателя (отказ), `ReconnectPolicy.Never`: `connect()` → `Failed`, потоков не остаётся. */
    @Test
    fun refusedConnectCyclesLeaveNoThreads() = timed {
        val port = TestBroker.freePort()
        val threadsBefore = BrokerTestKit.ioThreads()
        val fdsBefore = BrokerTestKit.openFds()
        repeat(REFUSED_CYCLES) {
            val client = MqttClient(options(port) { it.copy(reconnect = ReconnectPolicy.Never) }, NioActorHost())
            assertIs<ConnectResult.Failed>(withTimeout(10_000) { client.connect() })
            withTimeout(10_000) { client.close() }
        }
        awaitIoThreadsAtMost(threadsBefore, "после отказов")
        val fdsAfter = BrokerTestKit.openFds()
        if (fdsBefore != null && fdsAfter != null) {
            assertTrue(fdsAfter <= fdsBefore + FD_SLACK, "дескрипторы выросли: $fdsBefore -> $fdsAfter")
        }
    }

    /**
     * Клиент, закрывшийся сам (`ReconnectPolicy.Never`, отказ подключения), держит поток хоста до `close()` — вызывать
     * `close()` обязательно (KDoc фасада); после него поток уходит.
     */
    @Test
    fun selfClosedClientHoldsHostThreadUntilClose() = timed {
        val threadsBefore = BrokerTestKit.ioThreads()
        val client = client(TestBroker.freePort()) { it.copy(reconnect = ReconnectPolicy.Never) }
        assertIs<ConnectResult.Failed>(withTimeout(10_000) { client.connect() })
        assertIs<ConnectionState.Closed>(client.connection.value)
        assertEquals(threadsBefore + 1, BrokerTestKit.ioThreads(), "поток хоста живёт до close()")
        withTimeout(10_000) { client.close() }
        awaitIoThreadsAtMost(threadsBefore, "после close()")
    }

    /** `close()` во время паузы перед переподключением возвращается сразу (не ждёт задержку реконнекта). */
    @Test
    fun closeDuringReconnectBackoffIsFast() = timed {
        val threadsBefore = BrokerTestKit.ioThreads()
        val client = client(TestBroker.freePort()) {
            it.copy(reconnect = ReconnectPolicy(initial = 20.seconds, max = 20.seconds, jitter = 0.0))
        }
        client.start()
        BrokerTestKit.waitUntil(5_000, "клиент в паузе перед реконнектом") { client.connection.value is ConnectionState.Waiting }
        val started = System.nanoTime()
        withTimeout(10_000) { client.close() }
        val closeMs = (System.nanoTime() - started) / 1_000_000
        println("HostLifecycleTest.closeDuringBackoff: close() занял $closeMs мс")
        assertTrue(closeMs < 2_500, "close() в паузе реконнекта занял $closeMs мс (пауза 20 с)")
        assertEquals(ConnectionState.Closed(null), client.connection.value)
        awaitIoThreadsAtMost(threadsBefore, "после close()")
    }

    /**
     * `close()` во время ожидания CONNACK (сервер принял TCP и молчит): соединение закрывается без DISCONNECT (KDoc
     * `close()`), штатно (EOF у сервера, не RST), быстро — не ждёт `connackTimeout`.
     */
    @Test
    fun closeWhileWaitingForConnackSendsNoDisconnectAndIsFast() = timed {
        val server = ServerSocket(0, 1, InetAddress.getLoopbackAddress())
        track(AutoCloseable { server.close() })
        val received = java.util.concurrent.LinkedBlockingQueue<Any>()
        thread(isDaemon = true, name = "silent-server") {
            try {
                server.accept().use { socket ->
                    socket.soTimeout = 10_000
                    val bytes = java.io.ByteArrayOutputStream()
                    val buffer = ByteArray(1024)
                    while (true) {
                        val read = socket.getInputStream().read(buffer)
                        if (read < 0) break
                        bytes.write(buffer, 0, read)
                        received.put("data")
                    }
                    received.put(bytes.toByteArray())
                }
            } catch (e: Exception) {
                received.put(e)
            }
        }
        val threadsBefore = BrokerTestKit.ioThreads()
        val client = client(server.localPort) { it.copy(connackTimeout = 30.seconds) }
        client.start()
        assertEquals("data", received.poll(5, TimeUnit.SECONDS), "сервер получил CONNECT")
        val started = System.nanoTime()
        withTimeout(10_000) { client.close() }
        val closeMs = (System.nanoTime() - started) / 1_000_000
        println("HostLifecycleTest.closeBeforeConnack: close() занял $closeMs мс")
        assertTrue(closeMs < 2_500, "close() до CONNACK занял $closeMs мс (тайм-аут CONNACK 30 с)")

        var end: Any
        do {
            end = received.poll(5, TimeUnit.SECONDS) ?: fail("сервер не увидел конца соединения")
        } while (end == "data")
        val bytes = end as? ByteArray ?: fail("RST вместо EOF у сервера: $end")
        assertEquals(0x10, bytes[0].toInt() and 0xF0, "единственный пакет — CONNECT")
        assertEquals(2 + (bytes[1].toInt() and 0x7F), bytes.size, "после CONNECT нет других пакетов (DISCONNECT)")
        awaitIoThreadsAtMost(threadsBefore, "после close()")
    }

    /** Через фасад: `close()` во время подключения к «чёрной дыре» — быстро и без утечки потока. */
    @Test
    fun closeDuringHangingTcpConnectIsFast() = timed {
        val hole = findBlackHole()
        assumeTrue("нет «чёрной дыры»: loopback с полным backlog и 10.255.255.1 дают отказ, а не зависание", hole != null)
        println("HostLifecycleTest: «чёрная дыра» — $hole")
        val threadsBefore = BrokerTestKit.ioThreads()
        val client = MqttClient(
            MqttClientOptions(host = hole!!.hostString, port = hole.port, connectTimeout = 60.seconds),
            NioActorHost()
        )
        client.start()
        delay(300)
        assumeTrue("подключение к $hole завершилось за 300 мс — не «чёрная дыра»", client.connection.value is ConnectionState.Connecting)
        val started = System.nanoTime()
        withTimeout(10_000) { client.close() }
        val closeMs = (System.nanoTime() - started) / 1_000_000
        println("HostLifecycleTest.closeDuringHangingConnect: close() занял $closeMs мс")
        assertTrue(closeMs < 2_500, "close() во время подключения занял $closeMs мс (тайм-аут подключения 60 с)")
        awaitIoThreadsAtMost(threadsBefore, "после close()")
    }

    /** Адрес, подключение к которому висит: сначала loopback с заполненным backlog, затем 10.255.255.1. */
    private fun findBlackHole(): InetSocketAddress? {
        val server = ServerSocket(0, 1, InetAddress.getLoopbackAddress())
        track(AutoCloseable { server.close() })
        val target = InetSocketAddress(InetAddress.getLoopbackAddress(), server.localPort)
        val sockets = ArrayList<Socket>()
        track(AutoCloseable { sockets.forEach { runCatching { it.close() } } })
        repeat(20) {
            val socket = Socket().also { sockets += it }
            try {
                socket.connect(target, 300)
            } catch (_: SocketTimeoutException) {
                return target
            } catch (_: Exception) {
                return@repeat
            }
        }
        val remote = InetSocketAddress("10.255.255.1", 1883)
        return try {
            Socket().use { it.connect(remote, 300) }
            null
        } catch (_: SocketTimeoutException) {
            remote
        } catch (_: Exception) {
            null
        }
    }

    private companion object {
        const val START_CLOSE_CYCLES = 1_000
        const val CONNECTED_CYCLES = 1_000
        const val REFUSED_CYCLES = 100
        const val FD_SLACK = 10
    }
}
