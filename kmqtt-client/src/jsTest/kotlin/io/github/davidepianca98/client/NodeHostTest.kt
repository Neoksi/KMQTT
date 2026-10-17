@file:OptIn(MqttCoreApi::class)

package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.hostcontract.ContractDriver
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.TestResult
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/** SEVEN-250: платформенные сценарии Node-хоста (подмена резолвера и фабрики, утечки), как в `NioActorHostTest`. */
internal class NodeHostTest {

    private fun hostTest(block: suspend CoroutineScope.() -> Unit): TestResult = runTest(timeout = 120.seconds) {
        withContext(Dispatchers.Default) { block() }
    }

    /** Резолвер, который никогда не отвечает (`lookup` сокета Node). */
    private val hangingLookup: dynamic = js("(function (hostname, options, callback) { })")

    private fun hangingHost(): NodeHost =
        NodeHost(MqttClock.Monotonic, NodeTransportFactory { NodeTransport(it, null, true, hangingLookup) })

    @Test
    fun hangingDnsHitsConnectTimeout() = hostTest {
        val host = hangingHost()
        try {
            val driver = ContractDriver()
            host.start(driver)
            driver.awaitRun(5_000)
            val t0 = TimeSource.Monotonic.markNow()
            driver.send(host, HostAction.Connect("blocked.test", 1883, 150))
            val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent(5_000))
            val elapsed = t0.elapsedNow().inWholeMilliseconds
            assertEquals(MqttError.ConnectTimeout, failed.error)
            assertTrue(elapsed in 100..2_000, "дедлайн подключения: $elapsed мс")
        } finally {
            host.close(5_000)
        }
    }

    /** H5: подключение «висит» на резолве; close() < 200 мс. */
    @Test
    fun closeDuringHangingResolveIsFast() = hostTest {
        val host = hangingHost()
        val driver = ContractDriver()
        host.start(driver)
        driver.awaitRun(5_000)
        driver.send(host, HostAction.Connect("blocked.test", 1883, 60_000))
        delay(50)
        val t0 = TimeSource.Monotonic.markNow()
        assertTrue(host.close(), "close() дождался остановки")
        val elapsed = t0.elapsedNow().inWholeMilliseconds
        assertTrue(elapsed < 200, "close() во время подключения: $elapsed мс")
        assertNull(withTimeout(1_000) { driver.stopped.await() })
        assertTrue(driver.drainEvents().none { it == TransportEvent.Connected })
    }

    /** Исключение фабрики или `connect` (в т. ч. ошибка JS, не `Exception`) — `Failed(Io)`, хост живёт (SEVEN-255 D1). */
    @Test
    fun transportFactoryOrConnectExceptionIsConnectFailureAndHostSurvives() = hostTest {
        val server = NodeContractServer.listen(tls = null)
        var calls = 0
        val host = NodeHost(MqttClock.Monotonic, NodeTransportFactory { ctx ->
            when (++calls) {
                1 -> throw IllegalStateException("no sockets")
                2 -> object : MqttTransport by NodeTransport(ctx) {
                    override fun connect(host: String, port: Int, deadlineMs: Long) {
                        js("throw new Error('EMFILE')")
                    }
                }
                else -> NodeTransport(ctx)
            }
        })
        try {
            val driver = ContractDriver()
            host.start(driver)
            driver.awaitRun(5_000)
            repeat(2) { attempt ->
                driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
                val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent(), "попытка ${attempt + 1}")
                assertIs<MqttError.Io>(failed.error)
                assertFalse(driver.stopped.isCompleted, "хост не остановлен")
            }
            driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
            server.accept()
            assertEquals(TransportEvent.Connected, driver.awaitEvent())
        } finally {
            host.close(5_000)
            server.close()
        }
    }

    /** K6: 1 000 циклов start/connect/close не оставляют активных дескрипторов Node (сокеты, таймеры). */
    @Test
    fun thousandStartCloseCyclesLeakNoHandles() = hostTest {
        val server = NodeContractServer.listen(tls = null)
        try {
            repeat(5) { cycle(server) }
            delay(300)
            val before = activeHandles()
            val t0 = TimeSource.Monotonic.markNow()
            repeat(1_000) { cycle(server) }
            val elapsed = t0.elapsedNow().inWholeMilliseconds
            delay(500)
            val after = activeHandles()
            println("NodeHostTest: 1000 циклов за $elapsed мс; активные дескрипторы Node $before → $after")
            assertTrue(after <= before + 2, "активные дескрипторы: $before → $after")
        } finally {
            server.close()
        }
    }

    private suspend fun cycle(server: NodeContractServer) {
        val driver = ContractDriver()
        val host = NodeHost()
        host.start(driver)
        driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
        val peer = server.accept()
        assertEquals(TransportEvent.Connected, driver.awaitEvent())
        assertTrue(host.close(), "close() дождался остановки")
        assertNull(withTimeout(1_000) { driver.stopped.await() })
        peer.close()
    }

    private fun activeHandles(): Int = (js("process._getActiveHandles().length") as Number).toInt()
}
