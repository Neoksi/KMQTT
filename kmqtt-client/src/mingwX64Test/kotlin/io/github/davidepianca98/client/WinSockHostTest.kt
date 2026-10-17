@file:OptIn(MqttCoreApi::class)

package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.hostcontract.ContractDriver
import io.github.davidepianca98.socket.tls.TLSClientSettings
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.TestResult
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import platform.windows.Sleep
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/** SEVEN-250: платформенные сценарии хоста mingwX64 (подмена резолвера и фабрики), как в `NioActorHostTest`. */
internal class WinSockHostTest {

    private class Gate {
        private val released = atomic(false)
        private val entered = atomic(false)

        val isEntered: Boolean get() = entered.value

        /** Блокирующий «резолвер», который висит до [release] (поток `Dispatchers.IO`). */
        fun hang(): WinSock.Resolved {
            entered.value = true
            while (!released.value) Sleep(5u)
            return WinSock.Resolved.Error(WinSock.WSAHOST_NOT_FOUND)
        }

        fun release() {
            released.value = true
        }
    }

    private fun hostTest(block: suspend CoroutineScope.() -> Unit): TestResult = runTest(timeout = 60.seconds) {
        withContext(Dispatchers.Default) { block() }
    }

    private fun hostWith(factory: WinSockTransportFactory): WinSockHost =
        WinSockHost(nextHostThreadName(), MqttClock.Monotonic, factory, WinSockHost.DEFAULT_READ_BUFFER)

    @Test
    fun hangingDnsHitsConnectTimeoutWithoutBlockingHostThread() = hostTest {
        val gate = Gate()
        val host = hostWith { WinSockTransport(it) { _, _ -> gate.hang() } }
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
            gate.release()
            host.close(5_000)
        }
    }

    /** H5: подключение «висит» на резолве; close() < 200 мс. */
    @Test
    fun closeDuringHangingResolveIsFast() = hostTest {
        val gate = Gate()
        val host = hostWith { WinSockTransport(it) { _, _ -> gate.hang() } }
        try {
            val driver = ContractDriver()
            host.start(driver)
            driver.awaitRun(5_000)
            driver.send(host, HostAction.Connect("blocked.test", 1883, 60_000))
            withTimeout(5_000) { while (!gate.isEntered) kotlinx.coroutines.delay(1) }
            val t0 = TimeSource.Monotonic.markNow()
            assertTrue(host.close(), "close() дождался выхода цикла")
            val elapsed = t0.elapsedNow().inWholeMilliseconds
            assertTrue(elapsed < 200, "close() во время подключения: $elapsed мс")
            assertNull(withTimeout(1_000) { driver.stopped.await() })
            assertTrue(driver.drainEvents().none { it == TransportEvent.Connected })
        } finally {
            gate.release()
        }
    }

    /** Исключение фабрики или `connect` — `Failed(Io)`, хост живёт и следующий `Connect` проходит (как SEVEN-255 D1). */
    @Test
    fun transportFactoryOrConnectExceptionIsConnectFailureAndHostSurvives() = hostTest {
        val server = WinSockServer(false)
        var calls = 0 // фабрику зовёт только поток хоста
        val host = hostWith { ctx ->
            when (++calls) {
                1 -> throw IllegalStateException("no sockets")
                2 -> object : MqttTransport by WinSockTransport(ctx) {
                    override fun connect(host: String, port: Int, deadlineMs: Long) {
                        throw IllegalStateException("no sockets")
                    }
                }
                else -> WinSockTransport(ctx)
            }
        }
        try {
            val driver = ContractDriver()
            host.start(driver)
            driver.awaitRun(5_000)
            repeat(2) { attempt ->
                driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
                val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent(), "попытка ${attempt + 1}")
                assertEquals("IllegalStateException", assertIs<MqttError.Io>(failed.error).kind)
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

    @Test
    fun refusedConnectKindIsWinSockName() = hostTest {
        val closed = WinSockServer(false)
        val port = closed.port
        closed.close()
        val host = WinSockHost()
        try {
            val driver = ContractDriver()
            host.start(driver)
            driver.send(host, HostAction.Connect("127.0.0.1", port, 10_000))
            val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent(15_000))
            assertEquals("WSAECONNREFUSED", assertIs<MqttError.Io>(failed.error).kind)
        } finally {
            host.close(5_000)
        }
    }

    @Test
    fun mingwFactoryRefusesTlsAndWebSocket() = runTest {
        assertFailsWith<IllegalArgumentException> {
            MqttClient(MqttClientOptions(host = "localhost", port = 8883, tls = TLSClientSettings()))
        }
        assertFailsWith<IllegalArgumentException> {
            MqttClient(MqttClientOptions(host = "localhost", port = 1883, webSocket = MqttWebSocket()))
        }
    }
}
