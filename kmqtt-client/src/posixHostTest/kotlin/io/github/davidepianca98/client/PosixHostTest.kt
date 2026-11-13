@file:OptIn(MqttCoreApi::class)

package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.hostcontract.ContractDriver
import io.github.davidepianca98.socket.tls.TLSClientSettings
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.TestResult
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import platform.posix.usleep
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/** Платформенные сценарии хоста POSIX (Linux, Apple; подмена резолвера и фабрики), как в `WinSockHostTest`. */
internal class PosixHostTest {

    private class Gate {
        private val released = atomic(false)
        private val entered = atomic(false)

        val isEntered: Boolean get() = entered.value

        /** Блокирующий «резолвер», который висит до [release] (поток `Dispatchers.IO`). */
        fun hang(): PosixSockets.Resolved {
            entered.value = true
            while (!released.value) usleep(5_000u)
            return PosixSockets.Resolved.Error(platform.posix.EAI_NONAME)
        }

        fun release() {
            released.value = true
        }
    }

    private fun hostTest(block: suspend CoroutineScope.() -> Unit): TestResult = runTest(timeout = 120.seconds) {
        withContext(Dispatchers.Default) { block() }
    }

    private fun hostWith(factory: PosixTransportFactory): PosixHost =
        PosixHost(nextHostThreadName(), MqttClock.Monotonic, factory, PosixHost.DEFAULT_READ_BUFFER)

    @Test
    fun hangingDnsHitsConnectTimeoutWithoutBlockingHostThread() = hostTest {
        val gate = Gate()
        val host = hostWith { PosixTransport(it) { _, _ -> gate.hang() } }
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

    /** Подключение «висит» на резолве; close() < 200 мс. */
    @Test
    fun closeDuringHangingResolveIsFast() = hostTest {
        val gate = Gate()
        val host = hostWith { PosixTransport(it) { _, _ -> gate.hang() } }
        try {
            val driver = ContractDriver()
            host.start(driver)
            driver.awaitRun(5_000)
            driver.send(host, HostAction.Connect("blocked.test", 1883, 60_000))
            withTimeout(5_000) { while (!gate.isEntered) delay(1) }
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

    /** Исключение фабрики или `connect` — `Failed(Io)`, хост живёт и следующий `Connect` проходит. */
    @Test
    fun transportFactoryOrConnectExceptionIsConnectFailureAndHostSurvives() = hostTest {
        val server = PosixServer(false)
        var calls = 0 // фабрику зовёт только поток хоста
        val host = hostWith { ctx ->
            when (++calls) {
                1 -> throw IllegalStateException("no sockets")
                2 -> object : MqttTransport by PosixTransport(ctx) {
                    override fun connect(host: String, port: Int, deadlineMs: Long) {
                        throw IllegalStateException("no sockets")
                    }
                }
                else -> PosixTransport(ctx)
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
    fun refusedConnectKindIsErrnoName() = hostTest {
        val closed = PosixServer(false)
        val port = closed.port
        closed.close()
        val host = PosixHost()
        try {
            val driver = ContractDriver()
            host.start(driver)
            driver.send(host, HostAction.Connect("127.0.0.1", port, 10_000))
            val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent(15_000))
            assertEquals("ECONNREFUSED", assertIs<MqttError.Io>(failed.error).kind)
        } finally {
            host.close(5_000)
        }
    }

    /** Сброс соединения сервером (RST) при приостановленном чтении: `Failed`, а не холостой цикл на `POLLHUP`. */
    @Test
    fun peerResetWhilePausedFailsOnNextWrite() = hostTest {
        val server = PosixServer(false)
        val host = PosixHost()
        try {
            val driver = ContractDriver()
            host.start(driver)
            driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
            val peer = server.accept()
            assertEquals(TransportEvent.Connected, driver.awaitEvent())
            driver.send(host, HostAction.PauseRead(true))
            driver.awaitRun()
            // Непрочитанные байты на стороне сервера + close() = RST
            driver.send(host, HostAction.Write(ByteArray(16)))
            peer.readExactly(1)
            peer.close()
            delay(100)
            driver.clearRuns()
            delay(300)
            var turns = 0
            while (driver.runs.tryReceive().isSuccess) turns++
            assertTrue(turns < 20, "цикл не крутится вхолостую: $turns оборотов за 300 мс")
            driver.send(host, HostAction.Write(ByteArray(16)))
            val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent())
            val kind = assertIs<MqttError.Io>(failed.error).kind
            assertTrue(kind == "ECONNRESET" || kind == "EPIPE", "вид отказа: $kind")
        } finally {
            host.close(5_000)
            server.close()
        }
    }

    /**
     * 1 000 циклов start/connect/close не растят число дескрипторов и потоков процесса. Счётчик, которого платформа
     * не показывает ([processDescriptorCount], [processThreadCount] вернули -1), не проверяется — об этом печатается.
     */
    @Test
    fun thousandStartCloseCyclesLeakNoDescriptorsOrThreads() = hostTest {
        val server = PosixServer(false)
        try {
            repeat(5) { cycle(server) }
            delay(300)
            val fdsBefore = processDescriptorCount()
            val threadsBefore = processThreadCount()
            val t0 = TimeSource.Monotonic.markNow()
            repeat(1_000) { cycle(server) }
            val elapsed = t0.elapsedNow().inWholeMilliseconds
            delay(500)
            val fdsAfter = processDescriptorCount()
            val threadsAfter = processThreadCount()
            println(
                "PosixHostTest: 1000 циклов за $elapsed мс; дескрипторы $fdsBefore → $fdsAfter, " +
                    "потоки $threadsBefore → $threadsAfter"
            )
            if (fdsBefore < 0 || fdsAfter < 0) {
                println("PosixHostTest[$posixTestPlatform]: число дескрипторов процесса не проверено (платформа его не показывает)")
            } else {
                assertTrue(fdsAfter <= fdsBefore + 10, "дескрипторы процесса: $fdsBefore → $fdsAfter")
            }
            if (threadsBefore < 0 || threadsAfter < 0) {
                println("PosixHostTest[$posixTestPlatform]: число потоков процесса не проверено (платформа его не показывает)")
            } else {
                assertTrue(threadsAfter <= threadsBefore + 10, "потоки процесса: $threadsBefore → $threadsAfter")
            }
        } finally {
            server.close()
        }
    }

    private suspend fun cycle(server: PosixServer) {
        val driver = ContractDriver()
        val host = PosixHost()
        host.start(driver)
        driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
        val peer = server.accept()
        assertEquals(TransportEvent.Connected, driver.awaitEvent())
        assertTrue(host.close(), "close() дождался остановки")
        assertNull(withTimeout(1_000) { driver.stopped.await() })
        peer.close()
    }

    @Test
    fun platformFactoryRefusesWebSocketAndImplicitTrustAll() = runTest {
        // TLS на OpenSSL принимается (хост с TLS-транспортом), WebSocket — пока нет
        MqttClient(MqttClientOptions(host = "localhost", port = 8883, tls = TLSClientSettings())).close()
        assertFailsWith<IllegalArgumentException> {
            MqttClient(MqttClientOptions(host = "localhost", port = 1883, webSocket = MqttWebSocket()))
        }
        // «Доверять всем» только с явным verifyHostname = false, как на JVM
        assertFailsWith<IllegalArgumentException> {
            MqttClient(MqttClientOptions(host = "localhost", port = 8883, tls = TLSClientSettings(checkServerCertificate = false)))
        }
        MqttClient(
            MqttClientOptions(
                host = "localhost",
                port = 8883,
                tls = TLSClientSettings(checkServerCertificate = false),
                verifyHostname = false
            )
        ).close()
    }
}
