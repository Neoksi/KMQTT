@file:OptIn(MqttCoreApi::class)

package io.github.davidepianca98.client

import io.github.davidepianca98.ClientSocket
import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.hostcontract.ContractDriver
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.TestResult
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * IPv6 на петле `::1` (Linux, Apple). Хост нового клиента ([PosixHost]) подключается к литералу `::1` и к нему же в
 * скобках `[::1]`; старый [ClientSocket] (`kmqtt-common` getaddrinfo, семейство сокета по адресу) — тоже. Если IPv6
 * на петле недоступен (нет стека, `::1` не назначен — так бывает в Docker), тест печатает причину и ничего не проверяет.
 */
internal class Ipv6LoopbackTest {

    private fun hostTest(block: suspend CoroutineScope.() -> Unit): TestResult = runTest(timeout = 60.seconds) {
        withContext(Dispatchers.Default) { block() }
    }

    @Test
    fun hostConnectsToIpv6LoopbackLiteralAndBracketedLiteral() = hostTest {
        val server = PosixServer.ipv6OrNull() ?: return@hostTest
        try {
            for (name in listOf("::1", "[::1]")) {
                val host = PosixHost()
                try {
                    val driver = ContractDriver()
                    host.start(driver)
                    driver.send(host, HostAction.Connect(name, server.port, 5_000))
                    val peer = server.accept()
                    assertEquals(TransportEvent.Connected, driver.awaitEvent(), name)
                    driver.send(host, HostAction.Write(byteArrayOf(7, 8, 9)))
                    assertContentEquals(byteArrayOf(7, 8, 9), peer.readExactly(3), name)
                } finally {
                    host.close(5_000)
                }
            }
        } finally {
            server.close()
        }
    }

    @Test
    fun legacyClientSocketConnectsOverIpv6Loopback() = hostTest {
        val server = PosixServer.ipv6OrNull() ?: return@hostTest
        try {
            for (name in listOf("::1", "[::1]")) {
                val socket = ClientSocket(name, server.port, 1024, 50, 5_000) {}
                try {
                    val peer = server.accept()
                    socket.send(ubyteArrayOf(1u, 2u, 3u))
                    assertContentEquals(byteArrayOf(1, 2, 3), peer.readExactly(3), name)
                    peer.write(byteArrayOf(4, 5))
                    val received = mutableListOf<UByte>()
                    val started = TimeSource.Monotonic.markNow()
                    while (received.size < 2) {
                        socket.read()?.let { received.addAll(it) }
                        assertTrue(started.elapsedNow().inWholeMilliseconds < 5_000, "нет ответа по $name")
                        delay(1)
                    }
                    assertEquals(listOf<UByte>(4u, 5u), received, name)
                } finally {
                    socket.close()
                }
            }
        } finally {
            server.close()
        }
    }
}
