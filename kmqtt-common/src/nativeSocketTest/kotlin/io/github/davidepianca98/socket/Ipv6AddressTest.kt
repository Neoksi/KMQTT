package io.github.davidepianca98.socket

import io.github.davidepianca98.accept
import io.github.davidepianca98.addressFamilyOf
import io.github.davidepianca98.bind
import io.github.davidepianca98.close
import io.github.davidepianca98.connect
import io.github.davidepianca98.getErrno
import io.github.davidepianca98.getaddrinfo
import io.github.davidepianca98.hostForResolve
import io.github.davidepianca98.listen
import io.github.davidepianca98.set_non_blocking
import io.github.davidepianca98.socket
import io.github.davidepianca98.socket.tcp.Socket
import io.github.davidepianca98.socketsCleanup
import io.github.davidepianca98.socketsInit
import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.reinterpret
import platform.posix.AF_INET
import platform.posix.AF_INET6
import platform.posix.SOCK_STREAM
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.TimeSource

/**
 * разрешение имён `kmqtt-common` на native — `AF_UNSPEC`, IPv4 предпочтительно, IPv6 по литералу (и в
 * скобках), семейство по длине адреса; соединение `Socket` по петле `::1`. Если IPv6 на петле недоступен (нет стека,
 * `::1` не назначен — так бывает в Docker), тест соединения печатает причину и ничего не проверяет.
 */
class Ipv6AddressTest {

    private fun <T> withSockets(block: () -> T): T {
        socketsInit()
        try {
            return block()
        } finally {
            socketsCleanup()
        }
    }

    /** Порт из `sockaddr_in`/`sockaddr_in6` (смещение 2, сетевой порядок). */
    private fun portOf(address: kotlinx.cinterop.CPointer<platform.posix.sockaddr>): Int {
        val bytes = address.reinterpret<ByteVar>()
        return ((bytes[2].toInt() and 0xFF) shl 8) or (bytes[3].toInt() and 0xFF)
    }

    @Test
    fun bracketsAreStrippedOnlyFromIpv6Literal() {
        assertEquals("::1", hostForResolve("[::1]"))
        assertEquals("fe80::1%eth0", hostForResolve("[fe80::1%eth0]"))
        assertEquals("::1", hostForResolve("::1"))
        assertEquals("broker.local", hostForResolve("broker.local"))
        assertEquals("[]", hostForResolve("[]"))
    }

    @Test
    fun ipv6LiteralResolvesToSockaddrIn6() = withSockets {
        for (name in listOf("::1", "[::1]")) {
            memScoped {
                val result = assertNotNull(getaddrinfo(name, "1883"), name)
                assertEquals(AF_INET6, addressFamilyOf(result.second), name)
                assertEquals(28u, result.second, "sockaddr_in6: $name")
                assertEquals(1883, portOf(result.first), name)
            }
        }
    }

    @Test
    fun ipv4LiteralResolvesToSockaddrIn() = withSockets {
        memScoped {
            val result = assertNotNull(getaddrinfo("127.0.0.1", "8883"))
            assertEquals(AF_INET, addressFamilyOf(result.second))
            assertEquals(16u, result.second)
            assertEquals(8883, portOf(result.first))
        }
    }

    @Test
    fun socketConnectsOverIpv6Loopback() = withSockets {
        val listener = socket(AF_INET6, SOCK_STREAM, 0)
        if (listener < 0) {
            println("IPv6 недоступен (socket AF_INET6: ${getErrno()}): тест пропущен")
            return@withSockets
        }
        var client = -1
        var server = -1
        try {
            val bound = memScoped {
                val any = getaddrinfo("::1", "0") ?: return@memScoped false
                bind(listener, any.first, any.second) == 0
            }
            if (!bound) {
                println("IPv6 на петле недоступен (bind ::1: ${getErrno()}): тест пропущен")
                return@withSockets
            }
            assertEquals(0, listen(listener, 1), "listen: ${getErrno()}")
            val port = TestNet.localPort(listener)
            memScoped {
                val target = assertNotNull(getaddrinfo("[::1]", port.toString()))
                client = socket(addressFamilyOf(target.second), SOCK_STREAM, 0)
                assertTrue(client >= 0, "socket: ${getErrno()}")
                assertEquals(0, connect(client, target.first, target.second), "connect [::1]: ${getErrno()}")
            }
            server = accept(listener, null, null)
            assertTrue(server >= 0, "accept: ${getErrno()}")
            assertEquals(0, set_non_blocking(client))
            assertEquals(0, set_non_blocking(server))

            val clientSocket = Socket(client, null, ByteArray(1024))
            val serverSocket = Socket(server, null, ByteArray(1024))
            clientSocket.send(ubyteArrayOf(1u, 2u, 3u))
            val received = mutableListOf<UByte>()
            val started = TimeSource.Monotonic.markNow()
            while (received.size < 3) {
                serverSocket.read()?.let { received.addAll(it) }
                assertTrue(started.elapsedNow().inWholeMilliseconds < 5_000, "нет данных по ::1")
                TestNet.sleepMs(2)
            }
            assertContentEquals(listOf<UByte>(1u, 2u, 3u), received)
        } finally {
            if (client >= 0) close(client)
            if (server >= 0) close(server)
            close(listener)
        }
    }
}
