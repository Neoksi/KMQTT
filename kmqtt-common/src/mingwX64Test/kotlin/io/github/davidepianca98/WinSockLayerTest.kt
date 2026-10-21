package io.github.davidepianca98

import kotlinx.cinterop.*
import platform.posix.AF_INET
import platform.posix.SOCK_STREAM
import platform.posix.SOL_SOCKET
import platform.posix.SO_RCVTIMEO
import platform.posix.SO_SNDTIMEO
import platform.posix.fd_set
import platform.posix.sockaddr
import platform.posix.sockaddr_in
import platform.windows.getsockopt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.TimeSource

/**
 * Регрессия старого native-слоя сокетов `mingwX64Main/Posix.kt` на настоящем WinSock: `setsockopt`
 * передавал `optval.toString()`, `select` строил `timeval` только из микросекунд, `getaddrinfo` отдавал указатель
 * в список, который не освобождался.
 */
class WinSockLayerTest {

    private fun withSocket(block: MemScope.(Int) -> Unit) {
        socketsInit()
        val fd = socket(AF_INET, SOCK_STREAM, 0)
        assertTrue(fd >= 0, "socket() failed: ${getErrno()}")
        try {
            memScoped { block(fd) }
        } finally {
            close(fd)
            socketsCleanup()
        }
    }

    private fun MemScope.readDwordOption(fd: Int, option: Int): UInt {
        val value = alloc<UIntVar>()
        val length = alloc<IntVar>()
        length.value = sizeOf<UIntVar>().toInt()
        val rc = getsockopt(fd.convert(), SOL_SOCKET, option, value.ptr.reinterpret<ByteVar>(), length.ptr)
        assertEquals(0, rc, "getsockopt failed: ${getErrno()}")
        return value.value
    }

    @Test
    fun setsockoptPassesValueByPointer() = withSocket { fd ->
        assertEquals(0, set_recv_socket_timeout(fd, 1500))
        assertEquals(1500u, readDwordOption(fd, SO_RCVTIMEO))

        assertEquals(0, set_send_socket_timeout(fd, 2750))
        assertEquals(2750u, readDwordOption(fd, SO_SNDTIMEO))
    }

    @Test
    fun selectTimeoutLongerThanOneSecondWaitsAndTimesOut() = withSocket { fd ->
        val address = sockaddrIn(AF_INET.toUShort(), 0u) // INADDR_ANY, порт выбирает система
        assertEquals(0, bind(fd, address.ptr.reinterpret<sockaddr>(), sizeOf<sockaddr_in>().toUInt()), "bind: ${getErrno()}")
        assertEquals(0, listen(fd, 1), "listen: ${getErrno()}")

        val readfds = alloc<fd_set>()
        fdZero(readfds.ptr)
        fdSet(fd, readfds.ptr)

        val started = TimeSource.Monotonic.markNow()
        val rc = select(fd + 1, readfds.ptr, null, null, 1300)
        val elapsedMs = started.elapsedNow().inWholeMilliseconds

        // До правки tv_usec = 1 300 000 был недопустим: WSAEINVAL сразу, rc = -1
        assertEquals(0, rc, "select: rc=$rc error=${getErrno()}")
        assertTrue(elapsedMs in 1200..4000, "select ждал $elapsedMs мс вместо ≈ 1300")
    }

    @Test
    fun getaddrinfoReturnsIpv4AddressCopy() = withSocket { _ ->
        val result = getaddrinfo("localhost", "1883")
        assertNotNull(result)
        val (address, length) = result
        assertEquals(sizeOf<sockaddr_in>().toUInt(), length)
        val inet = address.reinterpret<sockaddr_in>().pointed
        assertEquals(AF_INET, inet.sin_family.toInt())
        val port = inet.sin_port.toInt()
        assertEquals(1883, ((port and 0xFF) shl 8) or ((port shr 8) and 0xFF)) // сетевой порядок байтов

        // Повторные резолвы не портят прежний результат (адрес скопирован из списка, а не указывает в него)
        repeat(200) { assertTrue(getaddrinfo("localhost", "1883") != null) }
        assertEquals(AF_INET, inet.sin_family.toInt())
    }

    @Test
    fun getaddrinfoUnknownHostReturnsNull() = withSocket { _ ->
        assertTrue(getaddrinfo("no-such-host.invalid", "1883") == null)
    }
}
