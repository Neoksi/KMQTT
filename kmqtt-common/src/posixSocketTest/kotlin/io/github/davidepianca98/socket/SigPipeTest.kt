package io.github.davidepianca98.socket

import io.github.davidepianca98.accept
import io.github.davidepianca98.bind
import io.github.davidepianca98.close
import io.github.davidepianca98.connect
import io.github.davidepianca98.getErrno
import io.github.davidepianca98.inet_pton
import io.github.davidepianca98.listen
import io.github.davidepianca98.send
import io.github.davidepianca98.sockaddrIn
import io.github.davidepianca98.socket
import io.github.davidepianca98.socketsCleanup
import io.github.davidepianca98.socketsInit
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.sizeOf
import kotlinx.cinterop.usePinned
import kotlinx.cinterop.value
import platform.posix.AF_INET
import platform.posix.ECONNRESET
import platform.posix.EPIPE
import platform.posix.SOCK_STREAM
import platform.posix.getsockname
import platform.posix.sockaddr_in
import platform.posix.socklen_tVar
import platform.posix.usleep
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Запись в сброшенное партнёром соединение даёт ошибку (`EPIPE`) и не убивает процесс сигналом `SIGPIPE`:
 * на Linux это флаг `send`, на Apple — `SO_NOSIGPIPE` на сокете, который ставят `socket` и `accept`. Без защиты
 * процесс тестов завершается по сигналу, и тест не доходит до проверок. Сокеты настоящие (петля `127.0.0.1`).
 */
class SigPipeTest {

    private class Loopback(var client: Int, var server: Int)

    private fun withLoopback(block: (Loopback) -> Unit) {
        socketsInit()
        val pair = Loopback(-1, -1)
        val listener = socket(AF_INET, SOCK_STREAM, 0)
        assertTrue(listener >= 0, "socket: ${getErrno()}")
        try {
            memScoped {
                val address = sockaddrIn(AF_INET.toUShort(), 0u)
                assertEquals(1, inet_pton(AF_INET, "127.0.0.1", address.sin_addr.ptr))
                val size = sizeOf<sockaddr_in>().toUInt()
                assertEquals(0, bind(listener, address.ptr.reinterpret(), size), "bind: ${getErrno()}")
                assertEquals(0, listen(listener, 1), "listen: ${getErrno()}")
                val length = alloc<socklen_tVar>()
                length.value = size.convert()
                assertEquals(0, getsockname(listener, address.ptr.reinterpret(), length.ptr), "getsockname")
                pair.client = socket(AF_INET, SOCK_STREAM, 0)
                assertTrue(pair.client >= 0, "socket: ${getErrno()}")
                assertEquals(0, connect(pair.client, address.ptr.reinterpret(), size), "connect: ${getErrno()}")
                pair.server = accept(listener, null, null)
                assertTrue(pair.server >= 0, "accept: ${getErrno()}")
            }
            block(pair)
        } finally {
            if (pair.client >= 0) close(pair.client)
            if (pair.server >= 0) close(pair.server)
            close(listener)
            socketsCleanup()
        }
    }

    /** Пишем в сокет, пока не получим ошибку: первая запись после закрытия партнёром может пройти, следующая — нет. */
    private fun writesEventuallyFailWithoutSignal(fd: Int) {
        val data = ByteArray(1024)
        var failure = 0
        repeat(100) {
            if (failure == 0) {
                val written = data.usePinned { send(fd, it.addressOf(0), data.size, 0) }
                if (written < 0) failure = getErrno() else usleep(20_000u)
            }
        }
        assertTrue(failure == EPIPE || failure == ECONNRESET, "errno=$failure: запись в закрытое соединение должна давать ошибку")
    }

    /** Сокет, созданный `socket` и подключённый `connect`: сервер закрылся, пишет клиент. */
    @Test
    fun connectedSocketDoesNotKillTheProcess() = withLoopback { pair ->
        close(pair.server)
        pair.server = -1
        writesEventuallyFailWithoutSignal(pair.client)
    }

    /** Сокет, полученный `accept`: клиент закрылся, пишет принятый сокет. */
    @Test
    fun acceptedSocketDoesNotKillTheProcess() = withLoopback { pair ->
        close(pair.client)
        pair.client = -1
        writesEventuallyFailWithoutSignal(pair.server)
    }
}
