package io.github.davidepianca98.socket

import io.github.davidepianca98.accept
import io.github.davidepianca98.bind
import io.github.davidepianca98.close
import io.github.davidepianca98.connect
import io.github.davidepianca98.getErrno
import io.github.davidepianca98.getaddrinfo
import io.github.davidepianca98.listen
import io.github.davidepianca98.set_non_blocking
import io.github.davidepianca98.socket
import io.github.davidepianca98.socketsCleanup
import io.github.davidepianca98.socketsInit
import kotlinx.cinterop.memScoped
import platform.posix.AF_INET
import platform.posix.SOCK_STREAM

/**
 * Пара соединённых TCP-сокетов на петле 127.0.0.1 (оба неблокирующие), [client] — исходящий,
 * [server] — принятый. Закрытие дескрипторов — дело сокетов-обёрток (`Socket.close`), [dispose] закрывает только то,
 * что обёрткам не отдали, и делает `socketsCleanup`.
 */
internal class TestSocketPair private constructor(val client: Int, val server: Int) {

    fun dispose(closeSockets: Boolean = false) {
        if (closeSockets) {
            close(client)
            close(server)
        }
        socketsCleanup()
    }

    companion object {
        fun open(): TestSocketPair {
            socketsInit()
            val listener = socket(AF_INET, SOCK_STREAM, 0)
            check(listener >= 0) { "socket: ${getErrno()}" }
            try {
                memScoped {
                    val any = getaddrinfo("127.0.0.1", "0") ?: error("getaddrinfo 127.0.0.1")
                    check(bind(listener, any.first, any.second) == 0) { "bind: ${getErrno()}" }
                }
                check(listen(listener, 1) == 0) { "listen: ${getErrno()}" }
                val port = TestNet.localPort(listener)
                val client = socket(AF_INET, SOCK_STREAM, 0)
                check(client >= 0) { "socket: ${getErrno()}" }
                memScoped {
                    val target = getaddrinfo("127.0.0.1", port.toString()) ?: error("getaddrinfo 127.0.0.1")
                    check(connect(client, target.first, target.second) == 0) { "connect: ${getErrno()}" }
                }
                val server = accept(listener, null, null)
                check(server >= 0) { "accept: ${getErrno()}" }
                check(set_non_blocking(client) == 0 && set_non_blocking(server) == 0) { "non-blocking" }
                return TestSocketPair(client, server)
            } finally {
                close(listener)
            }
        }
    }
}
