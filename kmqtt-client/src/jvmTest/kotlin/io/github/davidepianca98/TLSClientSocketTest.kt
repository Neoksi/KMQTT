package io.github.davidepianca98

import io.github.davidepianca98.socket.tls.TLSClientSettings
import java.net.ServerSocket
import java.net.SocketTimeoutException
import java.nio.channels.SocketChannel
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

/** A failed TLS client socket creation must not leave an open socket channel (file descriptor) behind. */
class TLSClientSocketTest {

    @Test
    fun failedConnectClosesTheChannel() {
        val port = ServerSocket(0).use { it.localPort } // Nobody listens there any more: connection refused
        val channel = SocketChannel.open()
        assertFailsWith<Exception> { TLSClientSocket.openChannel("127.0.0.1", port, 1_000, channel) }
        assertFalse(channel.isOpen)
    }

    @Test
    fun invalidCertificateOpensNoConnection() {
        ServerSocket(0).use { server ->
            server.soTimeout = 500
            assertFailsWith<Exception> {
                TLSClientSocket(
                    "127.0.0.1",
                    server.localPort,
                    1024,
                    250,
                    1_000,
                    TLSClientSettings(serverCertificate = "missing-certificate.pem"),
                    {}
                )
            }
            // Either no connection at all or a connection the client has already closed (a read timeout fails)
            val accepted =
                try {
                    server.accept()
                } catch (_: SocketTimeoutException) {
                    null
                }
            accepted?.use {
                it.soTimeout = 1_000
                assertEquals(-1, it.getInputStream().read(), "connection left open")
            }
        }
    }
}
