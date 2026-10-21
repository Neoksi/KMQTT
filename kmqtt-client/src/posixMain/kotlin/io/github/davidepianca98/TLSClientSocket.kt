package io.github.davidepianca98

import kotlinx.cinterop.*
import platform.posix.*
import io.github.davidepianca98.socket.IOException
import io.github.davidepianca98.socket.tls.TLSClientSettings
import io.github.davidepianca98.socket.tls.TLSSocket

/**
 * TLS-сокет старого клиента на native. Семейство сокета — по разрешённому адресу, как у [ClientSocket].
 */
public actual class TLSClientSocket private constructor(
    target: NativeSocketTarget,
    maximumPacketSize: Int,
    private val readTimeOut: Int,
    private val connectTimeOut: Int,
    tlsSettings: TLSClientSettings,
    host: String
) : TLSSocket(
    target.openSocket(),
    TLSClientEngine(tlsSettings, host),
    null,
    ByteArray(maximumPacketSize)
) {

    public actual constructor(
        address: String,
        port: Int,
        maximumPacketSize: Int,
        readTimeOut: Int,
        connectTimeOut: Int,
        tlsSettings: TLSClientSettings,
        checkCallback: () -> Unit
    ) : this(
        NativeSocketTarget.resolve(address, port),
        maximumPacketSize,
        readTimeOut,
        connectTimeOut,
        tlsSettings,
        address
    )

    private val readfds = nativeHeap.alloc<fd_set>()

    init {
        memScoped {
            if (socket == -1) {
                socketsCleanup()
                throw IOException("Invalid socket, error ${getErrno()}")
            }

            if (set_send_socket_timeout(socket, connectTimeOut.convert()) == -1) {
                socketsCleanup()
                throw IOException("Socket connect timeout set failed, error ${getErrno()}")
            }

            if (target.connectTo(socket) == -1) {
                socketsCleanup()
                throw IOException("Socket connect failed, error ${getErrno()}")
            }

            if (set_non_blocking(socket) == -1) {
                socketsCleanup()
                throw IOException("Failed ioctlsocket for non blocking with error ${getErrno()}")
            }
        }
    }

    override fun read(): UByteArray? {
        // Данные, расшифрованные во время отправки, отдаются сразу, без ожидания готовности сокета
        if (hasBufferedData) return super.read()
        memScoped {
            fdZero(readfds.ptr)
            fdSet(socket.convert(), readfds.ptr)

            select(
                socket + 1,
                readfds.ptr,
                null,
                null,
                readTimeOut.toLong()
            )

            if (fdIsSet(socket.convert(), readfds.ptr) != 0) {
                return super.read()
            } else {
                return null
            }
        }
    }

    public actual val handshakeComplete: Boolean
        get() = engine.isInitFinished
}
