package io.github.davidepianca98.socket.tcp

import io.github.davidepianca98.close
import io.github.davidepianca98.getEagain
import io.github.davidepianca98.getErrno
import io.github.davidepianca98.getEwouldblock
import io.github.davidepianca98.recv
import io.github.davidepianca98.shutdown
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.convert
import kotlinx.cinterop.usePinned
import io.github.davidepianca98.send
import io.github.davidepianca98.socket.IOException
import io.github.davidepianca98.socket.PendingSendQueue
import io.github.davidepianca98.socket.isTransientSocketError
import io.github.davidepianca98.socket.SocketClosedException
import io.github.davidepianca98.socket.SocketInterface

public actual open class Socket(
    protected val socket: Int,
    private val writeRequest: MutableList<Int>?,
    private val buffer: ByteArray
) : SocketInterface {

    /**
     * Исходящие данные, которые сокет сейчас не принял. Порядок сохраняется: пока очередь не пуста,
     * новые данные уходят только за ней, а не в обход.
     */
    private val pendingSendData = PendingSendQueue()

    actual override fun send(data: UByteArray) {
        if (!pendingSendData.submit(data, ::sendChunk)) requestWrite()
    }

    actual override fun sendRemaining() {
        if (!pendingSendData.drain(::sendChunk)) requestWrite()
    }

    /** Попросить цикл брокера сообщить, когда сокет станет доступен на запись (без дублей в списке). */
    private fun requestWrite() {
        val requests = writeRequest ?: return
        if (!requests.contains(socket)) requests.add(socket)
    }

    /** Одна попытка `send`: число принятых байтов, `0` — сокет сейчас не принимает (EAGAIN); иначе исключение. */
    private fun sendChunk(data: UByteArray): Int {
        if (data.isEmpty()) return 0
        val length = data.toByteArray().usePinned { pinned -> send(socket, pinned.addressOf(0), data.size, 0) }
        if (length >= 0) return length
        val error = getErrno()
        if (isTransient(error)) return 0
        close()
        throw IOException("Error in send $error")
    }

    /** `EAGAIN`/`EWOULDBLOCK` или потерянный код (0): повторить позже, соединение не закрывать. */
    private fun isTransient(error: Int): Boolean = isTransientSocketError(error, getEagain(), getEwouldblock())

    actual override fun read(): UByteArray? {
        buffer.usePinned { pinned ->
            val length = recv(socket.convert(), pinned.addressOf(0), buffer.size.convert(), 0)
            when {
                length == 0 -> {
                    close()
                    throw SocketClosedException()
                }
                length > 0 -> {
                    return pinned.get().toUByteArray().copyOfRange(0, length)
                }
                else -> {
                    val error = getErrno()
                    if (!isTransient(error)) {
                        close()
                        throw IOException("Error in recv: $error")
                    } else {
                        return null
                    }
                }
            }
        }
    }

    actual override fun close() {
        shutdown(socket)
        close(socket)
    }

}