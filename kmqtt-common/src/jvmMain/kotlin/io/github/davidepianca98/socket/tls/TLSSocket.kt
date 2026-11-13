package io.github.davidepianca98.socket.tls

import io.github.davidepianca98.socket.IOException
import io.github.davidepianca98.socket.SocketClosedException
import io.github.davidepianca98.socket.tcp.Socket
import io.github.davidepianca98.toUByteArray
import java.nio.BufferOverflowException
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.SocketChannel
import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLEngineResult
import javax.net.ssl.SSLException


public actual open class TLSSocket(
    channel: SocketChannel,
    key: SelectionKey?,
    sendBuffer1: ByteBuffer,
    private var receiveBuffer1: ByteBuffer,
    private var sendAppBuffer: ByteBuffer,
    private var receiveAppBuffer: ByteBuffer,
    protected val engine: SSLEngine
) : Socket(channel, key, sendBuffer1, receiveBuffer1) {

    init {
        try {
            engine.beginHandshake()
            runHandshake()
        } catch (e: Throwable) {
            // The channel belongs to this socket from now on: a failed handshake start must not leak it
            close()
            throw e
        }
    }

    private fun handleReceiveBufferUnderflow() {
        if (engine.session.packetBufferSize > receiveBuffer1.capacity()) {
            val newBuffer = ByteBuffer.allocate(engine.session.packetBufferSize)
            receiveBuffer1.flip()
            newBuffer.put(receiveBuffer1)
            receiveBuffer1 = newBuffer
        }
    }

    private fun handleReceiveBufferOverflow() {
        val size = engine.session.applicationBufferSize + receiveAppBuffer.position()
        val newBuffer = ByteBuffer.allocate(size)
        receiveAppBuffer.flip()
        newBuffer.put(receiveAppBuffer)
        receiveAppBuffer = newBuffer
    }

    private fun send0(data: UByteArray) {
        sendAppBuffer.clear()
        val dataByteArray = data.toByteArray()
        try {
            sendAppBuffer.put(dataByteArray)
        } catch (e: BufferOverflowException) {
            sendAppBuffer = ByteBuffer.allocate(sendAppBuffer.capacity() + dataByteArray.size)
            sendAppBuffer.put(dataByteArray)
        }
        sendAppBuffer.flip()
        try {
            var wrapCapacity = engine.session.packetBufferSize
            // One wrap call produces one TLS record: repeat until all application data is wrapped. The records go
            // through Socket.send, which owns the send buffer (it keeps the bytes the channel did not accept yet)
            do {
                val wrapped = ByteBuffer.allocate(wrapCapacity)
                val result = engine.wrap(sendAppBuffer, wrapped)
                @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
                when (result.status) {
                    SSLEngineResult.Status.BUFFER_UNDERFLOW -> {
                        throw SSLException("Buffer Underflow in wrap")
                    }
                    SSLEngineResult.Status.BUFFER_OVERFLOW -> {
                        wrapCapacity = maxOf(engine.session.packetBufferSize, wrapCapacity * 2)
                    }
                    SSLEngineResult.Status.OK -> {
                        if (sendAppBuffer.hasRemaining() && result.bytesConsumed() == 0 && result.bytesProduced() == 0) {
                            throw SSLException("Wrap made no progress with unsent data")
                        }
                        wrapped.flip()
                        super.send(wrapped.toUByteArray())
                    }
                    SSLEngineResult.Status.CLOSED -> {
                        throw SocketClosedException()
                    }
                }
            } while (result.status != SSLEngineResult.Status.OK || sendAppBuffer.hasRemaining())
        } catch (e: SocketClosedException) {
            engine.closeOutbound()
            close()
            throw e
        } catch (e: SSLException) {
            e.printStackTrace()
            engine.closeOutbound()
            close()
            throw IOException(e.message)
        }
    }

    override fun send(data: UByteArray) {
        send0(data)
    }

    private fun runHandshake(): Boolean {
        while (engine.handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED
            && engine.handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
        ) {
            @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
            when (engine.handshakeStatus) {
                SSLEngineResult.HandshakeStatus.NEED_WRAP -> {
                    send0(UByteArray(0))
                }
                SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING -> return true
                SSLEngineResult.HandshakeStatus.FINISHED -> return true
                SSLEngineResult.HandshakeStatus.NEED_TASK -> {
                    do {
                        val task = engine.delegatedTask
                        task?.run()
                    } while (task != null)
                    if (engine.handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_TASK) {
                        throw Exception("Handshake shouldn't need additional tasks")
                    }
                }
                SSLEngineResult.HandshakeStatus.NEED_UNWRAP -> {
                    if (!read0()) {
                        return false
                    }
                }
                else -> throw Exception("Unknown Handshake Status")
            }
        }
        return true
    }

    private fun read0(): Boolean {
        try {
            do {
                receiveBuffer1.flip()
                val result = engine.unwrap(receiveBuffer1, receiveAppBuffer)
                receiveBuffer1.compact()
                @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
                when (result.status) {
                    SSLEngineResult.Status.OK -> {}
                    SSLEngineResult.Status.BUFFER_UNDERFLOW -> {
                        handleReceiveBufferUnderflow()
                        if (super.readToBuffer() == 0) {
                            return false
                        }
                    }
                    SSLEngineResult.Status.BUFFER_OVERFLOW -> {
                        handleReceiveBufferOverflow()
                    }
                    SSLEngineResult.Status.CLOSED -> {
                        throw SocketClosedException()
                    }
                }
            } while (result.status != SSLEngineResult.Status.OK || (engine.handshakeStatus == SSLEngineResult.HandshakeStatus.FINISHED || engine.handshakeStatus == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING))
        } catch (e: SocketClosedException) {
            engine.closeOutbound()
            close()
            throw e
        } catch (e: SSLException) {
            e.printStackTrace()
            engine.closeOutbound()
            send0(UByteArray(0))
            close()
            throw IOException(e.message)
        }
        return true
    }

    override fun read(): UByteArray? {
        runHandshake()
        read0()
        receiveAppBuffer.flip()
        val result = receiveAppBuffer.toUByteArray()
        return if (result.isEmpty()) {
            null
        } else {
            result
        }
    }
}