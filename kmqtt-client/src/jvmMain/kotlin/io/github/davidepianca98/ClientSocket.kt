package io.github.davidepianca98

import io.github.davidepianca98.socket.tcp.Socket
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.SocketChannel

public actual class ClientSocket actual constructor(
    address: String,
    port: Int,
    maximumPacketSize: Int,
    private val readTimeOut: Int,
    connectTimeOut: Int,
    checkCallback: () -> Unit
) : Socket(
    SocketChannel.open(),
    null,
    ByteBuffer.allocate(maximumPacketSize),
    ByteBuffer.allocate(maximumPacketSize)
) {

    private val selector = Selector.open()

    init {
        try {
            channel.socket().connect(InetSocketAddress(address, port), connectTimeOut)
            channel.configureBlocking(false)
            channel.register(selector, SelectionKey.OP_READ)

            if (!channel.isConnected) {
                throw Exception("Connect timeout expired")
            }
        } catch (e: Throwable) {
            // A failed connection attempt must not leak the channel and the selector (file descriptors)
            close()
            throw e
        }
    }

    override fun close() {
        try {
            super.close()
        } finally {
            selector.close()
        }
    }

    override fun wakeup() {
        try {
            selector.wakeup()
        } catch (_: Exception) {
            // Closed concurrently: nothing to wake up
        }
    }

    override fun read(): UByteArray? {
        val count = selector.select(readTimeOut.toLong())
        return if (count > 0) {
            selector.selectedKeys().clear()
            super.read()
        } else {
            null
        }
    }
}