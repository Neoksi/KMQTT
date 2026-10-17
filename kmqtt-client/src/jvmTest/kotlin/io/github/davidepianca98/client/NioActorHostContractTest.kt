@file:OptIn(MqttCoreApi::class)

package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.hostcontract.ContractPeer
import io.github.davidepianca98.client.hostcontract.ContractServer
import io.github.davidepianca98.client.hostcontract.EndOfStream
import io.github.davidepianca98.client.hostcontract.MqttHostContractTest
import io.github.davidepianca98.client.hostcontract.PeerEnd
import io.github.davidepianca98.client.hostcontract.PendingProbe
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runInterruptible
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.net.SocketTimeoutException
import kotlin.test.fail

/** SEVEN-250: общий контракт хоста ([MqttHostContractTest]) на JVM-хосте `NioActorHost` + `NioTransport`. */
internal class NioActorHostContractTest : MqttHostContractTest() {

    override val platform: String = "jvm"

    override fun newHost(probe: PendingProbe?): MqttHost {
        val factory = if (probe == null) {
            NioTransportFactory.Tcp
        } else {
            NioTransportFactory { ctx -> NioTransport(ctx).also(probe::attach) }
        }
        return NioActorHost(nextHostThreadName(), MqttClock.Monotonic, factory, NioActorHost.DEFAULT_READ_BUFFER)
    }

    override suspend fun newServer(smallReceiveBuffer: Boolean): ContractServer = JvmContractServer(smallReceiveBuffer)

    private class JvmContractServer(smallReceiveBuffer: Boolean) : ContractServer {
        private val server = ServerSocket().apply {
            if (smallReceiveBuffer) receiveBufferSize = 4 * 1024
            bind(InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 50)
        }
        private val peers = ArrayList<Socket>()

        override val port: Int = server.localPort

        override suspend fun accept(timeoutMs: Long): ContractPeer = runInterruptible(Dispatchers.IO) {
            server.soTimeout = timeoutMs.toInt()
            val socket = try {
                server.accept()
            } catch (_: SocketTimeoutException) {
                fail("нет подключения за $timeoutMs мс")
            }
            synchronized(peers) { peers += socket }
            JvmPeer(socket)
        }

        override fun close() {
            runCatching { server.close() }
            synchronized(peers) { peers.forEach { runCatching { it.close() } } }
        }
    }

    private class JvmPeer(private val socket: Socket) : ContractPeer {
        override suspend fun readExactly(count: Int, timeoutMs: Long): ByteArray = runInterruptible(Dispatchers.IO) {
            socket.soTimeout = timeoutMs.toInt()
            val out = ByteArray(count)
            var offset = 0
            val input = socket.getInputStream()
            while (offset < count) {
                val n = input.read(out, offset, count - offset)
                if (n < 0) fail("EOF после $offset байт из $count")
                offset += n
            }
            out
        }

        override suspend fun write(bytes: ByteArray) = runInterruptible(Dispatchers.IO) {
            socket.getOutputStream().write(bytes)
            socket.getOutputStream().flush()
        }

        override suspend fun shutdownOutput() = runInterruptible(Dispatchers.IO) { socket.shutdownOutput() }

        override suspend fun awaitEnd(timeoutMs: Long): EndOfStream = runInterruptible(Dispatchers.IO) {
            socket.soTimeout = timeoutMs.toInt()
            val buffer = ByteArray(16 * 1024)
            var extra = 0
            try {
                while (true) {
                    val n = socket.getInputStream().read(buffer)
                    if (n < 0) return@runInterruptible EndOfStream(PeerEnd.Fin, extra)
                    extra += n
                }
                @Suppress("UNREACHABLE_CODE")
                EndOfStream(PeerEnd.Fin, extra)
            } catch (_: SocketTimeoutException) {
                EndOfStream(PeerEnd.Timeout, extra)
            } catch (_: SocketException) {
                EndOfStream(PeerEnd.Reset, extra)
            }
        }

        override fun close() {
            runCatching { socket.close() }
        }
    }
}
