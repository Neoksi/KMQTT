@file:OptIn(MqttCoreApi::class)

package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.hostcontract.ContractPeer
import io.github.davidepianca98.client.hostcontract.ContractServer
import io.github.davidepianca98.client.hostcontract.EndOfStream
import io.github.davidepianca98.client.hostcontract.MqttHostContractTest
import io.github.davidepianca98.client.hostcontract.PeerEnd
import io.github.davidepianca98.client.hostcontract.PendingProbe
import kotlinx.cinterop.alloc
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.sizeOf
import kotlinx.coroutines.delay
import platform.posix.AF_INET
import platform.posix.IPPROTO_TCP
import platform.posix.SD_SEND
import platform.posix.SOCK_STREAM
import platform.posix.accept
import platform.posix.bind
import platform.posix.closesocket
import platform.posix.listen
import platform.posix.shutdown
import platform.posix.sockaddr_in
import platform.posix.socket
import kotlin.test.fail
import kotlin.time.TimeSource

/** SEVEN-250: общий контракт хоста ([MqttHostContractTest]) на хосте mingwX64 [WinSockHost] + [WinSockTransport]. */
internal class WinSockHostContractTest : MqttHostContractTest() {

    override val platform: String = "mingwX64"

    override fun newHost(probe: PendingProbe?): MqttHost {
        val factory = if (probe == null) {
            WinSockTransportFactory.Tcp
        } else {
            WinSockTransportFactory { ctx -> WinSockTransport(ctx).also(probe::attach) }
        }
        return WinSockHost(nextHostThreadName(), MqttClock.Monotonic, factory, WinSockHost.DEFAULT_READ_BUFFER)
    }

    override suspend fun newServer(smallReceiveBuffer: Boolean): ContractServer = WinSockServer(smallReceiveBuffer)
}

/** Сервер на 127.0.0.1 на неблокирующих сокетах WinSock; ожидания — опросом с `delay` (поток не блокируется). */
internal class WinSockServer(smallReceiveBuffer: Boolean) : ContractServer {
    private val listener: ULong
    private val peers = ArrayList<WinSockPeer>()

    override val port: Int

    init {
        WinSock.ensureStarted()
        val s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)
        check(s != WinSock.INVALID) { "socket: ${WinSock.errorName(WinSock.lastError())}" }
        if (smallReceiveBuffer) WinSock.receiveBuffer(s, 4 * 1024)
        memScoped {
            val addr = alloc<sockaddr_in>()
            WinSock.loopback(addr, 0u)
            check(bind(s, addr.ptr.reinterpret(), sizeOf<sockaddr_in>().toInt()) == 0) { "bind" }
        }
        check(listen(s, 50) == 0) { "listen" }
        WinSock.setNonBlocking(s)
        listener = s
        port = WinSock.portOf(s)
    }

    override suspend fun accept(timeoutMs: Long): ContractPeer {
        val start = TimeSource.Monotonic.markNow()
        while (true) {
            val s = accept(listener, null, null)
            if (s != WinSock.INVALID) {
                WinSock.setNonBlocking(s)
                return WinSockPeer(s).also { peers += it }
            }
            val error = WinSock.lastError()
            if (error != WinSock.WSAEWOULDBLOCK) fail("accept: ${WinSock.errorName(error)}")
            if (start.elapsedNow().inWholeMilliseconds > timeoutMs) fail("нет подключения за $timeoutMs мс")
            delay(1)
        }
    }

    override fun close() {
        closesocket(listener)
        for (peer in peers) peer.close()
    }
}

internal class WinSockPeer(private var socket: ULong) : ContractPeer {
    private val buffer = ByteArray(64 * 1024)

    override suspend fun readExactly(count: Int, timeoutMs: Long): ByteArray {
        val out = ByteArray(count)
        var offset = 0
        val start = TimeSource.Monotonic.markNow()
        while (offset < count) {
            val chunk = if (count - offset < buffer.size) ByteArray(count - offset) else buffer
            val n = WinSock.recvSome(socket, chunk)
            when {
                n > 0 -> {
                    chunk.copyInto(out, offset, 0, n)
                    offset += n
                }
                n == 0 -> fail("EOF после $offset байт из $count")
                else -> {
                    if (-n != WinSock.WSAEWOULDBLOCK) fail("recv: ${WinSock.errorName(-n)} после $offset байт")
                    if (start.elapsedNow().inWholeMilliseconds > timeoutMs) fail("за $timeoutMs мс пришло $offset из $count")
                    delay(1)
                }
            }
        }
        return out
    }

    override suspend fun write(bytes: ByteArray) {
        var offset = 0
        while (offset < bytes.size) {
            val n = WinSock.sendSome(socket, bytes, offset, bytes.size - offset)
            if (n < 0) {
                if (-n != WinSock.WSAEWOULDBLOCK) fail("send: ${WinSock.errorName(-n)}")
                delay(1)
            } else {
                offset += n
            }
        }
    }

    override suspend fun shutdownOutput() {
        shutdown(socket, SD_SEND)
    }

    override suspend fun awaitEnd(timeoutMs: Long): EndOfStream {
        var extra = 0
        val start = TimeSource.Monotonic.markNow()
        while (true) {
            val n = WinSock.recvSome(socket, buffer)
            when {
                n > 0 -> extra += n
                n == 0 -> return EndOfStream(PeerEnd.Fin, extra)
                else -> {
                    if (-n != WinSock.WSAEWOULDBLOCK) return EndOfStream(PeerEnd.Reset, extra)
                    if (start.elapsedNow().inWholeMilliseconds > timeoutMs) return EndOfStream(PeerEnd.Timeout, extra)
                    delay(1)
                }
            }
        }
    }

    override fun close() {
        val s = socket
        socket = WinSock.INVALID
        if (s != WinSock.INVALID) closesocket(s)
    }
}
