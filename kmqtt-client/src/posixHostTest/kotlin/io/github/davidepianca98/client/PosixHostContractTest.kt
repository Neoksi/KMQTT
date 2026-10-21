@file:OptIn(MqttCoreApi::class)

package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.hostcontract.ContractPeer
import io.github.davidepianca98.client.hostcontract.ContractServer
import io.github.davidepianca98.client.hostcontract.EndOfStream
import io.github.davidepianca98.client.hostcontract.MqttHostContractTest
import io.github.davidepianca98.client.hostcontract.PeerEnd
import io.github.davidepianca98.client.hostcontract.PendingProbe
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.sizeOf
import kotlinx.cinterop.usePinned
import kotlinx.coroutines.delay
import platform.posix.AF_INET
import platform.posix.IPPROTO_TCP
import platform.posix.SHUT_WR
import platform.posix.SOCK_STREAM
import platform.posix.accept
import platform.posix.bind
import platform.posix.listen
import platform.posix.shutdown
import platform.posix.sockaddr_in
import platform.posix.socket
import kotlin.test.fail
import kotlin.time.TimeSource

/** Общий контракт хоста ([MqttHostContractTest]) на хосте POSIX [PosixHost] + [PosixTransport] (Linux, Apple). */
internal class PosixHostContractTest : MqttHostContractTest() {

    override val platform: String = posixTestPlatform

    override fun newHost(probe: PendingProbe?): MqttHost {
        val factory = if (probe == null) {
            PosixTransportFactory.Tcp
        } else {
            PosixTransportFactory { ctx -> PosixTransport(ctx).also(probe::attach) }
        }
        return PosixHost(nextHostThreadName(), MqttClock.Monotonic, factory, PosixHost.DEFAULT_READ_BUFFER)
    }

    override suspend fun newServer(smallReceiveBuffer: Boolean): ContractServer = PosixServer(smallReceiveBuffer)
}

/**
 * Сервер на 127.0.0.1 (или на `::1` при [ipv6]) на неблокирующих сокетах POSIX; ожидания — опросом с
 * `delay` (поток не блокируется).
 */
internal class PosixServer(smallReceiveBuffer: Boolean, ipv6: Boolean = false) : ContractServer {
    private val listener: Int
    private val peers = ArrayList<PosixPeer>()

    override val port: Int

    init {
        val loopback6 = if (ipv6) {
            (PosixSockets.resolve("::1", 0) as? PosixSockets.Resolved.Found)?.address ?: error("resolve ::1")
        } else {
            null
        }
        val s = socket(loopback6?.family ?: AF_INET, SOCK_STREAM, IPPROTO_TCP)
        check(s >= 0) { "socket: ${PosixSockets.errorName(PosixSockets.lastError())}" }
        // Буфер приёма слушающего сокета наследуют принятые соединения
        if (smallReceiveBuffer) PosixSockets.receiveBuffer(s, 4 * 1024)
        memScoped {
            if (loopback6 != null) {
                val bound = loopback6.bytes.usePinned {
                    bind(s, it.addressOf(0).reinterpret(), loopback6.bytes.size.convert())
                }
                if (bound != 0) {
                    val reason = PosixSockets.errorName(PosixSockets.lastError())
                    platform.posix.close(s)
                    error("bind ::1: $reason")
                }
            } else {
                val addr = alloc<sockaddr_in>()
                PosixSockets.loopback(addr, 0u)
                check(bind(s, addr.ptr.reinterpret(), sizeOf<sockaddr_in>().convert()) == 0) {
                    "bind: ${PosixSockets.errorName(PosixSockets.lastError())}"
                }
            }
        }
        check(listen(s, 128) == 0) { "listen" }
        PosixSockets.setNonBlocking(s)
        listener = s
        port = PosixSockets.portOf(s)
    }

    override suspend fun accept(timeoutMs: Long): ContractPeer {
        val start = TimeSource.Monotonic.markNow()
        while (true) {
            val s = accept(listener, null, null)
            if (s >= 0) {
                // Запись в сброшенное клиентом соединение — EPIPE, а не SIGPIPE (на Apple — SO_NOSIGPIPE на сокете)
                check(suppressSigPipe(s) == 0) { "SO_NOSIGPIPE" }
                PosixSockets.setNonBlocking(s)
                return PosixPeer(s).also { peers += it }
            }
            val error = PosixSockets.lastError()
            if (error != 0 && !PosixSockets.isRetry(error)) fail("accept: ${PosixSockets.errorName(error)}")
            if (start.elapsedNow().inWholeMilliseconds > timeoutMs) fail("нет подключения за $timeoutMs мс")
            delay(1)
        }
    }

    override fun close() {
        platform.posix.close(listener)
        for (peer in peers) peer.close()
    }

    companion object {
        /** Сервер на `::1` или `null`, если IPv6 на петле недоступен (нет стека IPv6, `::1` не назначен — Docker). */
        fun ipv6OrNull(): PosixServer? = try {
            PosixServer(false, ipv6 = true)
        } catch (e: IllegalStateException) {
            println("IPv6 на петле недоступен (${e.message}): тест пропущен")
            null
        }
    }
}

internal class PosixPeer(private var socket: Int) : ContractPeer {
    private val buffer = ByteArray(64 * 1024)

    override suspend fun readExactly(count: Int, timeoutMs: Long): ByteArray {
        val out = ByteArray(count)
        var offset = 0
        val start = TimeSource.Monotonic.markNow()
        while (offset < count) {
            val chunk = if (count - offset < buffer.size) ByteArray(count - offset) else buffer
            val n = PosixSockets.recvSome(socket, chunk)
            when {
                n > 0 -> {
                    chunk.copyInto(out, offset, 0, n)
                    offset += n
                }
                n == 0 -> fail("EOF после $offset байт из $count")
                else -> {
                    if (!PosixSockets.isRetry(-n)) fail("recv: ${PosixSockets.errorName(-n)} после $offset байт")
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
            val n = PosixSockets.sendSome(socket, bytes, offset, bytes.size - offset)
            if (n < 0) {
                if (!PosixSockets.isRetry(-n)) fail("send: ${PosixSockets.errorName(-n)}")
                delay(1)
            } else {
                offset += n
            }
        }
    }

    override suspend fun shutdownOutput() {
        shutdown(socket, SHUT_WR)
    }

    override suspend fun awaitEnd(timeoutMs: Long): EndOfStream {
        var extra = 0
        val start = TimeSource.Monotonic.markNow()
        while (true) {
            val n = PosixSockets.recvSome(socket, buffer)
            when {
                n > 0 -> extra += n
                n == 0 -> return EndOfStream(PeerEnd.Fin, extra)
                else -> {
                    if (!PosixSockets.isRetry(-n)) return EndOfStream(PeerEnd.Reset, extra)
                    if (start.elapsedNow().inWholeMilliseconds > timeoutMs) return EndOfStream(PeerEnd.Timeout, extra)
                    delay(1)
                }
            }
        }
    }

    override fun close() {
        val s = socket
        socket = PosixSockets.INVALID
        if (s != PosixSockets.INVALID) platform.posix.close(s)
    }
}
