@file:OptIn(MqttCoreApi::class)

package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.hostcontract.ContractPeer
import io.github.davidepianca98.client.hostcontract.ContractServer
import io.github.davidepianca98.client.hostcontract.EndOfStream
import io.github.davidepianca98.client.hostcontract.MqttHostContractTest
import io.github.davidepianca98.client.hostcontract.PeerEnd
import io.github.davidepianca98.client.hostcontract.PendingProbe
import io.github.davidepianca98.client.node.bufferOf
import io.github.davidepianca98.client.node.bytesOf
import io.github.davidepianca98.client.node.jsObject
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.test.fail
import kotlin.time.TimeSource
import io.github.davidepianca98.client.node.net.createServer as netCreateServer
import io.github.davidepianca98.client.node.tls.createServer as tlsCreateServer

/** общий контракт хоста ([MqttHostContractTest]) на Node-хосте [NodeHost] + [NodeTransport] (TCP). */
internal class NodeHostContractTest : MqttHostContractTest() {

    override val platform: String = "js-node"

    // Буфер приёма сервера в Node не уменьшить, а приём на loopback Windows вмещает мегабайты: берём с запасом
    override val bigPayload: Int = 64 * 1024 * 1024

    override val writeBackpressureVisible: Boolean = js("process.platform") != "win32"

    override fun newHost(probe: PendingProbe?): MqttHost = NodeHost(
        MqttClock.Monotonic,
        if (probe == null) NodeTransportFactory.Tcp else NodeTransportFactory { NodeTransport(it).also(probe::attach) }
    )

    // Буфер приёма сервера в Node не задаётся: сервер «не читает» — сокет на паузе (pauseOnConnect)
    override suspend fun newServer(smallReceiveBuffer: Boolean): ContractServer = NodeContractServer.listen(tls = null)
}

/** Сервер на 127.0.0.1 (`net` или `tls` при [tls] = `{key, cert}`); сторона соединения стартует на паузе. */
internal class NodeContractServer private constructor(tls: dynamic) : ContractServer {
    private val accepted = Channel<NodePeer>(Channel.UNLIMITED)
    private val peers = ArrayList<NodePeer>()
    private val server: dynamic

    init {
        val options = jsObject()
        options.allowHalfOpen = true
        options.pauseOnConnect = true
        val onConnection: (dynamic) -> Unit = { socket ->
            val peer = NodePeer(socket)
            peers += peer
            accepted.trySend(peer)
        }
        server = if (tls == null) {
            netCreateServer(options, onConnection)
        } else {
            options.key = tls.key
            options.cert = tls.cert
            tlsCreateServer(options, onConnection)
        }
    }

    override var port: Int = 0
        private set

    override suspend fun accept(timeoutMs: Long): ContractPeer =
        withTimeoutOrNull(timeoutMs) { accepted.receive() } ?: fail("нет подключения за $timeoutMs мс")

    override fun close() {
        server.close()
        for (peer in peers) peer.close()
    }

    companion object {
        suspend fun listen(tls: dynamic): NodeContractServer {
            val created = NodeContractServer(tls)
            suspendCancellableCoroutine { cont ->
                created.server.once("error") { e: dynamic -> cont.resumeWithException(e.unsafeCast<Throwable>()) }
                created.server.listen(0, "127.0.0.1") { cont.resume(Unit) }
            }
            created.port = (created.server.address().port as Number).toInt()
            return created
        }
    }
}

/** Сторона сервера: копит прочитанное; читает только после первого запроса теста. */
internal class NodePeer(private val socket: dynamic) : ContractPeer {
    private val chunks = ArrayDeque<ByteArray>()
    private var buffered = 0
    private var ended = false
    private var reset = false
    var errorCode: String? = null
        private set

    init {
        socket.on("data") { chunk: dynamic ->
            val bytes = bytesOf(chunk)
            chunks.addLast(bytes)
            buffered += bytes.size
        }
        socket.pause()
        socket.on("end") { ended = true }
        socket.on("error") { e: dynamic ->
            errorCode = (e?.code as? String) ?: "Error"
            reset = true
        }
    }

    private suspend fun waitFor(timeoutMs: Long, condition: () -> Boolean): Boolean {
        val start = TimeSource.Monotonic.markNow()
        while (!condition()) {
            if (start.elapsedNow().inWholeMilliseconds > timeoutMs) return false
            delay(1)
        }
        return true
    }

    override suspend fun readExactly(count: Int, timeoutMs: Long): ByteArray {
        socket.resume()
        if (!waitFor(timeoutMs) { buffered >= count || ended || reset }) fail("за $timeoutMs мс пришло $buffered из $count байт")
        if (buffered < count) fail("конец потока после $buffered байт из $count (сброс: $reset $errorCode)")
        val out = ByteArray(count)
        var offset = 0
        while (offset < count) {
            val chunk = chunks.removeFirst()
            val take = minOf(chunk.size, count - offset)
            chunk.copyInto(out, offset, 0, take)
            offset += take
            if (take < chunk.size) chunks.addFirst(chunk.copyOfRange(take, chunk.size))
        }
        buffered -= count
        return out
    }

    override suspend fun write(bytes: ByteArray) {
        socket.write(bufferOf(bytes.copyOf()))
    }

    override suspend fun shutdownOutput() {
        socket.end()
    }

    override suspend fun awaitEnd(timeoutMs: Long): EndOfStream {
        socket.resume()
        val done = waitFor(timeoutMs) { ended || reset }
        val extra = buffered
        chunks.clear()
        buffered = 0
        return when {
            reset -> EndOfStream(PeerEnd.Reset, extra)
            done -> EndOfStream(PeerEnd.Fin, extra)
            else -> EndOfStream(PeerEnd.Timeout, extra)
        }
    }

    override fun close() {
        socket.destroy()
    }
}
