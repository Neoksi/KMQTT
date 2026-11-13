package io.github.davidepianca98.client

import io.github.davidepianca98.client.hostcontract.ContractPeer
import io.github.davidepianca98.client.hostcontract.ContractServer
import io.github.davidepianca98.client.hostcontract.EndOfStream
import io.github.davidepianca98.client.hostcontract.PeerEnd
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

/**
 * WebSocket-сервер (RFC 6455) для контракта хоста и тестов WebSocket-транспортов JS — `net`/`tls` сервер на
 * 127.0.0.1, рукопожатие `Upgrade` и разбор кадров клиента. Сторона соединения ([WsPeer]) читает и пишет байты MQTT
 * как полезную нагрузку бинарных кадров, так что сценарии [io.github.davidepianca98.client.hostcontract.MqttHostContractTest]
 * идут без изменений; «FIN» сервера ([WsPeer.shutdownOutput]) — close-кадр 1000 и FIN TCP.
 *
 * [upgradeResponse] — ответ на `Upgrade` по ключу клиента (по умолчанию 101 с `Sec-WebSocket-Protocol: mqtt`).
 */
internal class WsContractServer private constructor(
    tls: dynamic,
    private val upgradeResponse: (key: String) -> String
) : ContractServer {
    private val accepted = Channel<WsPeer>(Channel.UNLIMITED)
    private val peers = ArrayList<WsPeer>()
    private val server: dynamic

    init {
        val options = jsObject()
        options.allowHalfOpen = true
        options.pauseOnConnect = true
        val onConnection: (dynamic) -> Unit = { socket ->
            val peer = WsPeer(socket, upgradeResponse)
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

    override suspend fun accept(timeoutMs: Long): WsPeer =
        withTimeoutOrNull(timeoutMs) { accepted.receive() } ?: fail("нет подключения за $timeoutMs мс")

    override fun close() {
        server.close()
        for (peer in peers) peer.close()
    }

    companion object {
        fun switchingProtocols(key: String, protocol: String? = "mqtt"): String =
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
                "Sec-WebSocket-Accept: ${NodeWebSocketTransport.acceptFor(key)}\r\n" +
                (if (protocol != null) "Sec-WebSocket-Protocol: $protocol\r\n" else "") + "\r\n"

        suspend fun listen(
            tls: dynamic = null,
            upgradeResponse: (key: String) -> String = { switchingProtocols(it) }
        ): WsContractServer {
            val created = WsContractServer(tls, upgradeResponse)
            suspendCancellableCoroutine { cont ->
                created.server.once("error") { e: dynamic -> cont.resumeWithException(e.unsafeCast<Throwable>()) }
                created.server.listen(0, "127.0.0.1") { cont.resume(Unit) }
            }
            created.port = (created.server.address().port as Number).toInt()
            return created
        }
    }
}

/**
 * Серверная сторона WebSocket-соединения. Рукопожатие — сразу по подключению (тест его не ждёт), после него сокет на
 * паузе: кадры читаются только по запросу теста ([readExactly], [awaitEnd]). Close-кадр клиента — ответ тем же кодом и
 * FIN (как у настоящего сервера); ping — pong.
 */
internal class WsPeer(private val socket: dynamic, private val upgradeResponse: (key: String) -> String) : ContractPeer {
    private val chunks = ArrayDeque<ByteArray>()
    private var buffered = 0
    private var ended = false
    private var reset = false
    private var errorCode: String? = null

    /** Заголовки запроса `Upgrade` (до пустой строки). */
    var requestHead: String? = null
        private set
    private var head = ByteArray(0)

    var closeCodeReceived: Int? = null
        private set
    val pongs = ArrayList<ByteArray>()
    private var closeSent = false

    /** Тест начал читать ([readExactly], [awaitEnd]): с этого момента сокет не на паузе. */
    private var reading = false

    // Разбор кадров клиента
    private val frameHeader = ByteArray(14)
    private var headerLength = 0
    private var inPayload = false
    private var opcode = 0
    private var remaining = 0L
    private val mask = ByteArray(4)
    private var maskIndex = 0
    private var control = ByteArray(0)

    init {
        socket.on("data") { chunk: dynamic -> onData(bytesOf(chunk)) }
        socket.on("end") { ended = true }
        socket.on("error") { e: dynamic ->
            errorCode = (e?.code as? String) ?: "Error"
            reset = true
        }
        socket.resume()
    }

    private fun onData(bytes: ByteArray) {
        if (requestHead == null) {
            head += bytes
            val text = head.decodeToString()
            val end = text.indexOf("\r\n\r\n")
            if (end < 0) return
            val request = text.substring(0, end)
            requestHead = request
            val key = request.split("\r\n").firstOrNull { it.lowercase().startsWith("sec-websocket-key:") }
                ?.substringAfter(':')?.trim().orEmpty()
            val response = upgradeResponse(key)
            socket.write(bufferOf(response.encodeToByteArray()))
            if (!response.startsWith("HTTP/1.1 101")) {
                socket.end()
                return
            }
            // Тест уже читает (readExactly до конца рукопожатия) — не ставить на паузу, иначе чтение не возобновится
            if (!reading) socket.pause()
            // Запрос — ASCII: индекс символа совпадает с индексом байта
            val rest = head.copyOfRange(end + 4, head.size)
            head = ByteArray(0)
            if (rest.isNotEmpty()) parse(rest)
            return
        }
        parse(bytes)
    }

    private fun parse(bytes: ByteArray) {
        var i = 0
        while (i < bytes.size) {
            if (!inPayload) {
                frameHeader[headerLength++] = bytes[i++]
                if (headerComplete()) startFrame()
                continue
            }
            val n = minOf(remaining, (bytes.size - i).toLong()).toInt()
            val payload = ByteArray(n)
            for (k in 0 until n) payload[k] = (bytes[i + k].toInt() xor mask[(maskIndex + k) and 3].toInt()).toByte()
            maskIndex = (maskIndex + n) and 3
            i += n
            remaining -= n
            if (opcode == OP_BINARY || opcode == OP_CONTINUATION) {
                if (n > 0) {
                    chunks.addLast(payload)
                    buffered += n
                }
            } else {
                control += payload
            }
            if (remaining == 0L) endFrame()
        }
    }

    private fun headerComplete(): Boolean {
        if (headerLength < 2) return false
        val len7 = frameHeader[1].toInt() and 0x7F
        val need = 2 + 4 + when (len7) {
            126 -> 2
            127 -> 8
            else -> 0
        }
        return headerLength >= need
    }

    private fun startFrame() {
        val b0 = frameHeader[0].toInt() and 0xFF
        val b1 = frameHeader[1].toInt() and 0xFF
        if (b1 and 0x80 == 0) {
            // Кадр клиента без маски — нарушение RFC 6455 §5.1: сервер рвёт соединение
            errorCode = "unmasked client frame"
            reset = true
            socket.destroy()
            return
        }
        opcode = b0 and 0x0F
        var p = 2
        remaining = when (val len7 = b1 and 0x7F) {
            126 -> (((frameHeader[2].toInt() and 0xFF) shl 8) or (frameHeader[3].toInt() and 0xFF)).toLong().also { p = 4 }
            127 -> {
                var value = 0L
                for (k in 2 until 10) value = (value shl 8) or (frameHeader[k].toLong() and 0xFF)
                p = 10
                value
            }
            else -> len7.toLong()
        }
        frameHeader.copyInto(mask, 0, p, p + 4)
        maskIndex = 0
        headerLength = 0
        inPayload = true
        control = ByteArray(0)
        if (remaining == 0L) endFrame()
    }

    private fun endFrame() {
        inPayload = false
        when (opcode) {
            OP_CLOSE -> {
                closeCodeReceived = if (control.size >= 2) {
                    ((control[0].toInt() and 0xFF) shl 8) or (control[1].toInt() and 0xFF)
                } else {
                    NO_CODE
                }
                if (!closeSent) {
                    closeSent = true
                    socket.write(bufferOf(frame(OP_CLOSE, control.copyOf(minOf(2, control.size)))))
                    socket.end()
                }
            }
            OP_PING -> if (!closeSent) socket.write(bufferOf(frame(OP_PONG, control)))
            OP_PONG -> pongs += control
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
        reading = true
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

    /** Бинарный кадр сервера (без маски) с [bytes]. */
    override suspend fun write(bytes: ByteArray) {
        writeFrame(OP_BINARY, bytes)
    }

    fun writeFrame(op: Int, payload: ByteArray) {
        socket.write(bufferOf(frame(op, payload)))
    }

    /** Сырые байты в сокет (кадр, собранный тестом). */
    fun writeRaw(bytes: ByteArray) {
        socket.write(bufferOf(bytes.copyOf()))
    }

    /** Close-кадр 1000 и FIN (чтение остаётся). */
    override suspend fun shutdownOutput() {
        if (!closeSent) {
            closeSent = true
            socket.write(bufferOf(frame(OP_CLOSE, byteArrayOf(0x03, 0xE8.toByte()))))
        }
        socket.end()
    }

    /** Читать и отбрасывать до FIN TCP; данные бинарных кадров — в [EndOfStream.extraBytes]. */
    override suspend fun awaitEnd(timeoutMs: Long): EndOfStream {
        reading = true
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

    companion object {
        const val OP_CONTINUATION = 0x0
        const val OP_TEXT = 0x1
        const val OP_BINARY = 0x2
        const val OP_CLOSE = 0x8
        const val OP_PING = 0x9
        const val OP_PONG = 0xA
        const val NO_CODE = -1

        /** Кадр сервера: FIN, без маски. */
        fun frame(op: Int, payload: ByteArray): ByteArray {
            val size = payload.size
            val lengthBytes = when {
                size <= 125 -> 0
                size <= 0xFFFF -> 2
                else -> 8
            }
            val out = ByteArray(2 + lengthBytes + size)
            out[0] = (0x80 or op).toByte()
            var p = 2
            when (lengthBytes) {
                0 -> out[1] = size.toByte()
                2 -> {
                    out[1] = 126
                    out[p++] = (size shr 8).toByte()
                    out[p++] = size.toByte()
                }
                else -> {
                    out[1] = 127
                    for (shift in 56 downTo 0 step 8) out[p++] = (size.toLong() shr shift).toByte()
                }
            }
            payload.copyInto(out, p)
            return out
        }
    }
}
