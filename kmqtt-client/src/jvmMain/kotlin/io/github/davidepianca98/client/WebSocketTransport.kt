package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.socket.tls.TLSClientSettings
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.security.MessageDigest
import java.security.SecureRandom
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

/**
 * Фабрика WebSocket-транспорта для `NioActorHost` (SEVEN-248): `ws://` — `WebSocketTransport(NioTransport)`, `wss://`
 * (при [tls]) — `WebSocketTransport(TlsTransport(NioTransport))` с теми же настройками TLS и [verifyHostname], что у
 * [NioTransportFactory.Companion.tls]. Настройки не печатаются и не попадают в ошибки.
 */
@MqttCoreApi
public fun NioTransportFactory.Companion.webSocket(
    webSocket: MqttWebSocket,
    tls: TLSClientSettings? = null,
    verifyHostname: Boolean = true
): NioTransportFactory = NioTransportFactory { context ->
    val tcp = NioTransport(context)
    val inner: MqttTransport = if (tls == null) tcp else TlsTransport(tcp, tls, verifyHostname)
    WebSocketTransport(inner, webSocket, secure = tls != null)
}

/**
 * MQTT поверх WebSocket (RFC 6455, подпротокол `mqtt`; SEVEN-248) над транспортом [inner] (`NioTransport` или
 * `TlsTransport`). Реализация на JDK, без библиотек. Все методы — только на потоке хоста.
 *
 * - Подключение: [inner] подключается (у TLS — с рукопожатием), затем одним `write` уходит HTTP-запрос `Upgrade`
 *   (путь и дополнительные заголовки из [settings], `Sec-WebSocket-Key`, `Sec-WebSocket-Version: 13`,
 *   `Sec-WebSocket-Protocol: mqtt`). Ответ проверяется: статус 101, `Upgrade: websocket`, `Connection: upgrade`,
 *   `Sec-WebSocket-Accept` по ключу, подпротокол — `mqtt` или не указан (другой — ошибка), без расширений.
 *   [TransportEvent.Connected] — только после ответа; срок [connect] покрывает резолв, TCP, TLS и `Upgrade`
 *   ([MqttError.ConnectTimeout]). Заголовки ответа — не больше [MAX_RESPONSE_HEADER] байт; байты после них — уже кадры.
 * - Исходящие: каждый [write] — один бинарный кадр с FIN (маска из [SecureRandom] на каждый кадр); до `Upgrade`
 *   данные ждут в очереди. Исходящей фрагментации нет: встроенный брокер форка не принимает кадры продолжения.
 * - Входящие: полезная нагрузка бинарных кадров и кадров продолжения отдаётся событиями [TransportEvent.Bytes] по мере
 *   прихода, без сборки кадра целиком (память — как у TCP: одно чтение сокета); границы пакетов MQTT с границами кадров
 *   не связаны (MQTT 6.0). Ping — ответ pong с той же нагрузкой; pong — пропускается. Это пинги WebSocket, к PINGREQ
 *   MQTT они отношения не имеют (keep-alive MQTT идёт своими пакетами внутри бинарных кадров).
 * - Нарушения протокола сервером — [TransportEvent.Failed] с [MqttError.Io] `kind = "websocket"`: маскированный кадр,
 *   биты RSV, текстовый кадр (MQTT-6.0.0-1), неизвестный opcode, управляющий кадр с FIN = 0 или длиннее 125 байт,
 *   продолжение без начала или новый кадр данных посреди фрагментированного, 64-битная длина со старшим битом.
 *   Отказ `Upgrade` — тот же `kind`; в тексте только код статуса HTTP (не причина, не заголовки и не тело: в них
 *   бывает адрес). Ошибки [inner] (`UnknownHost`, `ConnectTimeout`, `Io("ssl")` и т. д.) отдаются как есть.
 * - Конец потока: close-кадр сервера — ответный close-кадр с тем же кодом (если свой ещё не отправлен), затем
 *   [TransportEvent.Eof]; EOF TCP (в т. ч. посреди кадра: встроенный брокер форка шлёт усечённый close-кадр из одного
 *   байта) — тоже [TransportEvent.Eof]. После close-кадра сервера входящие данные не принимаются.
 * - Закрытие: при `HostAction.FlushAndClose` хост вызывает [closeOutput] — close-кадр 1000 ставится в очередь за
 *   данными, и [pendingWriteBytes] не обнуляется, пока не пришёл ответный close-кадр, EOF или ошибка (ожидание ответа
 *   ограничено сроком дописывания хоста); только после этого у `wss` закрывается поток TLS (close_notify). Если поток
 *   уже закрыт сервером (close-кадр или EOF), close-кадр не шлётся. Прямой [close] после `Upgrade` — close-кадр
 *   1000 по возможности и закрытие [inner].
 */
@MqttCoreApi
public class WebSocketTransport internal constructor(
    private val inner: MqttTransport,
    private val settings: MqttWebSocket,
    private val secure: Boolean,
    private val random: SecureRandom
) : MqttTransport {

    /** WebSocket над [inner]; [secure] — `wss` (порт по умолчанию в `Host` — 443, иначе 80). */
    public constructor(inner: MqttTransport, settings: MqttWebSocket, secure: Boolean) :
        this(inner, settings, secure, sharedRandom)

    private enum class Phase { Idle, Connecting, Upgrading, Open, Closed }

    private var phase = Phase.Idle
    private var deadlineMs: Long? = null
    private val events = ArrayList<TransportEvent>()
    private var hostHeader = ""
    private var key = ""

    /** Данные до конца `Upgrade`. */
    private val pendingOut = ArrayDeque<ByteArray>()
    private var pendingOutBytes = 0L

    /** Заголовки ответа на `Upgrade`, пока не пришёл пустой ряд. */
    private val response = ByteArrayOutputStream()

    // Разбор входящих кадров (потоковый): заголовок копится в [header], нагрузка отдаётся по мере прихода
    private val header = ByteArray(MAX_FRAME_HEADER)
    private var headerLength = 0
    private var inPayload = false
    private var opcode = 0
    private var fin = false
    private var payloadRemaining = 0L
    private val control = ByteArrayOutputStream()
    private var fragmented = false
    private val data = ByteArrayOutputStream()

    private var readInterest = true
    private var innerEof = false
    private var peerClosed = false
    private var closeSent = false
    private var awaitingCloseReply = false
    private var outputClosed = false
    private var eofSent = false

    override val pendingWriteBytes: Long
        get() = if (phase == Phase.Closed) {
            0
        } else {
            pendingOutBytes + inner.pendingWriteBytes + if (awaitingCloseReply) 1 else 0
        }

    override fun connect(host: String, port: Int, deadlineMs: Long) {
        check(phase == Phase.Idle) { "WebSocketTransport: connect() once per transport" }
        this.deadlineMs = deadlineMs
        hostHeader = hostHeader(host, port, secure)
        key = base64(ByteArray(16).also { random.nextBytes(it) })
        phase = Phase.Connecting
        inner.connect(host, port, deadlineMs)
    }

    /**
     * После `Upgrade` — бинарный кадр в [inner]; до — в очередь. Возвращает `bytes.size`, если кадр сразу ушёл в ОС
     * целиком, иначе 0. После отправленного close-кадра данные отбрасываются (RFC 6455 §5.5.1).
     */
    override fun write(bytes: ByteArray): Int {
        if (phase == Phase.Closed || bytes.isEmpty()) return 0
        if (phase != Phase.Open) {
            pendingOut.addLast(bytes)
            pendingOutBytes += bytes.size
            return 0
        }
        if (closeSent) return 0
        sendFrame(OP_BINARY, bytes)
        return if (phase == Phase.Open && inner.pendingWriteBytes == 0L) bytes.size else 0
    }

    override fun setReadInterest(enabled: Boolean) {
        readInterest = enabled
        if (phase == Phase.Closed || phase == Phase.Idle) return
        inner.setReadInterest(enabled || phase != Phase.Open)
        if (enabled) pump()
    }

    override fun poll(nowMs: Long) {
        if (phase == Phase.Closed || phase == Phase.Idle) return
        try {
            inner.poll(nowMs)
        } catch (e: Exception) {
            fail(NioTransport.errorOf(e))
            return
        }
        pump()
        val deadline = deadlineMs
        if (phase == Phase.Upgrading && deadline != null && nowMs >= deadline) fail(MqttError.ConnectTimeout)
    }

    override fun nextDeadlineMs(): Long? = when (phase) {
        Phase.Connecting -> inner.nextDeadlineMs()
        Phase.Upgrading -> deadlineMs
        else -> null
    }

    override fun events(): List<TransportEvent> {
        pump()
        if (events.isEmpty()) return emptyList()
        val out = ArrayList(events)
        events.clear()
        return out
    }

    override fun closeOutput() {
        if (phase != Phase.Open || outputClosed) return
        pump()
        if (phase != Phase.Open) return
        outputClosed = true
        if (!closeSent && !peerClosed && !innerEof) {
            sendClose(CLOSE_NORMAL)
            awaitingCloseReply = phase == Phase.Open
        }
        if (!awaitingCloseReply) inner.closeOutput()
    }

    override fun close() {
        if (phase == Phase.Closed) return
        if (phase == Phase.Open && !closeSent && !peerClosed && !innerEof) {
            try {
                sendClose(CLOSE_NORMAL)
            } catch (_: Exception) {
                // close-кадр — по возможности; соединение закрывается в любом случае
            }
        }
        release()
    }

    // --- внутреннее ---

    private fun release() {
        phase = Phase.Closed
        awaitingCloseReply = false
        try {
            inner.close()
        } catch (_: Exception) {
        }
        pendingOut.clear()
        pendingOutBytes = 0
        response.reset()
        control.reset()
        data.reset()
    }

    private fun fail(error: MqttError) {
        if (phase == Phase.Closed) return
        events += TransportEvent.Failed(error)
        release()
    }

    private fun protocolError(message: String): Nothing = throw IOException(message)

    /** Забрать события [inner] и разобрать входящие. */
    private fun pump() {
        if (phase == Phase.Closed || phase == Phase.Idle) return
        try {
            for (event in inner.events()) {
                if (phase == Phase.Closed) return
                when (event) {
                    TransportEvent.Connected -> onInnerConnected()
                    is TransportEvent.Bytes -> onBytes(event.bytes)
                    TransportEvent.Eof -> innerEof = true
                    is TransportEvent.Failed -> {
                        // [inner] уже закрыт; его ошибку (UnknownHost, ConnectTimeout, Io, в т. ч. "ssl") — как есть
                        events += event
                        release()
                        return
                    }
                }
            }
            checkEof()
        } catch (e: Exception) {
            fail(MqttError.Io(KIND, e as? IOException ?: IOException("WebSocket failure (${e.javaClass.simpleName})")))
        }
    }

    private fun onInnerConnected() {
        if (phase != Phase.Connecting) return
        phase = Phase.Upgrading
        inner.setReadInterest(true)
        inner.write(upgradeRequest())
    }

    private fun upgradeRequest(): ByteArray {
        val request = StringBuilder()
            .append("GET ").append(settings.path).append(" HTTP/1.1\r\n")
            .append("Host: ").append(hostHeader).append("\r\n")
            .append("Upgrade: websocket\r\n")
            .append("Connection: Upgrade\r\n")
            .append("Sec-WebSocket-Key: ").append(key).append("\r\n")
            .append("Sec-WebSocket-Version: 13\r\n")
            .append("Sec-WebSocket-Protocol: mqtt\r\n")
        for ((name, value) in settings.headers) request.append(name).append(": ").append(value).append("\r\n")
        request.append("\r\n")
        // Путь и заголовки проверены в MqttWebSocket: только ASCII
        return request.toString().toByteArray(Charsets.US_ASCII)
    }

    private fun onBytes(bytes: ByteArray) {
        when (phase) {
            Phase.Upgrading -> onResponseBytes(bytes)
            Phase.Open -> parseFrames(bytes, 0)
            else -> Unit
        }
    }

    private fun onResponseBytes(bytes: ByteArray) {
        val before = response.size()
        response.write(bytes, 0, bytes.size)
        val all = response.toByteArray()
        // Поиск конца заголовков начинается чуть раньше новых байт: разделитель мог разрезаться между чтениями
        val end = indexOfHeaderEnd(all, maxOf(0, before - 3))
        if (end < 0) {
            if (all.size > MAX_RESPONSE_HEADER) protocolError("WebSocket upgrade failed (response header too large)")
            return
        }
        if (end > MAX_RESPONSE_HEADER) protocolError("WebSocket upgrade failed (response header too large)")
        response.reset()
        checkResponse(String(all, 0, end, Charsets.ISO_8859_1))
        phase = Phase.Open
        deadlineMs = null
        inner.setReadInterest(readInterest)
        events += TransportEvent.Connected
        while (pendingOut.isNotEmpty()) {
            val payload = pendingOut.removeFirst()
            pendingOutBytes -= payload.size
            sendFrame(OP_BINARY, payload)
        }
        val rest = end + 4
        if (rest < all.size) parseFrames(all, rest)
    }

    private fun checkResponse(head: String) {
        val lines = head.split("\r\n")
        val status = lines.first().split(' ').filter { it.isNotEmpty() }
        if (status.size < 2 || !status[0].startsWith("HTTP/1.")) protocolError("WebSocket upgrade failed (not HTTP)")
        val code = status[1]
        if (code != "101") {
            val shown = if (code.length == 3 && code.all { it in '0'..'9' }) code else "invalid"
            protocolError("WebSocket upgrade refused (HTTP $shown)")
        }
        val fields = HashMap<String, String>()
        for (line in lines.drop(1)) {
            val colon = line.indexOf(':')
            if (colon <= 0) continue
            val name = line.substring(0, colon).trim().lowercase()
            val value = line.substring(colon + 1).trim()
            fields[name] = fields[name]?.let { "$it, $value" } ?: value
        }
        if (!fields["upgrade"].equals("websocket", ignoreCase = true)) {
            protocolError("WebSocket upgrade failed (no Upgrade: websocket)")
        }
        val connection = fields["connection"].orEmpty().split(',').map { it.trim() }
        if (connection.none { it.equals("upgrade", ignoreCase = true) }) {
            protocolError("WebSocket upgrade failed (no Connection: upgrade)")
        }
        if (fields["sec-websocket-accept"] != acceptFor(key)) {
            protocolError("WebSocket upgrade failed (wrong Sec-WebSocket-Accept)")
        }
        val protocol = fields["sec-websocket-protocol"]
        if (protocol != null && protocol != SUBPROTOCOL) {
            protocolError("WebSocket upgrade failed (subprotocol is not mqtt)")
        }
        if (fields["sec-websocket-extensions"] != null) {
            protocolError("WebSocket upgrade failed (unrequested extension)")
        }
    }

    /** Потоковый разбор кадров из [bytes] начиная с [from]; данные копятся в [data] и уходят одним событием. */
    private fun parseFrames(bytes: ByteArray, from: Int) {
        var i = from
        try {
            while (i < bytes.size && phase == Phase.Open && !peerClosed) {
                if (!inPayload) {
                    header[headerLength++] = bytes[i++]
                    if (headerComplete()) startFrame()
                    continue
                }
                val n = minOf(payloadRemaining, (bytes.size - i).toLong()).toInt()
                if (opcode == OP_CONTINUATION || opcode == OP_BINARY) {
                    data.write(bytes, i, n)
                } else {
                    control.write(bytes, i, n)
                }
                i += n
                payloadRemaining -= n
                if (payloadRemaining == 0L) endFrame()
            }
        } finally {
            if (data.size() > 0) {
                events += TransportEvent.Bytes(data.toByteArray())
                data.reset()
            }
        }
    }

    /** Заголовок кадра собран: 2 байта, затем 2 или 8 байт длины (маски у кадров сервера быть не должно). */
    private fun headerComplete(): Boolean {
        if (headerLength < 2) return false
        val len7 = header[1].toInt() and 0x7F
        val need = 2 + when (len7) {
            126 -> 2
            127 -> 8
            else -> 0
        }
        return headerLength >= need
    }

    private fun startFrame() {
        val b0 = header[0].toInt() and 0xFF
        val b1 = header[1].toInt() and 0xFF
        headerLength = 0
        if (b0 and 0x70 != 0) protocolError("WebSocket protocol error (RSV bits)")
        if (b1 and 0x80 != 0) protocolError("WebSocket protocol error (masked server frame)")
        fin = b0 and 0x80 != 0
        opcode = b0 and 0x0F
        val length: Long = when (val len7 = b1 and 0x7F) {
            126 -> ((header[2].toLong() and 0xFF) shl 8) or (header[3].toLong() and 0xFF)
            127 -> {
                var value = 0L
                for (k in 2 until 10) value = (value shl 8) or (header[k].toLong() and 0xFF)
                if (value < 0) protocolError("WebSocket protocol error (64-bit length)")
                value
            }
            else -> len7.toLong()
        }
        when (opcode) {
            OP_TEXT -> protocolError("WebSocket protocol error (text frame)")
            OP_BINARY -> if (fragmented) protocolError("WebSocket protocol error (data frame inside a fragmented one)")
            OP_CONTINUATION -> if (!fragmented) protocolError("WebSocket protocol error (continuation without start)")
            OP_CLOSE, OP_PING, OP_PONG -> {
                if (!fin) protocolError("WebSocket protocol error (fragmented control frame)")
                if (length > MAX_CONTROL_PAYLOAD) protocolError("WebSocket protocol error (control frame too long)")
                control.reset()
            }
            else -> protocolError("WebSocket protocol error (unknown opcode)")
        }
        payloadRemaining = length
        inPayload = true
        if (length == 0L) endFrame()
    }

    private fun endFrame() {
        inPayload = false
        when (opcode) {
            OP_BINARY, OP_CONTINUATION -> fragmented = !fin
            OP_PING -> if (!closeSent) sendFrame(OP_PONG, control.toByteArray())
            OP_PONG -> Unit
            OP_CLOSE -> onPeerClose(control.toByteArray())
        }
        control.reset()
    }

    private fun onPeerClose(payload: ByteArray) {
        peerClosed = true
        if (!closeSent) {
            // Ответ тем же кодом (RFC 6455 §5.5.1); без кода — пустой close-кадр
            sendFrame(OP_CLOSE, if (payload.size >= 2) payload.copyOf(2) else ByteArray(0))
            closeSent = true
        }
    }

    private fun checkEof() {
        if (phase == Phase.Upgrading && innerEof) {
            fail(MqttError.Io(KIND, IOException("WebSocket upgrade failed (connection closed)")))
            return
        }
        if (phase != Phase.Open) return
        if (awaitingCloseReply && (peerClosed || innerEof)) {
            awaitingCloseReply = false
            // Ответ получен (или поток кончился): теперь закрыть и поток [inner] (у TLS — close_notify)
            if (outputClosed) inner.closeOutput()
        }
        if (!eofSent && (peerClosed || innerEof)) {
            eofSent = true
            events += TransportEvent.Eof
        }
    }

    private fun sendClose(code: Int) {
        closeSent = true
        sendFrame(OP_CLOSE, byteArrayOf((code shr 8).toByte(), code.toByte()))
    }

    /** Кадр клиента: FIN, [op], маска; одним массивом в [inner]. */
    private fun sendFrame(op: Int, payload: ByteArray) {
        val size = payload.size
        val lengthBytes = when {
            size <= 125 -> 0
            size <= 0xFFFF -> 2
            else -> 8
        }
        val frame = ByteArray(2 + lengthBytes + 4 + size)
        frame[0] = (0x80 or op).toByte()
        var p = 2
        when (lengthBytes) {
            0 -> frame[1] = (0x80 or size).toByte()
            2 -> {
                frame[1] = (0x80 or 126).toByte()
                frame[p++] = (size shr 8).toByte()
                frame[p++] = size.toByte()
            }
            else -> {
                frame[1] = (0x80 or 127).toByte()
                val long = size.toLong()
                for (shift in 56 downTo 0 step 8) frame[p++] = (long shr shift).toByte()
            }
        }
        val mask = ByteArray(4)
        random.nextBytes(mask)
        System.arraycopy(mask, 0, frame, p, 4)
        p += 4
        for (k in 0 until size) frame[p + k] = (payload[k].toInt() xor mask[k and 3].toInt()).toByte()
        inner.write(frame)
    }

    internal companion object {
        const val KIND: String = "websocket"
        const val SUBPROTOCOL: String = "mqtt"

        /** Предел заголовков ответа на `Upgrade`. */
        const val MAX_RESPONSE_HEADER: Int = 8 * 1024
        const val MAX_CONTROL_PAYLOAD: Int = 125
        private const val MAX_FRAME_HEADER = 10
        const val CLOSE_NORMAL: Int = 1000

        const val OP_CONTINUATION: Int = 0x0
        const val OP_TEXT: Int = 0x1
        const val OP_BINARY: Int = 0x2
        const val OP_CLOSE: Int = 0x8
        const val OP_PING: Int = 0x9
        const val OP_PONG: Int = 0xA

        private const val GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

        val sharedRandom: SecureRandom by lazy { SecureRandom() }

        /** `Sec-WebSocket-Accept` для ключа (RFC 6455 §4.2.2). */
        fun acceptFor(key: String): String =
            base64(MessageDigest.getInstance("SHA-1").digest((key + GUID).toByteArray(Charsets.US_ASCII)))

        /** Base64 из stdlib Kotlin, не `java.util.Base64`: его нет на Android до API 26 (minSdk приложения — 24). */
        @OptIn(ExperimentalEncodingApi::class)
        fun base64(bytes: ByteArray): String = Base64.Default.encode(bytes)

        /** `Host` запроса: IPv6 в скобках, порт — если не по умолчанию для схемы. */
        fun hostHeader(host: String, port: Int, secure: Boolean): String {
            val name = if (':' in host && !host.startsWith("[")) "[$host]" else host
            val defaultPort = if (secure) 443 else 80
            return if (port == defaultPort) name else "$name:$port"
        }

        private fun indexOfHeaderEnd(bytes: ByteArray, from: Int): Int {
            var i = from
            while (i + 3 < bytes.size) {
                if (bytes[i] == CR && bytes[i + 1] == LF && bytes[i + 2] == CR && bytes[i + 3] == LF) return i
                i++
            }
            return -1
        }

        private const val CR: Byte = '\r'.code.toByte()
        private const val LF: Byte = '\n'.code.toByte()
    }
}
