package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi

/**
 * TLS поверх TCP-транспорта native-хоста ([inner]: `WinSockTransport` mingwX64, `PosixTransport` Linux) на сессии
 * OpenSSL с memory-BIO (перенос `TlsTransport` JVM, который так же оборачивает `NioTransport`).
 *
 * Хост обслуживает готовность сокета у внутреннего транспорта (он сам стоит в реестре хоста), а события и действия —
 * у этого. Все методы — только на потоке хоста.
 *
 * - Сессия ([sessions]) создаётся в [connect] до открытия сокета: ошибка настроек — [TransportEvent.Failed] с
 *   [MqttError.Io] `kind = "ssl-config"`, соединение не открывается.
 * - Рукопожатие — по событиям внутреннего транспорта; [TransportEvent.Connected] — только после него. Срок
 *   [connect] покрывает резолв, TCP и рукопожатие: по нему [MqttError.ConnectTimeout].
 * - Отказ TLS — [MqttError.Io] с видом из [NativeTlsException] (`ssl`, `ssl-trust`, `ssl-hostname`, `ssl-expired`) и
 *   причиной без адреса (H11). EOF во время рукопожатия — `ssl`.
 * - Обратное давление: при выключенном чтении ([setReadInterest] `false`) шифротекст копится, но не расшифровывается;
 *   внутреннему транспорту чтение выключается только после рукопожатия.
 * - Удалённая сторона закрыла поток (close_notify или EOF TCP после рукопожатия) — [TransportEvent.Eof] после всех
 *   расшифрованных данных.
 * - [closeOutput] (для `HostAction.FlushAndClose`) — close_notify в очередь за данными; [close] после рукопожатия
 *   ставит close_notify без ожидания ответа и закрывает сокет.
 *
 * [pendingWriteBytes] — открытый текст, ждущий конца рукопожатия, плюс шифротекст в очереди внутреннего транспорта.
 */
@MqttCoreApi
internal class NativeTlsTransport(
    private val inner: MqttTransport,
    private val sessions: NativeTlsSessionFactory
) : MqttTransport {

    private enum class Phase { Idle, Connecting, Handshaking, Open, Closed }

    private var phase = Phase.Idle
    private var session: NativeTlsSession? = null
    private var deadlineMs: Long? = null
    private val events = ArrayList<TransportEvent>()

    /** Шифротекст от внутреннего транспорта, ещё не переданный сессии (копится при выключенном чтении). */
    private val inbound = ArrayDeque<ByteArray>()

    /** Открытый текст, ещё не принятый сессией (до конца рукопожатия — весь); [appOutOffset] — принятое из головы. */
    private val appOut = ArrayDeque<ByteArray>()
    private var appOutOffset = 0
    private var appOutBytes = 0L

    private var readBuffer: ByteArray? = null
    private var readInterest = true
    private var innerEof = false
    private var peerClosed = false
    private var eofSent = false
    private var outputClosed = false

    override val pendingWriteBytes: Long
        get() = if (phase == Phase.Closed) 0 else appOutBytes + inner.pendingWriteBytes

    override fun connect(host: String, port: Int, deadlineMs: Long) {
        check(phase == Phase.Idle) { "NativeTlsTransport: connect() once per transport" }
        this.deadlineMs = deadlineMs
        val created = try {
            sessions.open(host)
        } catch (e: NativeTlsException) {
            phase = Phase.Connecting
            fail(MqttError.Io(e.kind, e))
            return
        } catch (e: Exception) {
            // Неожиданное исключение настроек: только класс, без текста (в нём может быть путь или содержимое)
            phase = Phase.Connecting
            fail(MqttError.Io(KIND_TLS_CONFIG, NativeTlsException(KIND_TLS_CONFIG, "TLS configuration failed (${e::class.simpleName})")))
            return
        }
        session = created
        phase = Phase.Connecting
        inner.connect(host, port, deadlineMs)
    }

    override fun write(bytes: ByteArray): Int {
        if (phase == Phase.Closed || bytes.isEmpty() || outputClosed) return 0
        appOut.addLast(bytes)
        appOutBytes += bytes.size
        if (phase != Phase.Open) return 0
        pump()
        return if (phase == Phase.Open && appOutBytes == 0L && inner.pendingWriteBytes == 0L) bytes.size else 0
    }

    override fun setReadInterest(enabled: Boolean) {
        readInterest = enabled
        if (phase == Phase.Closed) return
        inner.setReadInterest(enabled || phase != Phase.Open)
        if (enabled) pump()
    }

    override fun poll(nowMs: Long) {
        if (phase == Phase.Closed) return
        try {
            inner.poll(nowMs)
        } catch (e: Exception) {
            fail(MqttError.Io(e::class.simpleName ?: "Exception", e))
            return
        }
        pump()
        val deadline = deadlineMs
        if (phase == Phase.Handshaking && deadline != null && nowMs >= deadline) fail(MqttError.ConnectTimeout)
    }

    override fun nextDeadlineMs(): Long? = when (phase) {
        Phase.Connecting -> inner.nextDeadlineMs()
        Phase.Handshaking -> deadlineMs
        else -> null
    }

    override fun events(): List<TransportEvent> {
        pump()
        if (events.isEmpty()) return emptyList()
        val out = ArrayList(events)
        events.clear()
        return out
    }

    /**
     * Для `HostAction.FlushAndClose`: зашифровать оставшийся открытый текст, затем close_notify — в очередь внутреннего
     * транспорта за всеми TLS-записями данных (уходит последней записью). До конца рукопожатия — ничего.
     */
    override fun closeOutput() {
        if (phase != Phase.Open) return
        pump()
        val s = session ?: return
        if (phase != Phase.Open || outputClosed) return
        outputClosed = true
        try {
            s.shutdown()
            flushOutput(s)
        } catch (_: Exception) {
            // close_notify — по возможности; данные уже в очереди
        }
        appOut.clear()
        appOutOffset = 0
        appOutBytes = 0
    }

    override fun close() {
        if (phase == Phase.Closed) return
        val wasOpen = phase == Phase.Open
        phase = Phase.Closed
        val s = session
        if (wasOpen && s != null && !outputClosed) {
            outputClosed = true
            try {
                s.shutdown()
                flushOutput(s)
            } catch (_: Exception) {
                // close_notify — по возможности; сокет закрывается в любом случае
            }
        }
        release()
    }

    // --- внутреннее ---

    private fun release() {
        phase = Phase.Closed
        try {
            inner.close()
        } catch (_: Exception) {
        }
        session?.free()
        session = null
        inbound.clear()
        appOut.clear()
        appOutOffset = 0
        appOutBytes = 0
        readBuffer = null
    }

    private fun fail(error: MqttError) {
        if (phase == Phase.Closed) return
        events += TransportEvent.Failed(error)
        release()
    }

    /** Забрать события внутреннего транспорта и продвинуть сессию. */
    private fun pump() {
        if (phase == Phase.Closed || phase == Phase.Idle) return
        try {
            for (event in inner.events()) {
                if (phase == Phase.Closed) return
                when (event) {
                    TransportEvent.Connected -> onTcpConnected()
                    is TransportEvent.Bytes -> if (!peerClosed) inbound.addLast(event.bytes)
                    TransportEvent.Eof -> innerEof = true
                    is TransportEvent.Failed -> {
                        // Внутренний уже закрыт; его ошибку (UnknownHost, ConnectTimeout, Io) отдаём как есть
                        events += event
                        release()
                        return
                    }
                }
            }
            process()
            checkEof()
        } catch (e: NativeTlsException) {
            fail(MqttError.Io(e.kind, e))
        } catch (e: Exception) {
            val handshake = phase == Phase.Handshaking
            val text = if (handshake) HANDSHAKE_FAILED else TLS_FAILURE
            fail(MqttError.Io(KIND_TLS, NativeTlsException(KIND_TLS, "$text (${e::class.simpleName})")))
        }
    }

    private fun onTcpConnected() {
        if (session == null) return
        phase = Phase.Handshaking
        inner.setReadInterest(true)
    }

    private fun process() {
        val s = session ?: return
        if (phase == Phase.Handshaking) {
            while (inbound.isNotEmpty()) s.feed(inbound.removeFirst())
            val done = s.handshake()
            flushOutput(s)
            if (!done) return
            phase = Phase.Open
            deadlineMs = null
            inner.setReadInterest(readInterest)
            events += TransportEvent.Connected
        }
        if (phase != Phase.Open) return
        writeAppData(s)
        if (readInterest) readAppData(s)
        // Чтение могло продвинуть сессию (пересогласование, ответный close_notify): данные ждали — пробуем ещё раз
        if (phase == Phase.Open && appOut.isNotEmpty()) writeAppData(s)
    }

    private fun writeAppData(s: NativeTlsSession) {
        while (appOut.isNotEmpty() && phase == Phase.Open && !outputClosed) {
            val head = appOut.first()
            val length = minOf(head.size - appOutOffset, MAX_RECORD_INPUT)
            val n = s.writePlain(head, appOutOffset, length)
            flushOutput(s)
            if (n <= 0) return
            appOutOffset += n
            appOutBytes -= n
            if (appOutOffset >= head.size) {
                appOut.removeFirst()
                appOutOffset = 0
            }
        }
    }

    private fun readAppData(s: NativeTlsSession) {
        if (peerClosed) return
        while (inbound.isNotEmpty()) s.feed(inbound.removeFirst())
        val buffer = readBuffer ?: ByteArray(READ_BUFFER).also { readBuffer = it }
        while (phase == Phase.Open && readInterest) {
            val n = s.readPlain(buffer)
            flushOutput(s)
            when {
                n > 0 -> events += TransportEvent.Bytes(buffer.copyOf(n))
                n == NativeTlsSession.READ_CLOSED -> {
                    peerClosed = true
                    inbound.clear()
                    return
                }
                else -> return
            }
        }
    }

    /** Шифротекст сессии — отдельным массивом во внутренний транспорт (он не копирует). */
    private fun flushOutput(s: NativeTlsSession) {
        val out = s.takeOutput() ?: return
        inner.write(out)
    }

    private fun checkEof() {
        if (phase == Phase.Handshaking && (peerClosed || innerEof)) {
            fail(MqttError.Io(KIND_TLS, NativeTlsException(KIND_TLS, HANDSHAKE_FAILED)))
            return
        }
        if (phase != Phase.Open || eofSent) return
        // После close_notify — сразу; после EOF TCP — когда весь накопленный шифротекст разобран (обрезанный хвост
        // без целой записи отбрасывается: целостность пакетов MQTT проверяет фрейминг)
        val drained = peerClosed || (innerEof && readInterest && inbound.isEmpty())
        if (drained) {
            eofSent = true
            events += TransportEvent.Eof
        }
    }

    private companion object {
        const val HANDSHAKE_FAILED = "TLS handshake failed"
        const val TLS_FAILURE = "TLS failure"

        /** Буфер расшифровки: одна TLS-запись (16 КиБ открытого текста). */
        const val READ_BUFFER = 16 * 1024

        /** Открытого текста за один вызов сессии: одна TLS-запись, каждая уходит отдельным массивом. */
        const val MAX_RECORD_INPUT = 16 * 1024
    }
}
