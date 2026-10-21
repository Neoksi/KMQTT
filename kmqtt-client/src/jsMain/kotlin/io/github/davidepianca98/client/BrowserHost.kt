package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.node.setTimeout
import io.github.davidepianca98.socket.IOException

// хост и транспорт для браузера — только WebSocket (сырых сокетов в браузере нет). Код этого файла не зовёт
// API Node (`Buffer`, `setImmediate`, модули `node:*`): только WebSocket API WHATWG, `MessageChannel` и `setTimeout`.

/**
 * Хост клиента для браузера: цикл [JsEventLoopHost] (как у [NodeHost]), обороты через `MessageChannel`
 * (в браузере нет `setImmediate`, а `setTimeout(0)` после пяти вложенных вызовов ждёт не меньше 4 мс); транспорт —
 * [BrowserWebSocketTransport] из [createTransport] (аргумент — пробуждение хоста).
 */
@OptIn(MqttCoreApi::class)
internal class BrowserHost private constructor(
    clock: MqttClock,
    createTransport: (wakeup: () -> Unit) -> MqttTransport,
    scheduler: ImmediateScheduler
) : MqttHost by JsEventLoopHost(clock, createTransport, scheduler::schedule, scheduler::close) {

    constructor(clock: MqttClock, createTransport: (wakeup: () -> Unit) -> MqttTransport) :
        this(clock, createTransport, ImmediateScheduler())
}

/**
 * Следующий оборот цикла событий без задержки: `MessageChannel` (сообщение — макрозадача, отрисовку и ввод не
 * отнимает, как бесконечные микрозадачи), без него — `setTimeout(0)`. Канал создаётся при первом вызове и закрывается
 * [close] (в Node открытый порт держал бы процесс).
 */
internal class ImmediateScheduler {
    private var channel: dynamic = null
    private val queue = ArrayDeque<() -> Unit>()
    private var closed = false

    fun schedule(callback: () -> Unit) {
        if (closed) return
        val ch = channel ?: createChannel()
        if (ch == null) {
            setTimeout(callback, 0)
            return
        }
        queue.addLast(callback)
        ch.port2.postMessage(null)
    }

    fun close() {
        closed = true
        queue.clear()
        val ch = channel ?: return
        channel = null
        ch.port1.onmessage = null
        ch.port1.close()
        ch.port2.close()
    }

    private fun createChannel(): dynamic {
        val ctor: dynamic = js("typeof MessageChannel === 'function' ? MessageChannel : null")
        if (ctor == null) return null
        val ch: dynamic = js("new ctor()")
        ch.port1.onmessage = { _: dynamic -> queue.removeFirstOrNull()?.invoke() }
        // Node: порт с обработчиком держит процесс, пока хост не закрыт; незакрытый хост не должен держать процесс
        // (его держат сокеты и таймеры, пока они нужны). В браузере `unref` нет
        val port: dynamic = ch.port1
        js("if (typeof port.unref === 'function') port.unref()")
        channel = ch
        return ch
    }
}

/**
 * MQTT поверх WebSocket на WebSocket API браузера (WHATWG `WebSocket`, подпротокол `mqtt`). Кадры, маски,
 * ping/pong и рукопожатие делает браузер; транспорт переводит события API в [TransportEvent]. Работает в любой среде с
 * глобальным `WebSocket` (браузеры, Node 22+, Deno); тесты форка гоняют его на Node.
 *
 * Отличия от `WebSocketTransport` JVM и [NodeWebSocketTransport] — то, чего API браузера не даёт:
 * - Нет дополнительных заголовков `Upgrade` и настроек TLS (корни, свой сертификат, клиентский сертификат, ALPN, SNI,
 *   отключение проверок): проверяет браузер по своим корням и имени хоста. Фабрика [BrowserMqttClient] такие опции
 *   отвергает, а не пропускает молча.
 * - Отказ подключения (нет хоста, отказ TCP, ошибка TLS, отказ `Upgrade`) браузер не различает: всё —
 *   [MqttError.Io] `kind = "websocket"` с кодом закрытия, без [MqttError.UnknownHost]. Срок [connect] — по часам хоста
 *   ([MqttError.ConnectTimeout]).
 * - Нет паузы чтения: при `PauseRead` принятые сообщения копятся в транспорте и отдаются после снятия паузы (память
 *   ограничивает граница входящих клиента выше по стеку).
 * - Нет полузакрытия: после close-кадра сервера (или закрытия по нашему [closeOutput]) писать нельзя — исходящие
 *   отбрасываются. [closeOutput] — `close(1000)`: браузер шлёт close-кадр за уже отданными данными; [pendingWriteBytes]
 *   — `bufferedAmount` плюс 1, пока не пришло событие `close`.
 * - Конец потока: штатное закрытие (`wasClean`) — [TransportEvent.Eof]; нештатное (обрыв TCP без close-кадра, ошибка
 *   протокола, код 1006 и др.) — [TransportEvent.Failed] с [MqttError.Io] `kind = "websocket"` и кодом закрытия.
 *   Текстовое сообщение сервера — нарушение MQTT-6.0.0-1: [MqttError.Io] `kind = "websocket"`.
 * - Подпротокол ответа — `mqtt` или пусто (браузер сам отвергает чужой подпротокол).
 *
 * Адрес (путь из [settings] может нести токен в запросе) в ошибки и журнал не попадает. Все методы — только на цикле
 * событий.
 */
@MqttCoreApi
internal class BrowserWebSocketTransport(
    private val wakeup: () -> Unit,
    private val settings: MqttWebSocket,
    private val secure: Boolean,
    /** Конструктор `WebSocket`; по умолчанию глобальный. */
    private val webSocketConstructor: dynamic = globalWebSocket()
) : MqttTransport {

    private enum class Phase { Idle, Connecting, Open, Closed }

    private var phase = Phase.Idle
    private var socket: dynamic = null
    private var deadlineMs: Long? = null
    private val queue = ArrayList<TransportEvent>()
    private val preConnect = ArrayDeque<ByteArray>()
    private var preConnectBytes = 0L
    private var readInterest = true
    private var outputClosed = false
    private var peerClosed = false
    private var socketClosed = false

    override val pendingWriteBytes: Long
        get() {
            if (phase == Phase.Closed || socketClosed) return 0
            val buffered = (socket?.bufferedAmount as? Number)?.toLong() ?: 0L
            val tail = if (outputClosed) 1L else 0L
            return preConnectBytes + buffered + tail
        }

    override fun connect(host: String, port: Int, deadlineMs: Long) {
        check(phase == Phase.Idle) { "BrowserWebSocketTransport: connect() once per transport" }
        phase = Phase.Connecting
        this.deadlineMs = deadlineMs
        val ctor = webSocketConstructor
        if (ctor == null) {
            fail(MqttError.Io(KIND, IOException("WebSocket API is not available")))
            return
        }
        val url = url(host, port, secure, settings.path)
        val protocols = arrayOf(SUBPROTOCOL)
        val ws: dynamic = try {
            js("new ctor(url, protocols)")
        } catch (e: Throwable) {
            // Текст ошибки браузера содержит адрес — наружу только общий текст
            fail(MqttError.Io(KIND, IOException("WebSocket URL rejected")))
            return
        }
        socket = ws
        ws.binaryType = "arraybuffer"
        ws.onopen = { _: dynamic -> onOpen() }
        ws.onmessage = { event: dynamic -> onMessage(event.data) }
        ws.onclose = { event: dynamic -> onClose(event.wasClean == true, (event.code as? Number)?.toInt() ?: 0) }
        // `error` всегда идёт перед `close` и сведений не несёт
        ws.onerror = { _: dynamic -> wakeup() }
    }

    override fun write(bytes: ByteArray): Int {
        if (phase == Phase.Closed || bytes.isEmpty()) return 0
        if (phase != Phase.Open) {
            preConnect.addLast(bytes)
            preConnectBytes += bytes.size
            return 0
        }
        if (outputClosed || peerClosed || socketClosed) return 0
        // send(ArrayBufferView) копирует байты при вызове (WHATWG), массив вызывающего не удерживается
        socket.send(bytes)
        return if (((socket.bufferedAmount as? Number)?.toLong() ?: 0L) == 0L) bytes.size else 0
    }

    override fun closeOutput() {
        if (phase == Phase.Closed || outputClosed) return
        outputClosed = true
        if (phase == Phase.Open) closeSocket()
    }

    override fun setReadInterest(enabled: Boolean) {
        readInterest = enabled
        if (enabled && queue.isNotEmpty()) wakeup()
    }

    override fun poll(nowMs: Long) {
        val deadline = deadlineMs
        if (phase == Phase.Connecting && deadline != null && nowMs >= deadline) fail(MqttError.ConnectTimeout)
    }

    override fun nextDeadlineMs(): Long? = if (phase == Phase.Connecting) deadlineMs else null

    /**
     * На паузе чтения отдаются только события до первого принятого сообщения (порядок событий сохраняется: конец
     * потока и ошибки после данных ждут вместе с ними).
     */
    override fun events(): List<TransportEvent> {
        if (queue.isEmpty()) return emptyList()
        val count = if (readInterest) queue.size else queue.indexOfFirst { it is TransportEvent.Bytes }.let {
            if (it < 0) queue.size else it
        }
        if (count == 0) return emptyList()
        val out = ArrayList(queue.subList(0, count))
        queue.subList(0, count).clear()
        return out
    }

    override fun close() {
        if (phase == Phase.Closed) return
        phase = Phase.Closed
        preConnect.clear()
        preConnectBytes = 0
        val ws = socket ?: return
        socket = null
        ws.onopen = null
        ws.onmessage = null
        ws.onclose = null
        ws.onerror = null
        if (!socketClosed) {
            try {
                ws.close(CLOSE_NORMAL)
            } catch (_: Throwable) {
            }
        }
    }

    private fun onOpen() {
        if (phase != Phase.Connecting) return
        val protocol = socket.protocol as? String
        if (!protocol.isNullOrEmpty() && protocol != SUBPROTOCOL) {
            fail(MqttError.Io(KIND, IOException("WebSocket upgrade failed (subprotocol is not mqtt)")))
            wakeup()
            return
        }
        phase = Phase.Open
        deadlineMs = null
        queue += TransportEvent.Connected
        while (preConnect.isNotEmpty()) socket.send(preConnect.removeFirst())
        preConnectBytes = 0
        if (outputClosed) closeSocket()
        wakeup()
    }

    private fun onMessage(data: dynamic) {
        if (phase != Phase.Open || peerClosed) return
        if (js("data instanceof ArrayBuffer") as Boolean) {
            // Буфер сообщения — свой у каждого события: вид без копирования
            val bytes: ByteArray = js("new Int8Array(data)").unsafeCast<ByteArray>()
            if (bytes.isNotEmpty()) queue += TransportEvent.Bytes(bytes)
        } else {
            fail(MqttError.Io(KIND, IOException("WebSocket protocol error (text frame)")))
        }
        wakeup()
    }

    private fun onClose(wasClean: Boolean, code: Int) {
        socketClosed = true
        when (phase) {
            Phase.Connecting -> fail(MqttError.Io(KIND, IOException("WebSocket connection failed (code $code)")))
            Phase.Open -> if (wasClean) {
                if (!peerClosed) {
                    peerClosed = true
                    queue += TransportEvent.Eof
                }
            } else {
                fail(MqttError.Io(KIND, IOException("WebSocket closed abnormally (code $code)")))
            }
            else -> Unit
        }
        wakeup()
    }

    private fun closeSocket() {
        try {
            socket?.close(CLOSE_NORMAL)
        } catch (_: Throwable) {
        }
    }

    private fun fail(error: MqttError) {
        if (phase == Phase.Closed) return
        queue += TransportEvent.Failed(error)
        close()
    }

    internal companion object {
        const val KIND: String = "websocket"
        const val SUBPROTOCOL: String = "mqtt"
        const val CLOSE_NORMAL: Int = 1000

        fun globalWebSocket(): dynamic = js("typeof WebSocket === 'function' ? WebSocket : null")

        /** `ws(s)://хост:порт/путь`; IPv6 — в скобках. */
        fun url(host: String, port: Int, secure: Boolean, path: String): String {
            val name = if (':' in host && !host.startsWith("[")) "[$host]" else host
            return (if (secure) "wss://" else "ws://") + name + ":" + port + path
        }
    }
}
