package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.node.bufferOf
import io.github.davidepianca98.client.node.bytesOf
import io.github.davidepianca98.client.node.crypto.X509Certificate
import io.github.davidepianca98.client.node.crypto.createPrivateKey
import io.github.davidepianca98.client.node.fs.readFileSync
import io.github.davidepianca98.client.node.jsObject
import io.github.davidepianca98.socket.tls.TLSClientSettings
import io.github.davidepianca98.client.node.net.connect as netConnect
import io.github.davidepianca98.client.node.tls.connect as tlsConnect

/**
 * Окружение транспорта внутри [NodeHost]: пробуждение хоста (следующий оборот цикла). Создаёт только хост; транспорту
 * передаётся через [NodeTransportFactory].
 */
@MqttCoreApi
public class NodeTransportContext internal constructor(internal val wakeup: () -> Unit)

/** Фабрика транспорта для [NodeHost]: новый транспорт на каждое [HostAction.Connect]. */
@MqttCoreApi
public fun interface NodeTransportFactory {
    public fun create(context: NodeTransportContext): MqttTransport

    public companion object {
        /** Обычный TCP (`net.Socket`). */
        public val Tcp: NodeTransportFactory = NodeTransportFactory { NodeTransport(it, null, true) }

        /** TLS (`tls.TLSSocket`) по [settings]; [verifyHostname] — проверка имени хоста в сертификате. */
        public fun tls(settings: TLSClientSettings, verifyHostname: Boolean = true): NodeTransportFactory =
            NodeTransportFactory { NodeTransport(it, settings, verifyHostname) }
    }
}

/**
 * Транспорт Node.js: `net.Socket` (TCP) или `tls.TLSSocket` (TLS при [tls] `!= null`) под [NodeHost].
 *
 * - Подключение, резолв имени (`dns.lookup` внутри Node, пул libuv) и рукопожатие TLS — асинхронные; события сокета
 *   кладутся в очередь транспорта и будят хост, сам транспорт ничего не ждёт. Срок [connect] (резолв + TCP + TLS) —
 *   по часам хоста в [poll]: [MqttError.ConnectTimeout], сокет уничтожается.
 * - `allowHalfOpen: true`: FIN брокера даёт [TransportEvent.Eof], а сокет остаётся открытым на запись, пока хост не
 *   закроет транспорт (без этого Node сам отвечал бы FIN). `noDelay: true`.
 * - Чтение — событиями `data` (каждое — [TransportEvent.Bytes] с копией байтов); обратное давление — `pause()`/`resume()`.
 *   Запись — `socket.write` над `Buffer` поверх массива вызывающего (без копирования); до подключения байты ждут в
 *   своей очереди. [pendingWriteBytes] — эта очередь плюс `writableLength` сокета (ещё не отдано ОС) плюс 1, пока после
 *   [closeOutput] (`socket.end()`: у TCP — FIN, у TLS — close_notify и FIN) не пришло `finish`.
 * - [close] — `destroy()` без ожидания; штатное закрытие с дописыванием — `HostAction.FlushAndClose` (хост вызывает
 *   [closeOutput], ждёт `pendingWriteBytes == 0`, читая и отбрасывая входящее, затем [close]).
 * - Ошибки: `ENOTFOUND`/`EAI_*` → [MqttError.UnknownHost]; ошибка TLS (код не вида `E…` сетевой ошибки) →
 *   [MqttError.Io] `kind = "ssl"`; неверные настройки TLS (PEM, файл) → `kind = "ssl-config"`; прочее →
 *   [MqttError.Io] с кодом ошибки Node (`ECONNREFUSED`, `ECONNRESET`…) в `kind`. У ошибок TLS причина — исключение с
 *   общим текстом и кодом: текст Node содержит адрес и имена из сертификата (H11).
 *
 * TLS: цепочка проверяется по системным корням или, при `serverCertificate`, только по нему (PEM-строка или путь к
 * файлу, как на JVM); `checkServerCertificate = false` — без проверки; имя хоста — по [verifyHostname] (только если
 * цепочка проверяется); TLS 1.2+; SNI — `serverNameIndications` или имя хоста (не IP-адрес); ALPN —
 * `alpnProtocols`; клиентский сертификат и ключ — PEM-строка или путь, пароль ключа — `clientCertificatePassword`.
 *
 * Все методы — только на потоке цикла событий (в Node он один).
 */
@MqttCoreApi
public class NodeTransport internal constructor(
    private val context: NodeTransportContext,
    private val tls: TLSClientSettings?,
    private val verifyHostname: Boolean,
    /** Опция `lookup` сокета (функция резолва `dns.lookup`); `null` — резолв Node по умолчанию. Для тестов. */
    private val lookup: dynamic = null
) : MqttTransport {

    /** TCP-транспорт. */
    public constructor(context: NodeTransportContext) : this(context, null, true)

    private enum class Phase { Idle, Connecting, Connected, Closed }

    private var phase = Phase.Idle
    private var socket: dynamic = null
    private var deadlineMs: Long? = null
    private val events = ArrayList<TransportEvent>()
    private val preConnect = ArrayDeque<ByteArray>()
    private var preConnectBytes = 0L
    private var readInterest = true
    private var outputClosed = false
    private var finished = false

    override val pendingWriteBytes: Long
        get() {
            if (phase == Phase.Closed) return 0
            val written = socket?.writableLength as? Number
            val tail = if (outputClosed && !finished) 1L else 0L
            return preConnectBytes + (written?.toLong() ?: 0L) + tail
        }

    override fun connect(host: String, port: Int, deadlineMs: Long) {
        check(phase == Phase.Idle) { "NodeTransport: connect() once per transport" }
        phase = Phase.Connecting
        this.deadlineMs = deadlineMs
        val options = jsObject()
        options.host = host
        options.port = port
        options.allowHalfOpen = true
        options.noDelay = true
        if (lookup != null) options.lookup = lookup
        val s: dynamic = if (tls == null) {
            netConnect(options)
        } else {
            try {
                applyTls(options, tls, host, verifyHostname)
            } catch (e: Throwable) {
                fail(MqttError.Io(KIND_CONFIG, IllegalStateException("TLS settings rejected")))
                return
            }
            try {
                tlsConnect(options)
            } catch (e: Throwable) {
                fail(MqttError.Io(KIND_CONFIG, IllegalStateException("TLS settings rejected: ${codeOf(e)}")))
                return
            }
        }
        socket = s
        s.on(if (tls == null) "connect" else "secureConnect") { onConnected() }
        s.on("data") { chunk: dynamic ->
            if (phase == Phase.Connected) events += TransportEvent.Bytes(bytesOf(chunk))
            context.wakeup()
        }
        s.on("end") {
            if (phase == Phase.Connected) {
                events += TransportEvent.Eof
            } else if (phase == Phase.Connecting) {
                fail(if (tls != null) sslError("EOF") else MqttError.Io("EOF", null))
            }
            context.wakeup()
        }
        s.on("error") { error: dynamic ->
            fail(errorOf(error))
            context.wakeup()
        }
        s.on("close") {
            // Закрытие без ошибки и без нашего close(): сокет уничтожен снаружи
            if (phase == Phase.Connecting || phase == Phase.Connected) fail(MqttError.Io("closed", null))
            context.wakeup()
        }
        s.on("drain") { context.wakeup() }
        s.on("finish") {
            finished = true
            context.wakeup()
        }
        if (!readInterest) s.pause()
    }

    override fun write(bytes: ByteArray): Int {
        if (phase == Phase.Closed || bytes.isEmpty()) return 0
        if (phase != Phase.Connected) {
            preConnect.addLast(bytes)
            preConnectBytes += bytes.size
            return 0
        }
        socket.write(bufferOf(bytes))
        return if (((socket.writableLength as? Number)?.toLong() ?: 0L) == 0L) bytes.size else 0
    }

    override fun closeOutput() {
        if (phase == Phase.Closed || outputClosed) return
        outputClosed = true
        if (phase == Phase.Connected) socket.end()
    }

    override fun setReadInterest(enabled: Boolean) {
        readInterest = enabled
        val s = socket ?: return
        if (phase == Phase.Closed) return
        if (enabled) s.resume() else s.pause()
    }

    override fun poll(nowMs: Long) {
        val deadline = deadlineMs
        if (phase == Phase.Connecting && deadline != null && nowMs >= deadline) fail(MqttError.ConnectTimeout)
    }

    override fun nextDeadlineMs(): Long? = if (phase == Phase.Connecting) deadlineMs else null

    override fun events(): List<TransportEvent> {
        if (events.isEmpty()) return emptyList()
        val out = ArrayList(events)
        events.clear()
        return out
    }

    override fun close() {
        if (phase == Phase.Closed) return
        phase = Phase.Closed
        preConnect.clear()
        preConnectBytes = 0
        val s = socket ?: return
        socket = null
        s.removeAllListeners()
        s.on("error") { _: dynamic -> } // поздние ошибки уничтожаемого сокета не должны стать необработанными
        s.destroy()
    }

    private fun onConnected() {
        if (phase != Phase.Connecting) return
        phase = Phase.Connected
        deadlineMs = null
        events += TransportEvent.Connected
        while (preConnect.isNotEmpty()) socket.write(bufferOf(preConnect.removeFirst()))
        preConnectBytes = 0
        if (outputClosed) socket.end()
        context.wakeup()
    }

    private fun fail(error: MqttError) {
        if (phase == Phase.Closed) return
        events += TransportEvent.Failed(error)
        close()
    }

    private fun errorOf(error: dynamic): MqttError {
        val code = codeOf(error)
        return when {
            code in UNKNOWN_HOST_CODES -> MqttError.UnknownHost
            tls != null && !NETWORK_CODE.matches(code) -> sslError(code)
            else -> MqttError.Io(code, error as? Throwable)
        }
    }

    private fun sslError(code: String): MqttError =
        MqttError.Io(KIND_SSL, IllegalStateException("TLS failure: $code"))

    internal companion object {
        const val KIND_SSL = "ssl"
        const val KIND_CONFIG = "ssl-config"

        private val UNKNOWN_HOST_CODES = setOf("ENOTFOUND", "EAI_AGAIN", "EAI_NONAME", "EAI_NODATA", "EAI_FAIL")

        /** Сетевые коды libuv: `ECONNREFUSED`, `ETIMEDOUT`… (у ошибок TLS — `ERR_SSL_…`, `CERT_…` и т. п.). */
        private val NETWORK_CODE = Regex("E[A-Z]+")

        fun codeOf(error: dynamic): String {
            val code = error?.code as? String
            if (!code.isNullOrEmpty()) return code
            val name = error?.name as? String
            return if (!name.isNullOrEmpty()) name else "Error"
        }

        private fun pem(value: String): dynamic =
            inlinePemOrNull(value) ?: readFileSync(value)

        /** PEM сертификата (строка или файл), разобранный `X509Certificate`: неверный — исключение. */
        private fun parsedCertificate(value: String): dynamic {
            val content = pem(value)
            X509Certificate(content)
            return content
        }

        private val NO_IDENTITY_CHECK: dynamic = js("(function (host, cert) { return undefined; })")

        private fun applyTls(options: dynamic, settings: TLSClientSettings, host: String, verifyHostname: Boolean) {
            options.minVersion = "TLSv1.2"
            // Неверный PEM Node молча пропустил бы (ошибка была бы «не доверяю» при рукопожатии) — разбираем сами,
            // как JVM (`CertificateFactory`): ошибка — `ssl-config`
            settings.serverCertificate?.let { options.ca = parsedCertificate(it) }
            if (!settings.checkServerCertificate) options.rejectUnauthorized = false
            // Имя хоста не проверяется (verifyHostname = false): цепочка проверяется, личность сервера — нет
            if (settings.checkServerCertificate && !verifyHostname) options.checkServerIdentity = NO_IDENTITY_CHECK
            settings.clientCertificate?.let { options.cert = parsedCertificate(it) }
            if (settings.clientCertificate != null && settings.clientCertificateKey == null) {
                throw IllegalArgumentException("client certificate key missing")
            }
            settings.clientCertificateKey?.let {
                val key = pem(it)
                val keyOptions = jsObject()
                keyOptions.key = key
                settings.clientCertificatePassword?.let { password -> keyOptions.passphrase = password }
                createPrivateKey(keyOptions)
                options.key = key
            }
            settings.clientCertificatePassword?.let { options.passphrase = it }
            settings.alpnProtocols?.let { options.ALPNProtocols = it.toTypedArray() }
            val serverName = settings.serverNameIndications ?: host.takeUnless { isIpLiteral(it) }
            if (serverName != null) options.servername = serverName
        }

        private fun isIpLiteral(host: String): Boolean =
            host.contains(':') || host.all { it.isDigit() || it == '.' }
    }
}
