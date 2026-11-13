package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.socket.tls.TLSClientSettings
import java.io.File
import java.io.InputStream
import java.nio.ByteBuffer
import java.security.KeyFactory
import java.security.KeyStore
import java.security.PrivateKey
import java.security.cert.CertPathBuilderException
import java.security.cert.CertPathValidatorException
import java.security.cert.CertificateException
import java.security.cert.CertificateExpiredException
import java.security.cert.CertificateFactory
import java.security.cert.CertificateNotYetValidException
import java.security.cert.X509Certificate
import java.security.spec.InvalidKeySpecException
import java.security.spec.PKCS8EncodedKeySpec
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SNIHostName
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLEngineResult
import javax.net.ssl.SSLException
import javax.net.ssl.SSLHandshakeException
import javax.net.ssl.SSLPeerUnverifiedException
import javax.net.ssl.TrustManager
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

/**
 * Фабрика TLS-транспорта для `NioActorHost`: `TlsTransport(NioTransport(context), settings, verifyHostname)`.
 * Настройки ([settings]) не печатаются и не попадают в ошибки (в них пароль и ключ клиента).
 *
 * [SSLContext] (разбор сертификатов и ключей, `cacerts` без закрепления) строится один раз на фабрику при первом
 * подключении и используется всеми последующими; при ошибке настроек он не запоминается, и следующее
 * подключение пробует снова (например, появился файл сертификата). Файлы сертификатов читаются один раз.
 */
@MqttCoreApi
public fun NioTransportFactory.Companion.tls(
    settings: TLSClientSettings,
    verifyHostname: Boolean = true
): NioTransportFactory = tls(settings, verifyHostname, SslContextCache(settings))

@MqttCoreApi
internal fun NioTransportFactory.Companion.tls(
    settings: TLSClientSettings,
    verifyHostname: Boolean,
    contexts: SslContextCache
): NioTransportFactory = NioTransportFactory { TlsTransport(NioTransport(it), settings, verifyHostname, contexts) }

/**
 * [SSLContext] по настройкам, построенный лениво один раз ([build] вызывается не больше одного успешного раза).
 * Контекст потокобезопасен; ошибка построения не запоминается. Параметр [build] — для тестов.
 */
internal class SslContextCache(
    private val settings: TLSClientSettings,
    private val build: (TLSClientSettings) -> SSLContext = ::buildSslContext
) {
    @Volatile
    private var cached: SSLContext? = null

    fun get(): SSLContext = cached ?: synchronized(this) {
        cached ?: build(settings).also { cached = it }
    }
}

/**
 * TLS поверх [NioTransport] через [SSLEngine] в клиентском режиме.
 *
 * Хост обслуживает готовность сокета у внутреннего транспорта ([inner], ключ селектора ссылается на него), а события
 * и действия — у этого. Все методы — только на потоке хоста; задачи `NEED_TASK` исполняются на нём же (они короткие).
 *
 * - [SSLEngine] (сертификаты, ключи, доверие) создаётся в [connect] до открытия сокета: ошибка настроек —
 *   [TransportEvent.Failed] с [MqttError.Io] `kind = "ssl-config"`, соединение не открывается. [SSLContext] под ним
 *   строится один раз на фабрику [tls], а не при каждом переподключении.
 * - Рукопожатие — по событиям внутреннего транспорта; [TransportEvent.Connected] — только после него. Срок
 *   [connect] покрывает резолв, TCP и рукопожатие: по нему [MqttError.ConnectTimeout].
 * - Только TLS 1.2 и 1.3 (остальные версии из включённых движком отбрасываются; если не осталось ни одной — ошибка
 *   настроек). SNI — по имени хоста. Проверка имени хоста (`endpointIdentificationAlgorithm = "HTTPS"`) включена,
 *   если проверяется сертификат сервера (`checkServerCertificate = true`, в т. ч. с `serverCertificate`) и
 *   [verifyHostname] не выключен; при `checkServerCertificate = false` (доверять всем) не проверяется ничего.
 *   Фасад (`MqttClient`) такую комбинацию с `verifyHostname = true` не пропускает; этот уровень
 *   (`@MqttCoreApi`) её не проверяет — за явность отвечает тот, кто собирает хост сам.
 * - Буферы — по `SSLSession.packetBufferSize`/`applicationBufferSize` (растут, если сессия их увеличила).
 *   Каждая TLS-запись уходит во внутренний транспорт отдельным массивом (он не копирует).
 * - Ошибка TLS (рукопожатие, запись) — [TransportEvent.Failed] с [MqttError.Io] `kind = "ssl"`. При отказе
 *   рукопожатия по сертификату сервера вид уточняется по корневой причине исключения: `ssl-hostname` —
 *   имя хоста не совпало с сертификатом, `ssl-expired` — срок действия сертификата вышел (или ещё не наступил),
 *   `ssl-trust` — сертификат не доверенный (нет пути до закреплённого сертификата или системного хранилища); прочее —
 *   `ssl`. Уточнён только вид (строка без адреса), а не текст. Причина — исключение
 *   того же семейства ([SSLHandshakeException]/[SSLException]) с общим текстом: исходный текст исключений
 *   `SSLEngine` содержит адрес и имена из сертификата (H11), поэтому не передаётся.
 * - Удалённая сторона закрыла поток (close_notify или EOF TCP после рукопожатия) — [TransportEvent.Eof] после всех
 *   расшифрованных данных; EOF во время рукопожатия — ошибка `ssl`.
 * - [close]: после рукопожатия — `closeOutbound` и запись close_notify без ожидания ответа, затем закрытие сокета.
 *   При `HostAction.FlushAndClose` хост сначала вызывает [closeOutput] (close_notify — в очередь за данными), затем
 *   [close], когда [pendingWriteBytes] стал 0: close_notify ушёл в ОС последней записью.
 *
 * [pendingWriteBytes] — открытый текст, ждущий конца рукопожатия, плюс шифротекст в очереди внутреннего транспорта.
 */
@MqttCoreApi
public class TlsTransport internal constructor(
    private val inner: NioTransport,
    private val settings: TLSClientSettings,
    private val verifyHostname: Boolean,
    private val contexts: SslContextCache
) : MqttTransport {

    /** Транспорт с собственным [SSLContext], построенным при [connect]; фабрика [tls] делит один контекст на все подключения. */
    public constructor(
        inner: NioTransport,
        settings: TLSClientSettings,
        verifyHostname: Boolean = true
    ) : this(inner, settings, verifyHostname, SslContextCache(settings))

    private enum class Phase { Idle, Connecting, Handshaking, Open, Closed }

    private var phase = Phase.Idle
    private var engine: SSLEngine? = null
    private var deadlineMs: Long? = null
    private val events = ArrayList<TransportEvent>()

    /** Шифротекст от внутреннего транспорта, ещё не переложенный в [netIn]. */
    private val inbound = ArrayDeque<ByteBuffer>()

    /** Шифротекст для `unwrap`, в режиме записи между вызовами. */
    private var netIn: ByteBuffer = EMPTY

    /** Расшифрованное. */
    private var appIn: ByteBuffer = EMPTY

    /** Буфер для `wrap`; содержимое после каждой записи копируется. */
    private var netOut: ByteBuffer = EMPTY

    /** Открытый текст до конца рукопожатия. */
    private val appOut = ArrayDeque<ByteBuffer>()
    private var appOutBytes = 0L

    private var readInterest = true
    private var innerEof = false
    private var peerClosed = false
    private var eofSent = false

    override val pendingWriteBytes: Long
        get() = if (phase == Phase.Closed) 0 else appOutBytes + inner.pendingWriteBytes

    override fun connect(host: String, port: Int, deadlineMs: Long) {
        check(phase == Phase.Idle) { "TlsTransport: connect() once per transport" }
        this.deadlineMs = deadlineMs
        val created = try {
            createEngine(contexts.get(), settings, host, port, verifyHostname)
        } catch (e: Exception) {
            phase = Phase.Connecting
            fail(MqttError.Io(KIND_CONFIG, sanitized(e, handshake = false)))
            return
        }
        engine = created
        allocateBuffers(created)
        phase = Phase.Connecting
        inner.connect(host, port, deadlineMs)
    }

    /**
     * Ставит [bytes] на шифрование: до рукопожатия — в очередь, после — сразу в TLS-записи. Возвращает
     * `bytes.size`, если весь шифротекст сразу ушёл в ОС, иначе 0 (шифротекст ждёт `OP_WRITE`).
     */
    override fun write(bytes: ByteArray): Int {
        if (phase == Phase.Closed || bytes.isEmpty()) return 0
        appOut.addLast(ByteBuffer.wrap(bytes))
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
            // Исключение не должно останавливать цикл хоста (например, нехватка дескрипторов при открытии канала)
            fail(NioTransport.errorOf(e))
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
     * Для `HostAction.FlushAndClose`: зашифровать оставшийся открытый текст, затем `closeOutbound` и close_notify —
     * в очередь внутреннего транспорта за всеми TLS-записями данных. Хост закроет транспорт, когда очередь опустеет:
     * close_notify уходит последней записью (при прямом [close] он теряется, если буфер отправки сокета полон).
     * До конца рукопожатия — ничего (данных приложения ещё нет; [close] закроет сокет).
     */
    override fun closeOutput() {
        if (phase != Phase.Open) return
        pump()
        val e = engine ?: return
        if (phase != Phase.Open) return
        try {
            e.closeOutbound()
            var guard = 0
            while (!e.isOutboundDone && guard++ < 4) {
                if (!wrapInto(e, EMPTY_ARRAY)) break
            }
        } catch (_: Exception) {
            // close_notify — по возможности; данные уже в очереди
        }
        // После closeOutbound открытый текст не зашифровать: остаток (его быть не должно) не ждём
        appOut.clear()
        appOutBytes = 0
    }

    override fun close() {
        if (phase == Phase.Closed) return
        val wasOpen = phase == Phase.Open
        phase = Phase.Closed
        val e = engine
        if (wasOpen && e != null) {
            try {
                e.closeOutbound()
                var guard = 0
                while (!e.isOutboundDone && guard++ < 4) {
                    if (!wrapInto(e, EMPTY_ARRAY)) break
                }
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
        engine = null
        inbound.clear()
        appOut.clear()
        appOutBytes = 0
        netIn = EMPTY
        appIn = EMPTY
        netOut = EMPTY
    }

    private fun fail(error: MqttError) {
        if (phase == Phase.Closed) return
        events += TransportEvent.Failed(error)
        release()
    }

    /** Забрать события внутреннего транспорта и продвинуть движок. */
    private fun pump() {
        if (phase == Phase.Closed || phase == Phase.Idle) return
        try {
            for (event in inner.events()) {
                if (phase == Phase.Closed) return
                when (event) {
                    TransportEvent.Connected -> onTcpConnected()
                    is TransportEvent.Bytes -> if (!peerClosed) inbound.addLast(ByteBuffer.wrap(event.bytes))
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
        } catch (e: Exception) {
            val handshake = phase == Phase.Handshaking
            fail(MqttError.Io(if (handshake) handshakeKind(e) else KIND_SSL, sanitized(e, handshake = handshake)))
        }
    }

    private fun onTcpConnected() {
        val e = engine ?: return
        phase = Phase.Handshaking
        inner.setReadInterest(true)
        e.beginHandshake()
    }

    private fun process() {
        val e = engine ?: return
        var progress = true
        while (progress && (phase == Phase.Handshaking || phase == Phase.Open)) {
            progress = false
            val status = e.handshakeStatus
            when (status) {
                SSLEngineResult.HandshakeStatus.NEED_TASK -> {
                    while (true) (e.delegatedTask ?: break).run()
                    progress = true
                }
                SSLEngineResult.HandshakeStatus.NEED_WRAP -> {
                    if (e.isOutboundDone) break
                    progress = wrapInto(e, EMPTY_ARRAY)
                }
                SSLEngineResult.HandshakeStatus.NEED_UNWRAP -> progress = unwrapOnce(e)
                SSLEngineResult.HandshakeStatus.FINISHED, SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING -> {
                    if (phase == Phase.Handshaking) {
                        onHandshakeDone(e)
                        progress = true
                    } else {
                        if (appOut.isNotEmpty()) progress = wrapAppData(e)
                        if (readInterest && unwrapOnce(e)) progress = true
                    }
                }
                // NEED_UNWRAP_AGAIN (JDK 9+): по имени, чтобы не ссылаться на константу, которой нет на старом Android
                else -> progress = unwrapOnce(e)
            }
        }
    }

    private fun onHandshakeDone(e: SSLEngine) {
        phase = Phase.Open
        deadlineMs = null
        allocateBuffers(e)
        inner.setReadInterest(readInterest)
        events += TransportEvent.Connected
    }

    /** Шифрует открытый текст из [appOut]; `true`, если что-то зашифровано. */
    private fun wrapAppData(e: SSLEngine): Boolean {
        var any = false
        while (appOut.isNotEmpty() && phase == Phase.Open) {
            val src = appOut.first()
            val before = src.remaining()
            if (!wrapInto(e, arrayOf(src))) {
                if (e.isOutboundDone) throw SSLException("outbound closed")
                break
            }
            appOutBytes -= before - src.remaining()
            any = true
            if (!src.hasRemaining()) appOut.removeFirst()
        }
        return any
    }

    /**
     * Один `wrap`: результат (если есть) — отдельным массивом во внутренний транспорт. `true`, если движок что-то
     * потребил или выдал.
     */
    private fun wrapInto(e: SSLEngine, src: Array<ByteBuffer>): Boolean {
        while (true) {
            netOut.clear()
            val result = e.wrap(src, netOut)
            when (result.status) {
                SSLEngineResult.Status.BUFFER_OVERFLOW -> {
                    netOut = ByteBuffer.allocate(maxOf(netOut.capacity() * 2, e.session.packetBufferSize))
                    continue
                }
                else -> {
                    if (netOut.position() > 0) {
                        netOut.flip()
                        val record = ByteArray(netOut.remaining())
                        netOut.get(record)
                        inner.write(record)
                    }
                    return result.bytesConsumed() > 0 || result.bytesProduced() > 0
                }
            }
        }
    }

    /** Один `unwrap` из накопленного шифротекста; `true`, если движок что-то потребил или выдал. */
    private fun unwrapOnce(e: SSLEngine): Boolean {
        if (peerClosed) return false
        fillNetIn()
        if (netIn.position() == 0) return false
        netIn.flip()
        val result = try {
            e.unwrap(netIn, appIn)
        } finally {
            netIn.compact()
        }
        deliverAppIn()
        return when (result.status) {
            SSLEngineResult.Status.BUFFER_UNDERFLOW -> {
                val need = e.session.packetBufferSize
                if (netIn.capacity() < need) {
                    netIn = grow(netIn, need)
                    true
                } else if (netIn.hasRemaining() && inbound.isNotEmpty()) {
                    true
                } else {
                    false
                }
            }
            SSLEngineResult.Status.BUFFER_OVERFLOW -> {
                appIn = ByteBuffer.allocate(maxOf(appIn.capacity() * 2, e.session.applicationBufferSize))
                true
            }
            SSLEngineResult.Status.CLOSED -> {
                // close_notify от сервера: дальше только Eof (и, у TLS 1.2, ответный close_notify через NEED_WRAP)
                peerClosed = true
                inbound.clear()
                true
            }
            else -> result.bytesConsumed() > 0 || result.bytesProduced() > 0
        }
    }

    private fun fillNetIn() {
        while (inbound.isNotEmpty() && netIn.hasRemaining()) {
            val chunk = inbound.first()
            if (chunk.remaining() <= netIn.remaining()) {
                netIn.put(chunk)
                inbound.removeFirst()
            } else {
                val slice = chunk.duplicate()
                slice.limit(slice.position() + netIn.remaining())
                chunk.position(slice.limit())
                netIn.put(slice)
            }
        }
    }

    private fun deliverAppIn() {
        if (appIn.position() == 0) return
        appIn.flip()
        val bytes = ByteArray(appIn.remaining())
        appIn.get(bytes)
        appIn.clear()
        events += TransportEvent.Bytes(bytes)
    }

    private fun checkEof() {
        if (phase == Phase.Handshaking && (peerClosed || (innerEof && inbound.isEmpty()))) {
            fail(MqttError.Io(KIND_SSL, SSLHandshakeException(HANDSHAKE_FAILED)))
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

    private fun allocateBuffers(e: SSLEngine) {
        val session = e.session
        if (netIn.capacity() < session.packetBufferSize) netIn = grow(netIn, session.packetBufferSize)
        if (netOut.capacity() < session.packetBufferSize) netOut = ByteBuffer.allocate(session.packetBufferSize)
        if (appIn.capacity() < session.applicationBufferSize) {
            val bigger = ByteBuffer.allocate(session.applicationBufferSize)
            appIn.flip()
            bigger.put(appIn)
            appIn = bigger
        }
    }

    private companion object {
        const val KIND_CONFIG = "ssl-config"
        const val HANDSHAKE_FAILED = "TLS handshake failed"
        val EMPTY: ByteBuffer = ByteBuffer.allocate(0)
        val EMPTY_ARRAY: Array<ByteBuffer> = arrayOf(ByteBuffer.allocate(0))

        fun grow(buffer: ByteBuffer, capacity: Int): ByteBuffer {
            val bigger = ByteBuffer.allocate(capacity)
            buffer.flip()
            bigger.put(buffer)
            return bigger
        }

        /** Исключение без исходного текста (в нём адрес, имена из сертификата, пути): только семейство и класс. */
        fun sanitized(e: Throwable, handshake: Boolean): Throwable {
            val name = e.javaClass.simpleName.ifEmpty { e.javaClass.name }
            return if (handshake) {
                SSLHandshakeException("$HANDSHAKE_FAILED ($name)")
            } else {
                SSLException("TLS failure ($name)")
            }
        }
    }
}

private const val KIND_SSL = "ssl"
private const val KIND_HOSTNAME = "ssl-hostname"
private const val KIND_TRUST = "ssl-trust"
private const val KIND_EXPIRED = "ssl-expired"
private const val MAX_CAUSE_DEPTH = 16

/**
 * Вид отказа рукопожатия по корневой причине. Читает только классы и коды причин по цепочке
 * `cause`; текст исключения в результат не попадает. Порядок: срок действия, имя хоста, доверие.
 */
internal fun handshakeKind(e: Throwable): String {
    val chain = ArrayList<Throwable>()
    var current: Throwable? = e
    while (current != null && chain.size < MAX_CAUSE_DEPTH) {
        val t: Throwable = current
        if (chain.any { it === t }) break
        chain += t
        current = t.cause
    }
    if (chain.any(::isExpiry)) return KIND_EXPIRED
    if (chain.any(::isHostnameMismatch)) return KIND_HOSTNAME
    // «Не доверенный» — только когда путь до якоря доверия не построен или не прошёл проверку (JDK: ValidatorException с
    // CertPathBuilderException, Android: CertPathValidatorException); незнакомое `CertificateException` остаётся общим `ssl`
    val untrusted = chain.any { it is CertPathValidatorException || it is CertPathBuilderException }
    return if (untrusted) KIND_TRUST else KIND_SSL
}

private fun isExpiry(t: Throwable): Boolean = t is CertificateExpiredException ||
    t is CertificateNotYetValidException ||
    (t is CertPathValidatorException && (
        t.reason == CertPathValidatorException.BasicReason.EXPIRED ||
            t.reason == CertPathValidatorException.BasicReason.NOT_YET_VALID
        ))

/**
 * JDK (`HostnameChecker`) сообщает о несовпадении имени как `CertificateException` с текстом «No subject
 * alternative ...»/«No name matching ...», Conscrypt — `SSLPeerUnverifiedException`. Текст только
 * сопоставляется и дальше не идёт.
 */
private fun isHostnameMismatch(t: Throwable): Boolean {
    if (t is SSLPeerUnverifiedException) return true
    if (t !is CertificateException || isExpiry(t)) return false
    val message = t.message ?: return false
    return message.startsWith("No subject alternative", ignoreCase = true) ||
        message.startsWith("No name matching", ignoreCase = true)
}

/**
 * [SSLEngine] клиента по [settings] (перенос `TLSClientSocket.createEngine` с SNI, проверкой имени хоста и
 * TLS 1.2+). Ошибки — исключения; их текст наружу не отдаётся (см. [TlsTransport]).
 */
internal fun createEngine(settings: TLSClientSettings, host: String, port: Int, verifyHostname: Boolean): SSLEngine =
    createEngine(buildSslContext(settings), settings, host, port, verifyHostname)

/** [SSLContext] по настройкам (сертификаты, ключ клиента, доверие); ошибки — исключения без обращения к сети. */
internal fun buildSslContext(settings: TLSClientSettings): SSLContext {
    val context = SSLContext.getInstance(settings.version)
    context.init(keyManagers(settings), trustManagers(settings), null)
    return context
}

/** [SSLEngine] из готового [context]: SNI, ALPN, проверка имени хоста, только TLS 1.2+. */
internal fun createEngine(
    context: SSLContext,
    settings: TLSClientSettings,
    host: String,
    port: Int,
    verifyHostname: Boolean
): SSLEngine {
    val engine = context.createSSLEngine(host, port)
    engine.useClientMode = true
    val params = engine.sslParameters
    val enabled = engine.enabledProtocols.toSet()
    val protocols = ALLOWED_PROTOCOLS.filter { it in enabled }
    if (protocols.isEmpty()) throw SSLException("no TLS 1.2+ protocol enabled")
    params.protocols = protocols.toTypedArray()
    if (settings.checkServerCertificate && verifyHostname) params.endpointIdentificationAlgorithm = "HTTPS"
    // ALPN и явное имя для SNI (upstream KMQTT, #95): по умолчанию SNI — имя хоста подключения
    settings.alpnProtocols?.let { params.applicationProtocols = it.toTypedArray() }
    settings.serverNameIndications?.let { params.serverNames = listOf(SNIHostName(it)) }
    engine.sslParameters = params
    return engine
}

/** Допустимые версии TLS (только 1.2+), по убыванию. */
private val ALLOWED_PROTOCOLS = listOf("TLSv1.3", "TLSv1.2")

private fun trustManagers(settings: TLSClientSettings): Array<TrustManager>? {
    val serverCertificate = settings.serverCertificate
    return when {
        serverCertificate != null -> {
            val certificate = openPem(serverCertificate).use {
                CertificateFactory.getInstance("X.509").generateCertificate(it)
            }
            val keyStore = KeyStore.getInstance(KeyStore.getDefaultType())
            keyStore.load(null, null)
            keyStore.setCertificateEntry("server", certificate)
            val factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
            factory.init(keyStore)
            factory.trustManagers
        }
        !settings.checkServerCertificate -> arrayOf(TrustAll)
        else -> null
    }
}

private fun keyManagers(settings: TLSClientSettings): Array<javax.net.ssl.KeyManager>? {
    val clientCertificate = settings.clientCertificate ?: return null
    val keySource = settings.clientCertificateKey ?: throw SSLException("client certificate key missing")
    val certificate = openPem(clientCertificate).use {
        CertificateFactory.getInstance("X.509").generateCertificate(it)
    }
    val keyContent = openPem(keySource).use { it.bufferedReader().readText() }
    val key = privateKey(keyContent)
    val password = settings.clientCertificatePassword?.toCharArray()
    val keyStore = KeyStore.getInstance(KeyStore.getDefaultType())
    keyStore.load(null, null)
    keyStore.setKeyEntry("client", key, password, arrayOf(certificate))
    val factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
    factory.init(keyStore, password)
    password?.fill('\u0000')
    return factory.keyManagers
}

/**
 * PEM-строка или путь к файлу PEM: строка с `-----BEGIN ` — всегда PEM ([inlinePemOrNull] приводит
 * перенос и отступы к единому виду, а не-PEM — [InvalidPemException]), остальное — путь; строка из настроек не
 * превращается в чтение файла.
 */
private fun openPem(value: String): InputStream =
    inlinePemOrNull(value)?.byteInputStream() ?: File(value).inputStream()

/**
 * Ключ PKCS#8 (`BEGIN PRIVATE KEY`; у старого клиента также под заголовком EC): сначала RSA, затем EC.
 * Base64 из stdlib Kotlin, а не `java.util.Base64`: его нет на Android до API 26 (minSdk 24).
 */
@OptIn(ExperimentalEncodingApi::class)
internal fun privateKey(pem: String): PrivateKey {
    val base64 = pem.lineSequence()
        .map { it.trim() }
        .filter { it.isNotEmpty() && !it.startsWith("-----") }
        .joinToString("")
    val spec = PKCS8EncodedKeySpec(Base64.Default.decode(base64))
    return try {
        KeyFactory.getInstance("RSA").generatePrivate(spec)
    } catch (_: InvalidKeySpecException) {
        KeyFactory.getInstance("EC").generatePrivate(spec)
    }
}

private object TrustAll : X509TrustManager {
    override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?) {}

    override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?) {}

    override fun getAcceptedIssuers(): Array<X509Certificate> = arrayOf()
}
