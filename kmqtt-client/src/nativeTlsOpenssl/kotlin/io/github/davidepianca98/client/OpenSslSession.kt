package io.github.davidepianca98.client

import io.github.davidepianca98.socket.tls.TLSClientSettings
import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.UByteVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.cstr
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.nativeHeap
import kotlinx.cinterop.ptr
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.set
import kotlinx.cinterop.staticCFunction
import kotlinx.cinterop.usePinned
import kotlinx.cinterop.value
import openssl.*

// этот файл компилируется отдельно в каждой native-цели с TLS (mingwX64, linuxX64, linuxArm64, iOS, macOS):
// klib OpenSSL у каждой цели свой и не коммонизован. Код одинаков для всех целей: `long` C (32 бита на mingwX64,
// 64 — на Linux и Apple) — только через convert()/toLong().

internal actual fun openSslSessionFactory(settings: TLSClientSettings, verifyHostname: Boolean): NativeTlsSessionFactory =
    NativeTlsSessionFactory { host -> OpenSslSession.open(settings, verifyHostname, host) }

/**
 * Сессия OpenSSL с memory-BIO. Контекст `SSL_CTX` строится на каждое подключение ([open]) и сразу
 * отпускается: сессия держит на него свою ссылку (`SSL_new`), он освобождается вместе с ней ([free]).
 *
 * Доверие и проверки — как у `TlsTransport` JVM:
 * - только TLS 1.2 и 1.3 (`SSL_CTX_set_min_proto_version`);
 * - `serverCertificate` (PEM-строка или путь) — единственный якорь доверия, системное хранилище не
 *   используется; закреплённым может быть и самоподписанный, и УЦ, и лист под УЦ (`X509_V_FLAG_PARTIAL_CHAIN`, как
 *   якорь PKIX на JVM); срок действия якоря не проверяется, срок листа под ним — проверяется;
 * - без закрепления — системные корни ([systemCaFiles], [systemCaDirs], [systemStoreCertificates], плюс умолчания
 *   OpenSSL и переменные `SSL_CERT_FILE`/`SSL_CERT_DIR`);
 * - `checkServerCertificate = false` без закрепления — проверки нет (доверять всем); фасад требует тогда
 *   `verifyHostname = false`;
 * - проверка имени хоста — при `checkServerCertificate && verifyHostname`: имя — `serverNameIndications`, если задано,
 *   иначе адрес подключения; IP-литерал сверяется с IP в SAN, имя — с DNS в SAN (без частичных шаблонов `a*.b`);
 * - SNI — `serverNameIndications` или имя хоста (для IP-литерала SNI не шлётся), ALPN — `alpnProtocols`;
 * - ключ клиента — PEM-строка или путь, с паролем `clientCertificatePassword` (пароль в памяти C затирается).
 *
 * Вид отказа рукопожатия — по `SSL_get_verify_result`: срок — `ssl-expired`, имя — `ssl-hostname`,
 * путь доверия — `ssl-trust`, остальное — `ssl`. OpenSSL проверяет имя раньше сроков, поэтому у сертификата, который
 * и просрочен, и выдан на другое имя, вид — `ssl-hostname` (на JVM — `ssl-expired`).
 */
internal class OpenSslSession private constructor(private val ssl: CPointer<SSL>) : NativeTlsSession {

    private var freed = false
    private var done = false
    private var shutdownSent = false

    private val readBio: CPointer<BIO> = SSL_get_rbio(ssl) ?: error("no read BIO")
    private val writeBio: CPointer<BIO> = SSL_get_wbio(ssl) ?: error("no write BIO")

    override val isHandshakeDone: Boolean get() = done

    override fun handshake(): Boolean {
        if (done) return true
        ERR_clear_error()
        val rc = SSL_do_handshake(ssl)
        if (rc == 1) {
            done = true
            return true
        }
        when (SSL_get_error(ssl, rc)) {
            SSL_ERROR_WANT_READ, SSL_ERROR_WANT_WRITE -> return false
        }
        val verify = SSL_get_verify_result(ssl).toInt()
        ERR_clear_error()
        throw NativeTlsException(handshakeKind(verify), HANDSHAKE_FAILED)
    }

    override fun feed(bytes: ByteArray) {
        if (bytes.isEmpty()) return
        bytes.usePinned { pinned ->
            var offset = 0
            while (offset < bytes.size) {
                val n = BIO_write(readBio, pinned.addressOf(offset), bytes.size - offset)
                if (n <= 0) throw NativeTlsException(KIND_TLS, TLS_FAILURE)
                offset += n
            }
        }
    }

    override fun readPlain(into: ByteArray): Int {
        if (into.isEmpty()) return 0
        ERR_clear_error()
        val n = into.usePinned { SSL_read(ssl, it.addressOf(0), into.size) }
        if (n > 0) return n
        return when (SSL_get_error(ssl, n)) {
            SSL_ERROR_WANT_READ, SSL_ERROR_WANT_WRITE -> 0
            SSL_ERROR_ZERO_RETURN -> NativeTlsSession.READ_CLOSED
            else -> {
                ERR_clear_error()
                throw NativeTlsException(KIND_TLS, TLS_FAILURE)
            }
        }
    }

    override fun writePlain(bytes: ByteArray, offset: Int, length: Int): Int {
        if (length <= 0) return 0
        ERR_clear_error()
        val n = bytes.usePinned { SSL_write(ssl, it.addressOf(offset), length) }
        if (n > 0) return n
        return when (SSL_get_error(ssl, n)) {
            SSL_ERROR_WANT_READ, SSL_ERROR_WANT_WRITE -> 0
            else -> {
                ERR_clear_error()
                throw NativeTlsException(KIND_TLS, TLS_FAILURE)
            }
        }
    }

    override fun takeOutput(): ByteArray? {
        val pending = BIO_ctrl(writeBio, BIO_CTRL_PENDING, 0.convert(), null).toLong()
        if (pending <= 0L) return null
        val out = ByteArray(pending.toInt())
        val n = out.usePinned { BIO_read(writeBio, it.addressOf(0), out.size) }
        return when {
            n <= 0 -> null
            n < out.size -> out.copyOf(n)
            else -> out
        }
    }

    override fun shutdown() {
        if (shutdownSent || !done) return
        shutdownSent = true
        ERR_clear_error()
        SSL_shutdown(ssl)
        ERR_clear_error()
    }

    override fun free() {
        if (freed) return
        freed = true
        SSL_free(ssl) // вместе с BIO и ссылкой на SSL_CTX
    }

    companion object {
        private const val HANDSHAKE_FAILED = "TLS handshake failed"
        private const val TLS_FAILURE = "TLS failure"
        private const val CONFIG_FAILED = "TLS configuration failed"

        /** `X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS` (x509v3.h, в klib нет). */
        private const val NO_PARTIAL_WILDCARDS: UInt = 0x4u

        /** Сессия для подключения к [host]; отказ настроек — [NativeTlsException] `ssl-config`. */
        fun open(settings: TLSClientSettings, verifyHostname: Boolean, host: String): OpenSslSession {
            ERR_clear_error()
            val ctx = buildContext(settings)
            try {
                val ssl = SSL_new(ctx) ?: throw config("SSL_new")
                try {
                    configure(ssl, settings, verifyHostname, host)
                    return OpenSslSession(ssl)
                } catch (e: Throwable) {
                    SSL_free(ssl)
                    throw e
                }
            } finally {
                SSL_CTX_free(ctx) // у SSL своя ссылка на контекст
                ERR_clear_error()
            }
        }

        /** Для тестов: сколько объектов (сертификатов) оказалось в хранилище после загрузки системных корней. */
        internal fun systemRootCount(): Int {
            val ctx = SSL_CTX_new(TLS_client_method()) ?: return -1
            try {
                loadSystemRoots(ctx)
                return sk_X509_OBJECT_num(X509_STORE_get0_objects(SSL_CTX_get_cert_store(ctx)))
            } finally {
                SSL_CTX_free(ctx)
            }
        }

        private fun config(step: String): NativeTlsException = NativeTlsException(KIND_TLS_CONFIG, "$CONFIG_FAILED ($step)")

        private fun buildContext(settings: TLSClientSettings): CPointer<SSL_CTX> {
            val ctx = SSL_CTX_new(TLS_client_method()) ?: throw config("SSL_CTX_new")
            try {
                if (SSL_CTX_ctrl(ctx, SSL_CTRL_SET_MIN_PROTO_VERSION, TLS1_2_VERSION.convert(), null).toLong() != 1L) {
                    throw config("min protocol")
                }
                // Повтор SSL_write после WANT_* — с тем же содержимым, но массив может быть закреплён по другому адресу
                SSL_CTX_ctrl(ctx, SSL_CTRL_MODE, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER.convert(), null)
                val pinned = settings.serverCertificate
                when {
                    pinned != null -> {
                        loadPinned(ctx, pinned)
                        X509_VERIFY_PARAM_set_flags(SSL_CTX_get0_param(ctx), X509_V_FLAG_PARTIAL_CHAIN.convert())
                        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, staticCFunction(::verifyCallback))
                    }
                    !settings.checkServerCertificate -> SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, null)
                    else -> {
                        loadSystemRoots(ctx)
                        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, staticCFunction(::verifyCallback))
                    }
                }
                settings.clientCertificate?.let { loadClientKey(ctx, it, settings) }
                settings.alpnProtocols?.let { setAlpn(ctx, it) }
                return ctx
            } catch (e: Throwable) {
                SSL_CTX_free(ctx)
                throw e
            }
        }

        private fun configure(ssl: CPointer<SSL>, settings: TLSClientSettings, verifyHostname: Boolean, host: String) {
            val sni = settings.serverNameIndications
            val ip = ipLiteralOrNull(host)
            val sniName = sni ?: if (ip == null) host else null
            if (sniName != null) {
                val rc = memScoped {
                    SSL_ctrl(ssl, SSL_CTRL_SET_TLSEXT_HOSTNAME, TLSEXT_NAMETYPE_host_name.convert(), sniName.cstr.ptr)
                }
                if (rc.toLong() != 1L) throw config("SNI")
            }
            // Как на JVM: имя проверяется, только если проверяется сертификат (с закреплённым — тоже) и не выключено явно
            if (settings.checkServerCertificate && verifyHostname) {
                val name = sni ?: host
                val nameIp = ipLiteralOrNull(name)
                if (nameIp != null) {
                    if (X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl), nameIp) != 1) throw config("host IP")
                } else {
                    SSL_set_hostflags(ssl, NO_PARTIAL_WILDCARDS)
                    if (SSL_set1_host(ssl, name) != 1) throw config("host name")
                }
            }
            val rbio = BIO_new(BIO_s_mem()) ?: throw config("BIO_new")
            val wbio = BIO_new(BIO_s_mem())
            if (wbio == null) {
                BIO_free(rbio)
                throw config("BIO_new")
            }
            SSL_set_bio(ssl, rbio, wbio) // теперь BIO принадлежат SSL
            SSL_set_connect_state(ssl)
        }

        /** Закреплённый сертификат: PEM-строка (один блок) или путь к файлу PEM (все сертификаты файла). */
        private fun loadPinned(ctx: CPointer<SSL_CTX>, value: String) {
            val pem = try {
                inlinePemOrNull(value)
            } catch (_: InvalidPemException) {
                throw config("pinned certificate PEM")
            }
            if (pem == null) {
                if (SSL_CTX_load_verify_locations(ctx, value, null) != 1) throw config("pinned certificate file")
                return
            }
            val cert = readCertificate(pem) ?: throw config("pinned certificate PEM")
            try {
                if (X509_STORE_add_cert(SSL_CTX_get_cert_store(ctx), cert) != 1) throw config("pinned certificate")
            } finally {
                X509_free(cert)
            }
        }

        private fun readCertificate(pem: String): CPointer<X509>? {
            val bio = BIO_new(BIO_s_mem()) ?: return null
            try {
                BIO_puts(bio, pem)
                return PEM_read_bio_X509(bio, null, null, null)
            } finally {
                BIO_free(bio)
            }
        }

        /** Системные корни: умолчания OpenSSL (и `SSL_CERT_FILE`/`SSL_CERT_DIR`), связки и каталоги ОС, хранилище ОС. */
        private fun loadSystemRoots(ctx: CPointer<SSL_CTX>) {
            SSL_CTX_set_default_verify_paths(ctx)
            for (file in systemCaFiles()) SSL_CTX_load_verify_locations(ctx, file, null)
            for (dir in systemCaDirs()) SSL_CTX_load_verify_locations(ctx, null, dir)
            val store = SSL_CTX_get_cert_store(ctx)
            for (der in systemStoreCertificates()) {
                if (der.isEmpty()) continue
                val cert = der.usePinned { pinned ->
                    memScoped {
                        val cursor = alloc<CPointerVar<UByteVar>>()
                        cursor.value = pinned.addressOf(0).reinterpret()
                        d2i_X509(null, cursor.ptr, der.size.convert())
                    }
                } ?: continue
                X509_STORE_add_cert(store, cert) // повтор того же корня — ошибка, она не мешает
                X509_free(cert)
            }
            // Отсутствующие файлы и повторы оставляют ошибки в очереди потока: они не относятся к подключению
            ERR_clear_error()
        }

        private fun loadClientKey(ctx: CPointer<SSL_CTX>, certificate: String, settings: TLSClientSettings) {
            val keySource = settings.clientCertificateKey ?: throw config("client key missing")
            val certPem = try {
                inlinePemOrNull(certificate)
            } catch (_: InvalidPemException) {
                throw config("client certificate PEM")
            }
            if (certPem != null) {
                val cert = readCertificate(certPem) ?: throw config("client certificate PEM")
                try {
                    if (SSL_CTX_use_certificate(ctx, cert) != 1) throw config("client certificate")
                } finally {
                    X509_free(cert)
                }
            } else if (SSL_CTX_use_certificate_file(ctx, certificate, SSL_FILETYPE_PEM) != 1) {
                throw config("client certificate file")
            }
            val keyPem = try {
                inlinePemOrNull(keySource)
            } catch (_: InvalidPemException) {
                throw config("client key PEM")
            }
            withPassword(settings.clientCertificatePassword) { password ->
                if (keyPem != null) {
                    val bio = BIO_new(BIO_s_mem()) ?: throw config("BIO_new")
                    val key = try {
                        BIO_puts(bio, keyPem)
                        PEM_read_bio_PrivateKey(bio, null, null, password)
                    } finally {
                        BIO_free(bio)
                    }
                    key ?: throw config("client key PEM")
                    try {
                        if (SSL_CTX_use_PrivateKey(ctx, key) != 1) throw config("client key")
                    } finally {
                        EVP_PKEY_free(key)
                    }
                } else {
                    // Пароль файла ключа — стандартный обработчик OpenSSL читает его из userdata контекста
                    SSL_CTX_set_default_passwd_cb_userdata(ctx, password)
                    try {
                        if (SSL_CTX_use_PrivateKey_file(ctx, keySource, SSL_FILETYPE_PEM) != 1) {
                            throw config("client key file")
                        }
                    } finally {
                        SSL_CTX_set_default_passwd_cb_userdata(ctx, null)
                    }
                }
            }
            if (SSL_CTX_check_private_key(ctx) != 1) throw config("client certificate and key do not match")
        }

        /** Пароль ключа строкой C на время [block]; после — затирается и освобождается. */
        private inline fun <T> withPassword(password: String?, block: (CPointer<ByteVar>?) -> T): T {
            if (password == null) return block(null)
            val bytes = password.encodeToByteArray()
            val buffer = nativeHeap.allocArray<ByteVar>(bytes.size + 1)
            try {
                for (i in bytes.indices) buffer[i] = bytes[i]
                buffer[bytes.size] = 0
                return block(buffer)
            } finally {
                OPENSSL_cleanse(buffer, (bytes.size + 1).convert())
                bytes.fill(0)
                nativeHeap.free(buffer.rawValue)
            }
        }

        private fun setAlpn(ctx: CPointer<SSL_CTX>, protocols: List<String>) {
            val wire = ArrayList<Byte>()
            for (proto in protocols) {
                val bytes = proto.encodeToByteArray()
                if (bytes.isEmpty() || bytes.size > 255) throw config("ALPN")
                wire += bytes.size.toByte()
                wire.addAll(bytes.toList())
            }
            val array = wire.toByteArray()
            if (array.isEmpty()) return
            val rc = array.usePinned { SSL_CTX_set_alpn_protos(ctx, it.addressOf(0).reinterpret(), array.size.convert()) }
            if (rc != 0) throw config("ALPN")
        }

        /** Вид отказа рукопожатия по коду проверки сертификата. */
        internal fun handshakeKind(verify: Int): String = when (verify) {
            X509_V_OK -> KIND_TLS
            X509_V_ERR_CERT_HAS_EXPIRED, X509_V_ERR_CERT_NOT_YET_VALID -> KIND_TLS_EXPIRED
            X509_V_ERR_HOSTNAME_MISMATCH, X509_V_ERR_IP_ADDRESS_MISMATCH -> KIND_TLS_HOSTNAME
            X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT,
            X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY,
            X509_V_ERR_UNABLE_TO_VERIFY_LEAF_SIGNATURE,
            X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT,
            X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN,
            X509_V_ERR_CERT_SIGNATURE_FAILURE,
            X509_V_ERR_CERT_UNTRUSTED,
            X509_V_ERR_CERT_REJECTED,
            X509_V_ERR_INVALID_CA,
            X509_V_ERR_PATH_LENGTH_EXCEEDED,
            X509_V_ERR_INVALID_PURPOSE -> KIND_TLS_TRUST
            else -> KIND_TLS
        }
    }
}

/**
 * Проверка цепочки: всё, как решил OpenSSL, кроме срока действия якоря доверия (сертификата из хранилища — закреплённого
 * или системного корня): его срок не проверяется, как у PKIX на JVM. Якорь — сертификат цепочки с глубиной
 * не меньше числа недоверенных (`num_untrusted`): они идут от листа, доверенные — над ними.
 */
private fun verifyCallback(ok: Int, store: CPointer<X509_STORE_CTX>?): Int {
    if (ok == 1 || store == null) return ok
    val error = X509_STORE_CTX_get_error(store)
    if (error == X509_V_ERR_CERT_HAS_EXPIRED || error == X509_V_ERR_CERT_NOT_YET_VALID) {
        val depth = X509_STORE_CTX_get_error_depth(store)
        if (depth >= X509_STORE_CTX_get_num_untrusted(store)) {
            X509_STORE_CTX_set_error(store, X509_V_OK)
            return 1
        }
    }
    return 0
}
