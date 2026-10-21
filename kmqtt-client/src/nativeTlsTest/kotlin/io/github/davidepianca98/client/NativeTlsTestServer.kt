package io.github.davidepianca98.client

import kotlinx.cinterop.CPointer
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.convert
import kotlinx.cinterop.toKString
import kotlinx.cinterop.usePinned
import openssl.*

/**
 * Сервер TLS на OpenSSL с memory-BIO для тестов: собеседник [NativeTlsSession] без сокетов.
 * [certificate]/[key] — PEM сервера; [maxVersion] — предел версии (например `TLS1_1_VERSION`); [clientCa] — требовать
 * сертификат клиента, выданный этим УЦ.
 */
internal class NativeTlsTestServer(
    certificate: String,
    key: String,
    maxVersion: Int? = null,
    clientCa: String? = null
) {
    private val ctx: CPointer<SSL_CTX> = SSL_CTX_new(TLS_server_method()) ?: error("SSL_CTX_new")
    private val ssl: CPointer<SSL>
    private val readBio: CPointer<BIO>
    private val writeBio: CPointer<BIO>
    private var freed = false

    init {
        val cert = readPem(certificate) { PEM_read_bio_X509(it, null, null, null) }
        val pkey = readPem(key) { PEM_read_bio_PrivateKey(it, null, null, null) }
        check(SSL_CTX_use_certificate(ctx, cert) == 1) { "SSL_CTX_use_certificate" }
        check(SSL_CTX_use_PrivateKey(ctx, pkey) == 1) { "SSL_CTX_use_PrivateKey" }
        X509_free(cert)
        EVP_PKEY_free(pkey)
        if (maxVersion != null) {
            // Старые версии разрешены только для проверки отказа клиента (уровень безопасности 0)
            SSL_CTX_set_security_level(ctx, 0)
            SSL_CTX_ctrl(ctx, SSL_CTRL_SET_MIN_PROTO_VERSION, TLS1_VERSION.convert(), null)
            SSL_CTX_ctrl(ctx, SSL_CTRL_SET_MAX_PROTO_VERSION, maxVersion.convert(), null)
        }
        if (clientCa != null) {
            val ca = readPem(clientCa) { PEM_read_bio_X509(it, null, null, null) }
            X509_STORE_add_cert(SSL_CTX_get_cert_store(ctx), ca)
            X509_free(ca)
            SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER or SSL_VERIFY_FAIL_IF_NO_PEER_CERT, null)
        } else {
            SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, null)
        }
        ssl = SSL_new(ctx) ?: error("SSL_new")
        readBio = BIO_new(BIO_s_mem()) ?: error("BIO_new")
        writeBio = BIO_new(BIO_s_mem()) ?: error("BIO_new")
        SSL_set_bio(ssl, readBio, writeBio)
        SSL_set_accept_state(ssl)
    }

    private fun <T> readPem(pem: String, read: (CPointer<BIO>) -> T?): T {
        val bio = BIO_new(BIO_s_mem()) ?: error("BIO_new")
        try {
            BIO_puts(bio, pem)
            return read(bio) ?: error("PEM")
        } finally {
            BIO_free(bio)
        }
    }

    /** Рукопожатие закончено. */
    val handshakeDone: Boolean get() = SSL_is_init_finished(ssl) != 0

    /** Имя SNI, которое прислал клиент, или `null`. */
    val serverName: String? get() = SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name)?.toKString()

    /**
     * Подтверждённый сертификат клиента (после рукопожатия с [clientCa]). `SSL_SESSION_get0_peer` есть и в OpenSSL
     * 1.1.1, и в 3.x (`SSL_get1_peer_certificate` — только в 3.x); ссылка не владеющая, освобождать не нужно.
     */
    val clientCertificateVerified: Boolean
        get() = SSL_get_session(ssl)?.let { SSL_SESSION_get0_peer(it) } != null &&
            SSL_get_verify_result(ssl).toInt() == X509_V_OK

    /** Получен close_notify клиента. */
    var closeNotifyReceived: Boolean = false
        private set

    /** Шаг сервера: принять шифротекст, продвинуть рукопожатие, расшифровать (открытый текст копится в [received]). */
    fun feed(bytes: ByteArray) {
        if (bytes.isNotEmpty()) {
            bytes.usePinned { BIO_write(readBio, it.addressOf(0), bytes.size) }
        }
        if (!handshakeDone) {
            ERR_clear_error()
            val rc = SSL_do_handshake(ssl)
            if (rc != 1) {
                val err = SSL_get_error(ssl, rc)
                if (err != SSL_ERROR_WANT_READ && err != SSL_ERROR_WANT_WRITE) handshakeFailed = true
                ERR_clear_error()
                return
            }
        }
        val buffer = ByteArray(16 * 1024)
        while (true) {
            ERR_clear_error()
            val n = buffer.usePinned { SSL_read(ssl, it.addressOf(0), buffer.size) }
            if (n > 0) {
                received.addAll(buffer.copyOf(n).toList())
                continue
            }
            if (SSL_get_error(ssl, n) == SSL_ERROR_ZERO_RETURN) closeNotifyReceived = true
            ERR_clear_error()
            return
        }
    }

    /** Рукопожатие сервера провалилось (например, клиент прислал оповещение). */
    var handshakeFailed: Boolean = false
        private set

    /** Расшифрованное от клиента. */
    val received: MutableList<Byte> = ArrayList()

    /** Зашифровать данные для клиента. */
    fun send(bytes: ByteArray) {
        bytes.usePinned { pinned ->
            var offset = 0
            while (offset < bytes.size) {
                val n = SSL_write(ssl, pinned.addressOf(offset), minOf(16 * 1024, bytes.size - offset))
                check(n > 0) { "SSL_write ${SSL_get_error(ssl, n)}" }
                offset += n
            }
        }
    }

    /** Поставить close_notify. */
    fun shutdown() {
        SSL_shutdown(ssl)
        ERR_clear_error()
    }

    /** Шифротекст для клиента или пустой массив. */
    fun takeOutput(): ByteArray {
        val pending = BIO_ctrl(writeBio, BIO_CTRL_PENDING, 0.convert(), null).toLong().toInt()
        if (pending <= 0) return ByteArray(0)
        val out = ByteArray(pending)
        out.usePinned { BIO_read(writeBio, it.addressOf(0), pending) }
        return out
    }

    fun free() {
        if (freed) return
        freed = true
        SSL_free(ssl)
        SSL_CTX_free(ctx)
    }
}
