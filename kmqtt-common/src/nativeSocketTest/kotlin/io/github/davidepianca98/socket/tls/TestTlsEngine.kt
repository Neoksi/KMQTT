package io.github.davidepianca98.socket.tls

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.convert
import openssl.*

/**
 * Тестовый TLS-движок на OpenSSL с memory-BIO (как `TLSClientEngine` клиента и `TLSServerEngine` брокера, которые
 * internal в своих модулях). Сервер — с самоподписанным сертификатом [TEST_CERT]/[TEST_KEY], клиент —
 * без проверки сертификата. [maxTls12] ограничивает сервер TLS 1.2: в нём клиент после своего `Finished` ждёт
 * `Finished` сервера, а сервер к этому моменту уже может слать прикладные данные — так `SSL_write` клиента получает
 * `WANT_READ`, а в том же `recv` приходят прикладные записи.
 */
internal class TestTlsEngine(server: Boolean, maxTls12: Boolean = false) : TLSEngine {

    private val sslContext: CPointer<SSL_CTX>
    private val ssl: CPointer<SSL>
    private val readBio: CPointer<BIO>
    private val writeBio: CPointer<BIO>
    private var freed = false

    init {
        OPENSSL_init_ssl(0u, null)
        sslContext = SSL_CTX_new(if (server) TLS_server_method() else TLS_client_method())
            ?: error("SSL_CTX_new")
        if (server) {
            val certBio = BIO_new(BIO_s_mem())
            BIO_puts(certBio, TEST_CERT)
            val cert = PEM_read_bio_X509(certBio, null, null, null) ?: error("PEM_read_bio_X509")
            BIO_free(certBio)
            val keyBio = BIO_new(BIO_s_mem())
            BIO_puts(keyBio, TEST_KEY)
            val key = PEM_read_bio_PrivateKey(keyBio, null, null, null) ?: error("PEM_read_bio_PrivateKey")
            BIO_free(keyBio)
            check(SSL_CTX_use_certificate(sslContext, cert) == 1) { "SSL_CTX_use_certificate" }
            check(SSL_CTX_use_PrivateKey(sslContext, key) == 1) { "SSL_CTX_use_PrivateKey" }
            X509_free(cert)
            EVP_PKEY_free(key)
        }
        SSL_CTX_set_verify(sslContext, SSL_VERIFY_NONE, null)
        if (maxTls12) {
            // SSL_CTX_set_max_proto_version — макрос; long у C на mingwX64 32-битный, на Linux 64-битный
            val rc = SSL_CTX_ctrl(sslContext, SSL_CTRL_SET_MAX_PROTO_VERSION, TLS1_2_VERSION.convert(), null)
            check(rc.toLong() == 1L) { "SSL_CTX_set_max_proto_version" }
        }
        ssl = SSL_new(sslContext) ?: error("SSL_new")
        readBio = BIO_new(BIO_s_mem()) ?: error("BIO_new")
        writeBio = BIO_new(BIO_s_mem()) ?: error("BIO_new")
        if (server) SSL_set_accept_state(ssl) else SSL_set_connect_state(ssl)
        SSL_set_bio(ssl, readBio, writeBio)
    }

    override val isInitFinished: Boolean
        get() = SSL_is_init_finished(ssl) != 0

    override val bioShouldRetry: Boolean
        get() = BIO_test_flags(writeBio, BIO_FLAGS_SHOULD_RETRY) == 0

    override fun write(buffer: CPointer<ByteVar>, length: Int): Int = SSL_write(ssl, buffer, length)

    override fun read(buffer: CPointer<ByteVar>, length: Int): Int = SSL_read(ssl, buffer, length)

    override fun bioRead(buffer: CPointer<ByteVar>, length: Int): Int = BIO_read(writeBio, buffer, length)

    override fun bioWrite(buffer: CPointer<ByteVar>, length: Int): Int = BIO_write(readBio, buffer, length)

    override fun getError(result: Int): Int = SSL_get_error(ssl, result)

    override fun close() {
        if (!freed) {
            freed = true
            SSL_free(ssl)
            SSL_CTX_free(sslContext)
        }
    }

    companion object {
        // Самоподписанный тестовый сертификат (EC P-256, CN=localhost, до 2126 г.) — только для тестов
        const val TEST_KEY: String = """-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgU/7/0dK0fKpx/NDZ
RQOgyMA6IdI8ZgCThMupw0NF7MGhRANCAARpKcUWWDi0LVFMVHGOaC2MhX3y4ODh
z6qzUyT8wjtdiFbxISzWbAadaFtyd27KtSTiJYdd+npJXIPEsZiO6Bxe
-----END PRIVATE KEY-----
"""
        const val TEST_CERT: String = """-----BEGIN CERTIFICATE-----
MIIBgDCCASWgAwIBAgIUNiik4cYVd5XUSDHrOKKHjUtnIxwwCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MTAxODIyMzgyN1oYDzIxMjYwOTI0
MjIzODI3WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIBBggqhkjO
PQMBBwNCAARpKcUWWDi0LVFMVHGOaC2MhX3y4ODhz6qzUyT8wjtdiFbxISzWbAad
aFtyd27KtSTiJYdd+npJXIPEsZiO6Bxeo1MwUTAdBgNVHQ4EFgQUrKruIds0vv4D
N2RBZ1wEJDqcIAowHwYDVR0jBBgwFoAUrKruIds0vv4DN2RBZ1wEJDqcIAowDwYD
VR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNJADBGAiEA5H6gqrpewuy1rJI6u1/P
alpY/unsgz7hYtzQqiHZpKUCIQDRjP/lhZiyVYgMmwvbiKk3DHEtJq0Iu5jktzSc
+p/eJQ==
-----END CERTIFICATE-----
"""
    }
}

/**
 * Наблюдатель за клиентским движком: считает прикладные данные, расшифрованные после `WANT_READ` у `SSL_write` и до
 * следующей записи, то есть ровно ту ветку `TLSSocket.send0`, в которой раньше терялись данные. Тест
 * проверяет, что ветка действительно пройдена, иначе он ничего не доказывает.
 */
internal class ObservedTlsEngine(private val delegate: TLSEngine) : TLSEngine by delegate {

    private var lastWasWrite = false
    private var writeWantsRead = false

    /** Сколько раз `SSL_read` отдал прикладные данные внутри `WANT_READ` записи. */
    var appDataReadDuringWrite: Int = 0
        private set

    /** Сколько раз `SSL_write` вернул `WANT_READ`. */
    var writeWantRead: Int = 0
        private set

    override fun write(buffer: CPointer<ByteVar>, length: Int): Int {
        writeWantsRead = false
        lastWasWrite = true
        return delegate.write(buffer, length)
    }

    override fun read(buffer: CPointer<ByteVar>, length: Int): Int {
        lastWasWrite = false
        val n = delegate.read(buffer, length)
        if (n > 0 && writeWantsRead) appDataReadDuringWrite++
        return n
    }

    override fun getError(result: Int): Int {
        val error = delegate.getError(result)
        if (lastWasWrite && error == SSL_ERROR_WANT_READ) {
            writeWantsRead = true
            writeWantRead++
        }
        return error
    }
}
