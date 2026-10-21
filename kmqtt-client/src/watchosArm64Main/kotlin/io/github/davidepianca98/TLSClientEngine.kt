package io.github.davidepianca98

import kotlinx.cinterop.*
import openssl.*
import platform.posix.getenv
import platform.posix.strcpy
import platform.posix.strlen
import io.github.davidepianca98.socket.IOException
import io.github.davidepianca98.socket.tls.TLSClientSettings
import io.github.davidepianca98.socket.tls.TLSEngine

/**
 * TLS-движок старого блокирующего клиента на OpenSSL (memory-BIO). SNI передаётся завершённой
 * нулём C-строкой; имя хоста сертификата проверяется (`SSL_set1_host` / `X509_VERIFY_PARAM_set1_ip_asc`),
 * когда включена проверка сертификата; `SSL_CTX` отпускается сразу после `SSL_new` (ссылка переходит к SSL);
 * пароль ключа живёт в native-копии всю сессию и затирается в [close]; ALPN/SNI — во всех вариантах,
 * включая iOS.
 */
internal actual class TLSClientEngine actual constructor(tlsSettings: TLSClientSettings, host: String) : TLSEngine {

    private val context: CPointer<SSL>
    private val readBio: CPointer<BIO>
    private val writeBio: CPointer<BIO>

    /** Native-копия пароля ключа (NUL в конце): userdata обработчика валиден, пока жива сессия. */
    private val passwordBuffer: CPointer<ByteVar>? =
        tlsSettings.clientCertificatePassword?.let { password ->
            val bytes = password.encodeToByteArray()
            nativeHeap.allocArray<ByteVar>(bytes.size + 1).also { buffer ->
                for (i in bytes.indices) buffer[i] = bytes[i]
                buffer[bytes.size] = 0
                bytes.fill(0)
            }
        }

    private val passwordLength: Int = tlsSettings.clientCertificatePassword?.encodeToByteArray()?.size ?: 0

    fun loadServerCertsFromString(context: CPointer<SSL_CTX>, certBuffer: String): Boolean {
        val bio = BIO_new(BIO_s_mem())
        BIO_puts(bio, certBuffer)

        val inf = PEM_X509_INFO_read_bio(bio, null, null, null)

        if (inf == null) {
            BIO_free(bio)
            return false
        }

        val ctx = SSL_CTX_get_cert_store(context);

        var loaded = 0
        for (i in 0..<sk_X509_INFO_num(inf)) {
            val itmp = sk_X509_INFO_value(inf, i)
            if (itmp != null && itmp.pointed.x509 != null) {
                if (X509_STORE_add_cert(ctx, itmp.pointed.x509) != 1) {
                    sk_X509_INFO_pop_free(inf, staticCFunction { buf: CPointer<X509_INFO>? ->
                        X509_INFO_free(buf)
                    })
                    BIO_free(bio)
                    return false
                }
                loaded++
            }
        }

        sk_X509_INFO_pop_free(inf, staticCFunction { buf: CPointer<X509_INFO>? ->
            X509_INFO_free(buf)
        })
        BIO_free(bio)

        return loaded > 0
    }

    init {
        val readBio = BIO_new(BIO_s_mem()) ?: throw IOException("Failed allocating read BIO")

        val writeBio = BIO_new(BIO_s_mem())
        if (writeBio == null) {
            BIO_free(readBio)
            throw IOException("Failed allocating read BIO")
        }

        val method = TLS_client_method()
        val sslContext = SSL_CTX_new(method)!!
        var clientContext: CPointer<SSL>? = null
        try {
            if (tlsSettings.serverCertificate != null) {
                if (!loadServerCertsFromString(sslContext, tlsSettings.serverCertificate!!)) {
                    // Try file
                    if (SSL_CTX_load_verify_locations(sslContext, tlsSettings.serverCertificate, null) != 1) {
                        throw Exception("Server certificate path not found")
                    }
                }
            } else {
                if (!tlsSettings.checkServerCertificate) {
                    SSL_CTX_set_cert_verify_callback(
                        sslContext,
                        staticCFunction { buf: CPointer<X509_STORE_CTX>?, arg: COpaquePointer? ->
                            1
                        },
                        null
                    )
                } else {
                    if (SSL_CTX_load_verify_locations(sslContext, null, getenv(X509_get_default_cert_dir_env()?.toKString())?.toKString()) != 1) {
                        throw Exception("Server certificate path not found")
                    }
                }
            }

            // Пароль — до чтения ключа: чтение ключа из памяти и из файла берут его из userdata контекста
            passwordBuffer?.let {
                SSL_CTX_set_default_passwd_cb_userdata(sslContext, it)
                SSL_CTX_set_default_passwd_cb(
                    sslContext,
                    staticCFunction { buf: CPointer<ByteVar>?, size: Int, _: Int, password: COpaquePointer? ->
                        if (password == null) {
                            0
                        } else {
                            val len = strlen(password.reinterpret<ByteVar>().toKString())
                            if (size < len.toInt() + 1) {
                                0
                            } else {
                                strcpy(buf, password.reinterpret<ByteVar>().toKString())
                                len.toInt()
                            }
                        }
                    })
            }

            if (tlsSettings.clientCertificate != null) {
                val bio = BIO_new(BIO_s_mem())
                BIO_puts(bio, tlsSettings.clientCertificate)
                val certificate = PEM_read_bio_X509(bio, null, null, null)

                if (certificate != null) {
                    if (SSL_CTX_use_certificate(sslContext, certificate) != 1) {
                        throw Exception("Cannot load client's certificate")
                    }
                } else {
                    // Load file
                    if (SSL_CTX_use_certificate_file(sslContext, tlsSettings.clientCertificate, SSL_FILETYPE_PEM) != 1) {
                        throw Exception("Cannot load client's certificate file")
                    }
                }

                BIO_puts(bio, tlsSettings.clientCertificateKey)
                val key = PEM_read_bio_PrivateKey(bio, null, null, null)
                if (key != null) {
                    if (SSL_CTX_use_PrivateKey(sslContext, key) != 1) {
                        throw Exception("Cannot load client's key")
                    }
                } else {
                    if (SSL_CTX_use_PrivateKey_file(sslContext, tlsSettings.clientCertificateKey!!, SSL_FILETYPE_PEM) != 1) {
                        throw Exception("Cannot load client's key file")
                    }
                }
                if (SSL_CTX_check_private_key(sslContext) != 1) {
                    throw Exception("Client's certificate and key don't match")
                }
            }

            tlsSettings.alpnProtocols?.let {
                val protoBytes = it.flatMap { proto ->
                    val bytes = proto.encodeToByteArray()
                    listOf(bytes.size.toByte()) + bytes.toList()
                }.toByteArray()

                protoBytes.usePinned { pinned ->
                    val result = SSL_CTX_set_alpn_protos(
                        sslContext,
                        pinned.addressOf(0).reinterpret(),
                        protoBytes.size.toUInt()
                    )
                    if (result != 0) {
                        throw Exception("Failed to set ALPN protocols")
                    }
                }
            }

            val ssl = SSL_new(sslContext)
            if (ssl == null) {
                throw IOException("Failed allocating SSL")
            }
            SSL_CTX_free(sslContext) // ссылка на контекст перешла к SSL, дальше освобождается вместе с ним
            clientContext = ssl

            tlsSettings.serverNameIndications?.let { sni ->
                // cstr: строка с завершающим NUL, SNI обязан быть C-строкой
                val result = memScoped {
                    SSL_ctrl(
                        ssl,
                        SSL_CTRL_SET_TLSEXT_HOSTNAME,
                        TLSEXT_NAMETYPE_host_name,
                        sni.cstr.ptr
                    )
                }

                if (result != 1) {
                    throw Exception("Failed to set SNI hostname")
                }
            }

            // Проверка имени хоста сертификата, когда проверка сертификата включена
            if (tlsSettings.checkServerCertificate) {
                val name = tlsSettings.serverNameIndications ?: host
                if (isIpLiteral(name)) {
                    if (X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl), name) != 1) {
                        throw Exception("Failed to set expected host IP")
                    }
                } else {
                    SSL_set_hostflags(ssl, NO_PARTIAL_WILDCARDS)
                    if (SSL_set1_host(ssl, name) != 1) {
                        throw Exception("Failed to set expected host name")
                    }
                }
            }

            SSL_set_verify(ssl, SSL_VERIFY_PEER, null)

            SSL_set_connect_state(ssl)
            SSL_set_bio(ssl, readBio, writeBio)
            context = ssl
            this.readBio = readBio
            this.writeBio = writeBio
        } catch (e: Throwable) {
            if (clientContext == null) {
                SSL_CTX_free(sslContext)
            } else {
                SSL_free(clientContext)
            }
            BIO_free(readBio)
            BIO_free(writeBio)
            passwordBuffer?.let { OPENSSL_cleanse(it, (passwordLength + 1).convert()) }
            throw e
        }
    }

    actual override val isInitFinished: Boolean
        get() = SSL_is_init_finished(context) != 0

    actual override val bioShouldRetry: Boolean
        get() = BIO_test_flags(writeBio, BIO_FLAGS_SHOULD_RETRY) == 0

    actual override fun write(buffer: CPointer<ByteVar>, length: Int): Int {
        return SSL_write(context, buffer, length)
    }

    actual override fun read(buffer: CPointer<ByteVar>, length: Int): Int {
        return SSL_read(context, buffer, length)
    }

    actual override fun bioRead(buffer: CPointer<ByteVar>, length: Int): Int {
        return BIO_read(writeBio, buffer, length)
    }

    actual override fun bioWrite(buffer: CPointer<ByteVar>, length: Int): Int {
        return BIO_write(readBio, buffer, length)
    }

    actual override fun getError(result: Int): Int {
        return SSL_get_error(context, result)
    }

    actual override fun close() {
        SSL_free(context)
        passwordBuffer?.let {
            OPENSSL_cleanse(it, (passwordLength + 1).convert())
            nativeHeap.free(it.rawValue)
        }
    }
}

/** `X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS` (x509v3.h, в klib нет). */
private const val NO_PARTIAL_WILDCARDS: UInt = 0x4u

/** IP-литерал (IPv4 из четырёх числовых частей или IPv6 с ':') — сверяется через set1_ip_asc, не set1_host. */
private fun isIpLiteral(value: String): Boolean {
    if (value.any { it == ':' }) return true
    val parts = value.split('.')
    return parts.size == 4 && parts.all { it.isNotEmpty() && it.all(Char::isDigit) }
}
