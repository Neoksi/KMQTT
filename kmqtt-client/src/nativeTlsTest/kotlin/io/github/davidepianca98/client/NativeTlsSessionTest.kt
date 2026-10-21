package io.github.davidepianca98.client

import io.github.davidepianca98.client.NativeTlsTestCerts.CA
import io.github.davidepianca98.client.NativeTlsTestCerts.CLIENT
import io.github.davidepianca98.client.NativeTlsTestCerts.CLIENT_KEY_ENCRYPTED
import io.github.davidepianca98.client.NativeTlsTestCerts.CLIENT_KEY_PASSWORD
import io.github.davidepianca98.client.NativeTlsTestCerts.EXPIRED
import io.github.davidepianca98.client.NativeTlsTestCerts.EXPIRED_KEY
import io.github.davidepianca98.client.NativeTlsTestCerts.OTHER
import io.github.davidepianca98.client.NativeTlsTestCerts.SELF
import io.github.davidepianca98.client.NativeTlsTestCerts.SELF_EXPIRED
import io.github.davidepianca98.client.NativeTlsTestCerts.SELF_EXPIRED_KEY
import io.github.davidepianca98.client.NativeTlsTestCerts.SELF_KEY
import io.github.davidepianca98.client.NativeTlsTestCerts.SERVER
import io.github.davidepianca98.client.NativeTlsTestCerts.SERVER_KEY
import io.github.davidepianca98.socket.tls.TLSClientSettings
import openssl.TLS1_1_VERSION
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Сессия OpenSSL нового клиента против сервера OpenSSL в памяти: доверие, имя хоста, сроки, SNI, ключ
 * клиента, версии — как `TlsTransportTest` JVM. Без сети.
 */
class NativeTlsSessionTest {

    /** Прогнать рукопожатие; `null` — удалось, иначе вид отказа клиента. */
    private fun handshake(client: NativeTlsSession, server: NativeTlsTestServer): String? {
        try {
            repeat(50) {
                val done = client.handshake()
                client.takeOutput()?.let { server.feed(it) }
                val back = server.takeOutput()
                if (back.isNotEmpty()) client.feed(back)
                if (done && server.handshakeDone) return null
                if (server.handshakeFailed) error("server handshake failed")
            }
            error("handshake did not finish")
        } catch (e: NativeTlsException) {
            return e.kind
        }
    }

    private fun connect(
        settings: TLSClientSettings,
        host: String = "localhost",
        verifyHostname: Boolean = true,
        server: NativeTlsTestServer
    ): Pair<NativeTlsSession, String?> {
        val client = openSslSessionFactory(settings, verifyHostname).open(host)
        return client to handshake(client, server)
    }

    private fun kind(
        settings: TLSClientSettings,
        cert: String,
        key: String,
        host: String = "localhost",
        verifyHostname: Boolean = true
    ): String? {
        val server = NativeTlsTestServer(cert, key)
        val (client, kind) = connect(settings, host, verifyHostname, server)
        client.free()
        server.free()
        return kind
    }

    @Test
    fun pinnedSelfSignedHandshakeExchangeAndCloseNotify() {
        val server = NativeTlsTestServer(SELF, SELF_KEY)
        val (client, kind) = connect(TLSClientSettings(serverCertificate = SELF), server = server)
        assertNull(kind, "рукопожатие с закреплённым самоподписанным")
        assertTrue(client.isHandshakeDone)
        assertEquals("localhost", server.serverName, "SNI — имя хоста")

        val payload = ByteArray(100_000) { (it % 251).toByte() }
        var offset = 0
        while (offset < payload.size) {
            val n = client.writePlain(payload, offset, minOf(16 * 1024, payload.size - offset))
            assertTrue(n > 0)
            offset += n
            client.takeOutput()?.let { server.feed(it) }
        }
        assertContentEquals(payload, server.received.toByteArray(), "сервер получил всё по порядку")

        server.send("hello".encodeToByteArray())
        client.feed(server.takeOutput())
        val buffer = ByteArray(16 * 1024)
        val n = client.readPlain(buffer)
        assertEquals("hello", buffer.copyOf(n).decodeToString())
        assertEquals(0, client.readPlain(buffer), "больше нечего")

        server.shutdown()
        client.feed(server.takeOutput())
        assertEquals(NativeTlsSession.READ_CLOSED, client.readPlain(buffer), "close_notify сервера")

        client.shutdown()
        client.takeOutput()?.let { server.feed(it) }
        assertTrue(server.closeNotifyReceived, "close_notify клиента")
        client.free()
        client.free() // повтор безвреден
        server.free()
    }

    @Test
    fun pinnedCaAcceptsItsLeafByNameAndByIp() {
        assertNull(kind(TLSClientSettings(serverCertificate = CA), SERVER, SERVER_KEY, "localhost"))
        assertNull(kind(TLSClientSettings(serverCertificate = CA), SERVER, SERVER_KEY, "127.0.0.1"), "IP SAN")
    }

    @Test
    fun pinnedLeafUnderCaIsAnAnchor() {
        // Как якорь PKIX на JVM: закреплён сам лист, УЦ не известен (X509_V_FLAG_PARTIAL_CHAIN)
        assertNull(kind(TLSClientSettings(serverCertificate = SERVER), SERVER, SERVER_KEY))
    }

    @Test
    fun hostnameMismatchIsSslHostname() {
        val pin = TLSClientSettings(serverCertificate = CA)
        assertEquals("ssl-hostname", kind(pin, SERVER, SERVER_KEY, "other.example"))
        assertEquals("ssl-hostname", kind(pin, SERVER, SERVER_KEY, "10.0.0.1"), "IP не из SAN")
        assertEquals("ssl-hostname", kind(pin, SERVER, SERVER_KEY, "[::1]"), "IPv6 не из SAN")
        // SAN самоподписанного — только DNS localhost: по IP имя не совпадает
        assertEquals("ssl-hostname", kind(TLSClientSettings(serverCertificate = SELF), SELF, SELF_KEY, "127.0.0.1"))
        // Имя с лишним суффиксом не совпадает
        assertEquals("ssl-hostname", kind(pin, SERVER, SERVER_KEY, "localhost.evil"))
    }

    @Test
    fun hostnameCheckCanBeDisabledExplicitly() {
        assertNull(kind(TLSClientSettings(serverCertificate = CA), SERVER, SERVER_KEY, "other.example", verifyHostname = false))
    }

    @Test
    fun explicitSniIsSentAndChecked() {
        val server = NativeTlsTestServer(SERVER, SERVER_KEY)
        val (client, kind) = connect(
            TLSClientSettings(serverCertificate = CA, serverNameIndications = "localhost"),
            host = "10.0.0.1",
            server = server
        )
        assertNull(kind, "имя проверяется по явному SNI")
        assertEquals("localhost", server.serverName)
        client.free()
        server.free()

        val byIp = NativeTlsTestServer(SERVER, SERVER_KEY)
        val (ipClient, ipKind) = connect(TLSClientSettings(serverCertificate = CA), host = "127.0.0.1", server = byIp)
        assertNull(ipKind)
        assertNull(byIp.serverName, "для IP-литерала SNI не шлётся")
        ipClient.free()
        byIp.free()
    }

    @Test
    fun untrustedAndWrongPinAreSslTrust() {
        assertEquals("ssl-trust", kind(TLSClientSettings(), SELF, SELF_KEY), "без закрепления — системные корни")
        assertEquals("ssl-trust", kind(TLSClientSettings(serverCertificate = OTHER), SELF, SELF_KEY), "не тот закреплённый")
        assertEquals("ssl-trust", kind(TLSClientSettings(serverCertificate = OTHER), SERVER, SERVER_KEY))
    }

    @Test
    fun expiredLeafUnderPinnedCaIsSslExpired() {
        assertEquals("ssl-expired", kind(TLSClientSettings(serverCertificate = CA), EXPIRED, EXPIRED_KEY))
    }

    @Test
    fun expiredCertificateWithoutTheCaPinnedIsUntrusted() {
        assertEquals("ssl-trust", kind(TLSClientSettings(serverCertificate = OTHER), EXPIRED, EXPIRED_KEY))
    }

    @Test
    fun pinnedExpiredCertificateIsAccepted() {
        // срок якоря доверия не проверяется
        assertNull(kind(TLSClientSettings(serverCertificate = SELF_EXPIRED), SELF_EXPIRED, SELF_EXPIRED_KEY))
    }

    @Test
    fun trustAllConnectsByIpWithoutChecks() {
        assertNull(
            kind(TLSClientSettings(checkServerCertificate = false), EXPIRED, EXPIRED_KEY, "10.0.0.1", verifyHostname = false)
        )
    }

    @Test
    fun tlsBelow12IsRejected() {
        val server = NativeTlsTestServer(SELF, SELF_KEY, maxVersion = TLS1_1_VERSION)
        val client = openSslSessionFactory(TLSClientSettings(serverCertificate = SELF), true).open("localhost")
        val kind = handshakeKind(client, server)
        assertEquals("ssl", kind, "TLS 1.1 отклонён")
        client.free()
        server.free()
    }

    private fun handshakeKind(client: NativeTlsSession, server: NativeTlsTestServer): String? {
        try {
            repeat(50) {
                if (client.handshake()) return null
                client.takeOutput()?.let { server.feed(it) }
                val back = server.takeOutput()
                if (back.isNotEmpty()) client.feed(back) else if (server.handshakeFailed) client.feed(ByteArray(0))
            }
        } catch (e: NativeTlsException) {
            return e.kind
        }
        return "no-result"
    }

    @Test
    fun invalidSettingsAreSslConfigWithoutDetails() {
        val badPem = "-----BEGIN CERTIFICATE-----\nnot base64 !!\n-----END CERTIFICATE-----"
        val e = assertFailsWith<NativeTlsException> {
            openSslSessionFactory(TLSClientSettings(serverCertificate = badPem), true).open("localhost")
        }
        assertEquals("ssl-config", e.kind)
        assertFalse("not base64" in (e.message ?: ""), "текст без содержимого настроек")

        val missing = "/nonexistent/kmqtt/secret-path.pem"
        val f = assertFailsWith<NativeTlsException> {
            openSslSessionFactory(TLSClientSettings(serverCertificate = missing), true).open("localhost")
        }
        assertEquals("ssl-config", f.kind)
        assertFalse("secret-path" in (f.message ?: ""), "текст без пути")

        val noKey = assertFailsWith<NativeTlsException> {
            openSslSessionFactory(TLSClientSettings(serverCertificate = CA, clientCertificate = CLIENT), true).open("localhost")
        }
        assertEquals("ssl-config", noKey.kind)
    }

    @Test
    fun clientCertificateWithEncryptedKey() {
        val settings = TLSClientSettings(
            serverCertificate = CA,
            clientCertificate = CLIENT,
            clientCertificateKey = CLIENT_KEY_ENCRYPTED,
            clientCertificatePassword = CLIENT_KEY_PASSWORD
        )
        val server = NativeTlsTestServer(SERVER, SERVER_KEY, clientCa = CA)
        val (client, kind) = connect(settings, server = server)
        assertNull(kind, "рукопожатие с сертификатом клиента")
        assertTrue(server.clientCertificateVerified, "сервер проверил сертификат клиента")
        client.free()
        server.free()

        val wrong = assertFailsWith<NativeTlsException> {
            openSslSessionFactory(settings.copy(clientCertificatePassword = "wrong"), true).open("localhost")
        }
        assertEquals("ssl-config", wrong.kind, "неверный пароль ключа")
    }

    @Test
    fun handshakeKindByVerifyCode() {
        assertEquals("ssl", OpenSslSession.handshakeKind(0))
        assertEquals("ssl-expired", OpenSslSession.handshakeKind(10))
        assertEquals("ssl-hostname", OpenSslSession.handshakeKind(62))
        assertEquals("ssl-trust", OpenSslSession.handshakeKind(20))
        assertEquals("ssl", OpenSslSession.handshakeKind(50))
    }

    @Test
    fun systemRootsAreLoaded() {
        val count = OpenSslSession.systemRootCount()
        println("NativeTlsSessionTest: системных корней загружено: $count")
        assertTrue(count > 0, "системное хранилище не пустое (Windows ROOT / связка дистрибутива)")
    }

    @Test
    fun ipLiterals() {
        assertEquals("127.0.0.1", ipLiteralOrNull("127.0.0.1"))
        assertEquals("::1", ipLiteralOrNull("[::1]"))
        assertEquals("fe80::1", ipLiteralOrNull("fe80::1"))
        assertNull(ipLiteralOrNull("localhost"))
        assertNull(ipLiteralOrNull("1.2.3"))
        assertNull(ipLiteralOrNull("256.1.1.1"))
        assertNull(ipLiteralOrNull("1.2.3.4.example"))
    }
}
