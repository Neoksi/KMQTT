package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.socket.tls.TLSClientSettings
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.convert
import kotlinx.cinterop.toKString
import kotlinx.cinterop.usePinned
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import platform.posix.fclose
import platform.posix.fopen
import platform.posix.fread
import platform.posix.getenv
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.time.Duration.Companion.seconds

/**
 * Фасад `MqttClient(options)` с TLS против тестового TLS-брокера (mosquitto + OpenSSL).
 * Без переменных окружения тесты пропускаются (печатают строку SKIP):
 * - `KMQTT_TLS_A` — `хост:порт` листенера A с сертификатом `selfsigned-no-ip` (SAN `localhost`, `kmqtt-testbed.local`),
 *   хост — имя из SAN;
 * - `KMQTT_TLS_A_BADHOST` — другой адрес того же листенера, которого нет в SAN (например `127.0.0.1`);
 * - `KMQTT_TLS_B` — `хост:порт` листенера B с `server-by-ca` (SAN — IP из `make-certs.sh`, `127.0.0.1`, `localhost`);
 * - `KMQTT_TLS_CERTS` — каталог `tls/out` (публичные сертификаты: `selfsigned-no-ip/cert.pem`, `ca/ca.crt`,
 *   `server-by-ca/cert.pem`, `rotation/cert.pem`).
 * Порядок запуска — в `Readme.ru.md`.
 */
class NativeTlsTestbedTest {

    private class Endpoint(val host: String, val port: Int)

    private fun env(name: String): String? = getenv(name)?.toKString()?.takeIf { it.isNotBlank() }

    private fun endpoint(name: String): Endpoint? {
        val value = env(name) ?: return null
        val colon = value.lastIndexOf(':')
        return Endpoint(value.substring(0, colon), value.substring(colon + 1).toInt())
    }

    private val a = endpoint("KMQTT_TLS_A")
    private val aBadHost = env("KMQTT_TLS_A_BADHOST")
    private val b = endpoint("KMQTT_TLS_B")
    private val certs = env("KMQTT_TLS_CERTS")

    private fun available(test: String): Boolean {
        val ok = a != null && aBadHost != null && b != null && certs != null
        if (!ok) println("SKIP NativeTlsTestbedTest.$test: нет KMQTT_TLS_A/KMQTT_TLS_A_BADHOST/KMQTT_TLS_B/KMQTT_TLS_CERTS")
        return ok
    }

    private fun cert(relative: String): String = "$certs/$relative"

    private fun readText(path: String): String {
        val file = fopen(path, "rb") ?: error("нет файла $path")
        try {
            val out = ArrayList<Byte>()
            val buffer = ByteArray(4096)
            while (true) {
                val n = buffer.usePinned { fread(it.addressOf(0), 1.convert(), buffer.size.convert(), file) }.toInt()
                if (n <= 0) break
                for (i in 0 until n) out += buffer[i]
            }
            return out.toByteArray().decodeToString()
        } finally {
            fclose(file)
        }
    }

    private fun options(endpoint: Endpoint, host: String, settings: TLSClientSettings, verifyHostname: Boolean = true) =
        MqttClientOptions(
            host = host,
            port = endpoint.port,
            tls = settings,
            verifyHostname = verifyHostname,
            reconnect = ReconnectPolicy.Never,
            connectTimeout = 10.seconds,
            connackTimeout = 10.seconds
        )

    private fun timed(block: suspend CoroutineScope.() -> Unit) = runBlocking { withTimeout(60_000, block) }

    private suspend fun connectError(options: MqttClientOptions): MqttError {
        val client = MqttClient(options)
        try {
            val result = client.connect()
            assertIs<ConnectResult.Failed>(result, "подключение должно провалиться: $result")
            return result.error
        } finally {
            client.close()
        }
    }

    @Test
    fun publishAndReceiveOverTlsWithPinnedSelfSigned() = timed {
        if (!available("publishAndReceiveOverTlsWithPinnedSelfSigned")) return@timed
        val endpoint = a!!
        // Закреплённый — путём к файлу (OpenSSL читает файл сам)
        val client = MqttClient(options(endpoint, endpoint.host, TLSClientSettings(serverCertificate = cert("selfsigned-no-ip/cert.pem"))))
        try {
            val connected = client.connect()
            assertIs<ConnectResult.Connected>(connected, "подключение по TLS: $connected")
            client.subscriptions.set("g", setOf("kmqtt/native-tls/#"), Qos.AT_LEAST_ONCE)
            assertEquals(
                mapOf<String, TopicState>("kmqtt/native-tls/#" to TopicState.Granted(Qos.AT_LEAST_ONCE)),
                client.subscriptions.awaitApplied("g")
            )
            val message = async(Dispatchers.IO) {
                client.incoming.filterIsInstance<MqttIncoming.Message>().first { it.topic == "kmqtt/native-tls/x" }
            }
            val payload = ByteArray(200_000) { (it % 253).toByte() }
            assertEquals(
                PublishResult.Delivered(connected.epoch),
                client.publish("kmqtt/native-tls/x", payload, Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
            )
            val received = message.await()
            assertEquals(payload.toList(), received.payload.toList(), "сообщение вернулось через брокер по TLS")
        } finally {
            withContext(Dispatchers.IO) { client.close() }
        }
        assertEquals(ConnectionState.Closed(null), client.connection.value)
    }

    @Test
    fun pinnedCaAndPinnedLeafAcceptTheLeafSignedByCa() = timed {
        if (!available("pinnedCaAndPinnedLeafAcceptTheLeafSignedByCa")) return@timed
        val endpoint = b!!
        for (pin in listOf(readText(cert("ca/ca.crt")), readText(cert("server-by-ca/cert.pem")))) {
            val client = MqttClient(options(endpoint, endpoint.host, TLSClientSettings(serverCertificate = pin)))
            try {
                assertIs<ConnectResult.Connected>(client.connect(), "закреплён УЦ или лист под ним (PEM-строкой)")
            } finally {
                client.close()
            }
        }
    }

    @Test
    fun hostnameMismatchIsSslHostnameWithoutAddress() = timed {
        if (!available("hostnameMismatchIsSslHostnameWithoutAddress")) return@timed
        val error = connectError(options(a!!, aBadHost!!, TLSClientSettings(serverCertificate = cert("selfsigned-no-ip/cert.pem"))))
        assertIs<MqttError.Io>(error)
        assertEquals("ssl-hostname", error.kind)
        assertFalse(aBadHost in error.toString() || aBadHost in (error.cause?.message ?: ""), "ошибка без адреса: $error")
    }

    @Test
    fun hostnameCheckOffConnectsByOtherAddress() = timed {
        if (!available("hostnameCheckOffConnectsByOtherAddress")) return@timed
        val client = MqttClient(
            options(a!!, aBadHost!!, TLSClientSettings(serverCertificate = cert("selfsigned-no-ip/cert.pem")), verifyHostname = false)
        )
        try {
            assertIs<ConnectResult.Connected>(client.connect())
        } finally {
            client.close()
        }
    }

    @Test
    fun untrustedAndWrongPinAreSslTrust() = timed {
        if (!available("untrustedAndWrongPinAreSslTrust")) return@timed
        val endpoint = a!!
        val noPin = connectError(options(endpoint, endpoint.host, TLSClientSettings()))
        assertEquals("ssl-trust", (noPin as MqttError.Io).kind, "без закрепления — системные корни: $noPin")
        val wrongPin = connectError(options(endpoint, endpoint.host, TLSClientSettings(serverCertificate = cert("rotation/cert.pem"))))
        assertEquals("ssl-trust", (wrongPin as MqttError.Io).kind, "не тот закреплённый: $wrongPin")
    }
}
