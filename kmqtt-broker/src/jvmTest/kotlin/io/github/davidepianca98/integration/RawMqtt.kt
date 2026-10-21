package io.github.davidepianca98.integration

import io.github.davidepianca98.mqtt.broker.Broker
import io.github.davidepianca98.mqtt.broker.interfaces.Authentication
import io.github.davidepianca98.socket.tls.TLSSettings
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.File
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.security.cert.X509Certificate
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager
import kotlin.concurrent.thread
import kotlin.test.assertEquals

// обвязка тестов брокера с настройками по умолчанию против «сырых» TCP/TLS-клиентов. Пакеты MQTT
// собираются вручную, чтобы тесты не зависели от клиентов форка.

internal const val RAW_READ_TIMEOUT_MS = 10_000

internal class RawPacket(val firstByte: Int, val body: ByteArray) {
    val type get() = firstByte shr 4
    val retain get() = firstByte and 0x01 == 1
}

internal class RawClient(
    port: Int,
    private val version: Int,
    clientId: String,
    tls: Boolean = false,
    keepAliveSeconds: Int = 60
) : AutoCloseable {
    private val socket: Socket = if (tls) {
        val context = SSLContext.getInstance("TLS")
        // тестовый ключ брокера самоподписан, проверка цепочки здесь не предмет теста
        context.init(null, arrayOf<TrustManager>(object : X509TrustManager {
            override fun checkClientTrusted(chain: Array<X509Certificate>?, authType: String?) {}
            override fun checkServerTrusted(chain: Array<X509Certificate>?, authType: String?) {}
            override fun getAcceptedIssuers() = arrayOf<X509Certificate>()
        }), null)
        context.socketFactory.createSocket(InetAddress.getLoopbackAddress(), port)
    } else {
        Socket(InetAddress.getLoopbackAddress(), port)
    }.apply { soTimeout = RAW_READ_TIMEOUT_MS }
    private val input = DataInputStream(socket.getInputStream().buffered())
    private val output = socket.getOutputStream()

    init {
        val body = ByteArrayOutputStream()
        body.write(utf8("MQTT"))
        body.write(version)
        body.write(0x02) // clean session / clean start
        body.write(byteArrayOf((keepAliveSeconds shr 8).toByte(), keepAliveSeconds.toByte()))
        if (version == 5) body.write(0x00)
        body.write(utf8(clientId))
        write(0x10, body.toByteArray())
        assertEquals(2, readPacket().type, "CONNACK")
    }

    fun subscribe(filter: String, optionsByte: Int = 0) {
        val body = ByteArrayOutputStream()
        body.write(byteArrayOf(0x00, 0x01))
        if (version == 5) body.write(0x00)
        body.write(utf8(filter))
        body.write(optionsByte)
        write(0x82, body.toByteArray())
        assertEquals(9, readPacket().type, "SUBACK")
    }

    fun publishBytes(topic: String, payload: ByteArray, retain: Boolean): ByteArray {
        val body = ByteArrayOutputStream()
        body.write(utf8(topic))
        if (version == 5) body.write(0x00)
        body.write(payload)
        val out = ByteArrayOutputStream()
        out.write(0x30 or (if (retain) 1 else 0))
        out.write(remainingLength(body.size()))
        out.write(body.toByteArray())
        return out.toByteArray()
    }

    fun ping() = write(0xC0, ByteArray(0))

    fun publish(topic: String, payload: ByteArray, retain: Boolean) = writeRaw(publishBytes(topic, payload, retain))

    fun writeRaw(bytes: ByteArray) {
        output.write(bytes)
        output.flush()
    }

    private fun write(firstByte: Int, body: ByteArray) {
        output.write(firstByte)
        output.write(remainingLength(body.size))
        output.write(body)
        output.flush()
    }

    fun readPacket(): RawPacket {
        val first = input.readUnsignedByte()
        var length = 0
        var shift = 0
        while (true) {
            val b = input.readUnsignedByte()
            length = length or ((b and 0x7F) shl shift)
            if (b and 0x80 == 0) break
            shift += 7
        }
        val body = ByteArray(length)
        input.readFully(body)
        return RawPacket(first, body)
    }

    override fun close() = socket.close()

    private companion object {
        fun utf8(text: String): ByteArray {
            val bytes = text.toByteArray(Charsets.UTF_8)
            return byteArrayOf((bytes.size shr 8).toByte(), bytes.size.toByte()) + bytes
        }

        fun remainingLength(value: Int): ByteArray {
            val out = ByteArrayOutputStream()
            var rest = value
            do {
                var b = rest and 0x7F
                rest = rest shr 7
                if (rest > 0) b = b or 0x80
                out.write(b)
            } while (rest > 0)
            return out.toByteArray()
        }
    }
}

/** Брокер форка на loopback в своём потоке (`step()` и `stop()` вызываются из него же). */
internal class RawTestBroker(
    maximumPacketSize: UInt = 32768u,
    tls: Boolean = false,
    authentication: Authentication? = null,
    webSocket: Boolean = false
) : AutoCloseable {
    val port = ServerSocket(0).use { it.localPort }

    /** Порт WebSocket брокера или `null`, если он не включён. */
    val webSocketPort: Int? = if (webSocket) ServerSocket(0).use { it.localPort } else null

    @Volatile
    private var stopRequested = false
    private val worker: Thread

    init {
        val broker = Broker(
            port = port,
            host = "127.0.0.1",
            maximumPacketSize = maximumPacketSize,
            tlsSettings = if (tls) tlsSettings() else null,
            authentication = authentication,
            webSocketPort = webSocketPort
        )
        worker = thread(name = "raw-test-broker-$port", isDaemon = true) {
            while (!stopRequested) broker.step()
            broker.stop()
        }
    }

    override fun close() {
        stopRequested = true
        worker.join(5_000)
    }

    /** Брокер читает хранилище по пути к файлу, поэтому ресурс копируется во временный файл. */
    private fun tlsSettings(): TLSSettings {
        val file = File.createTempFile("kmqtt-raw-test-server", ".p12")
        file.deleteOnExit()
        val resource = RawTestBroker::class.java.classLoader.getResourceAsStream("tls/server.p12")
            ?: error("нет ресурса tls/server.p12")
        resource.use { input -> file.outputStream().use { input.copyTo(it) } }
        return TLSSettings(keyStoreFilePath = file.absolutePath, keyStorePassword = "kmqtt-test")
    }
}
