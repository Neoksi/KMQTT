package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.BrokerScript
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.broker.Broker
import io.github.davidepianca98.socket.tls.TLSClientSettings
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.InputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.security.KeyStore
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLServerSocket
import kotlin.concurrent.thread
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.fail
import kotlin.time.Duration.Companion.seconds

/**
 * JVM-фабрика `MqttClient(options)` (S06b-T05b): TLS-хост при `tls != null` с проверкой имени хоста по умолчанию
 * (решение владельца 2026-10-16), отказ общей фабрики при `tls` и TCP-хосте, никогда — открытый текст.
 * Сервер TLS — как в `TlsTransportTest` (`tls/server.p12`, SAN только `dns:localhost`).
 */
class MqttClientJvmTest {
    private val toClose = ArrayList<AutoCloseable>()
    private val clients = ArrayList<MqttClient>()

    @AfterTest
    fun tearDown() {
        runBlocking { clients.forEach { runCatching { withTimeout(10_000) { it.close() } } } }
        toClose.asReversed().forEach { runCatching { it.close() } }
    }

    private fun resource(name: String): InputStream =
        javaClass.classLoader.getResourceAsStream(name) ?: fail("нет ресурса $name")

    private val serverPem: String by lazy {
        resource("tls/server.pem").use { it.readBytes().decodeToString() }.replace("\r\n", "\n")
    }

    private fun tlsServer(): SSLServerSocket {
        val keyStore = KeyStore.getInstance("PKCS12")
        resource("tls/server.p12").use { keyStore.load(it, KEYSTORE_PASSWORD) }
        val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
        kmf.init(keyStore, KEYSTORE_PASSWORD)
        val context = SSLContext.getInstance("TLS")
        context.init(kmf.keyManagers, null, null)
        val server = context.serverSocketFactory.createServerSocket(0, 50, InetAddress.getLoopbackAddress()) as SSLServerSocket
        toClose += server
        return server
    }

    private fun plainServer(): ServerSocket {
        val server = ServerSocket()
        server.bind(InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 50)
        toClose += server
        return server
    }

    /** Один MQTT-пакет из потока: первый байт, длина (varint), тело. */
    private fun InputStream.readPacket(): Int {
        val first = read()
        if (first < 0) return -1
        var length = 0
        var shift = 0
        while (true) {
            val b = read()
            if (b < 0) return -1
            length = length or ((b and 0x7F) shl shift)
            if (b and 0x80 == 0) break
            shift += 7
        }
        readNBytes(length)
        return first
    }

    /**
     * MQTT-брокер на одно соединение поверх [server]: принимает, читает CONNECT, отвечает CONNACK, читает следующий
     * пакет (DISCONNECT) и закрывает. Первые байты пакетов клиента — в очереди (исключение — тоже в очереди).
     */
    private fun serveOnce(server: ServerSocket): LinkedBlockingQueue<Any> {
        val seen = LinkedBlockingQueue<Any>()
        thread(isDaemon = true, name = "jvm-factory-test-broker") {
            try {
                server.accept().use { socket ->
                    socket.soTimeout = 10_000
                    val input = socket.getInputStream()
                    val connect = input.readPacket()
                    seen.put(connect)
                    if (connect != 0x10) return@use
                    socket.getOutputStream().apply {
                        write(BrokerScript(MQTTVersion.MQTT5).connack())
                        flush()
                    }
                    seen.put(input.readPacket())
                }
            } catch (e: Exception) {
                seen.put(e)
            }
        }
        return seen
    }

    private fun client(options: MqttClientOptions): MqttClient = MqttClient(options).also { clients += it }

    private fun options(host: String, port: Int, tls: TLSClientSettings?, verifyHostname: Boolean = true) = MqttClientOptions(
        host = host,
        port = port,
        tls = tls,
        verifyHostname = verifyHostname,
        keepAlive = 10.seconds,
        connectTimeout = 10.seconds,
        closeLinger = 1.seconds,
        reconnect = ReconnectPolicy.Never,
    )

    @Test
    fun tlsOptionsConnectOverTlsWithPinnedCertificateAndHostnameCheck() = runBlocking<Unit> {
        val server = tlsServer()
        val seen = serveOnce(server)
        val client = client(options("localhost", server.localPort, TLSClientSettings(serverCertificate = serverPem)))
        assertIs<ConnectResult.Connected>(withTimeout(10_000) { client.connect() })
        assertEquals(0x10, seen.poll(10, TimeUnit.SECONDS), "сервер расшифровал CONNECT")
        withTimeout(10_000) { client.close() }
        assertEquals(0xE0, seen.poll(10, TimeUnit.SECONDS), "DISCONNECT по TLS")
        assertEquals(ConnectionState.Closed(null), client.connection.value)
    }

    @Test
    fun hostnameCheckIsOnByDefaultAndFailsByIpWithoutDetails() = runBlocking<Unit> {
        val server = tlsServer()
        serveOnce(server)
        val client = client(options("127.0.0.1", server.localPort, TLSClientSettings(serverCertificate = serverPem)))
        val result = assertIs<ConnectResult.Failed>(withTimeout(10_000) { client.connect() })
        assertEquals("ssl", assertIs<MqttError.Io>(result.error).kind)
        val text = result.toString() + client.connection.value.toString()
        for (detail in listOf("127.0.0.1", "localhost", server.localPort.toString())) assertFalse(detail in text, text)
    }

    @Test
    fun hostnameCheckCanBeDisabledExplicitly() = runBlocking<Unit> {
        val server = tlsServer()
        serveOnce(server)
        val client = client(
            options("127.0.0.1", server.localPort, TLSClientSettings(serverCertificate = serverPem), verifyHostname = false),
        )
        assertIs<ConnectResult.Connected>(withTimeout(10_000) { client.connect() })
    }

    @Test
    fun tlsOptionsNeverFallBackToPlaintext() = runBlocking<Unit> {
        val server = plainServer()
        val first = LinkedBlockingQueue<Int>()
        thread(isDaemon = true, name = "jvm-factory-test-plain") {
            runCatching {
                server.accept().use { socket: Socket ->
                    socket.soTimeout = 10_000
                    first.put(socket.getInputStream().read())
                }
            }
        }
        val client = client(options("localhost", server.localPort, TLSClientSettings(serverCertificate = serverPem)))
        assertIs<ConnectResult.Failed>(withTimeout(10_000) { client.connect() })
        assertEquals(0x16, first.poll(10, TimeUnit.SECONDS), "первый байт — запись TLS-рукопожатия, а не CONNECT (0x10)")
    }

    @Test
    fun tlsOptionsWithTcpHostAreRefusedBeforeHostStarts() = runBlocking<Unit> {
        val host = NioActorHost()
        val options = options("broker.test", 8883, TLSClientSettings(clientCertificatePassword = "tls-pass"))
        val error = assertFailsWith<IllegalArgumentException> { MqttClient(options, host) }
        val message = error.message.orEmpty()
        for (detail in listOf("broker.test", "8883", "tls-pass")) assertFalse(detail in message, message)
        assertFalse(Thread.getAllStackTraces().keys.any { it.name == host.name }, "поток хоста не запускался")
        host.close()
    }

    @Test
    fun plainOptionsUseTcpHostAgainstForkBroker() = runBlocking<Unit> {
        val port = ServerSocket(0).use { it.localPort }
        val broker = Broker(port = port, host = "127.0.0.1")
        val stop = AtomicBoolean(false)
        val worker = thread(name = "jvm-factory-fork-broker", isDaemon = true) {
            while (!stop.get()) broker.step()
            broker.stop()
        }
        toClose += AutoCloseable {
            stop.set(true)
            worker.join(5_000)
        }
        val client = client(options("127.0.0.1", port, tls = null))
        assertIs<ConnectResult.Connected>(withTimeout(10_000) { client.connect() })
        assertEquals(
            PublishResult.Delivered((client.connection.value as ConnectionState.Connected).epoch),
            withTimeout(10_000) { client.publish("jvm/x", byteArrayOf(1), until = PublishStage.Delivered) },
        )
        withTimeout(10_000) { client.close() }
        assertEquals(ConnectionState.Closed(null), client.connection.value)
    }

    private companion object {
        val KEYSTORE_PASSWORD = "kmqtt-test".toCharArray()
    }
}
