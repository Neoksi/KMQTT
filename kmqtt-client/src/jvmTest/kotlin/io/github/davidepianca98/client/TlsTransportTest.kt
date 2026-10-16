@file:OptIn(MqttCoreApi::class)

package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.socket.tls.TLSClientSettings
import kotlinx.coroutines.runBlocking
import java.io.InputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.security.KeyStore
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLHandshakeException
import javax.net.ssl.SSLServerSocket
import javax.net.ssl.SSLSocket
import kotlin.concurrent.thread
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/** S06b-T04b: `TlsTransport` (SSLEngine над `NioTransport`) в `NioActorHost` против локального `SSLServerSocket`. */
class TlsTransportTest {

    private class TestDriver : HostDriver {
        val commands = ConcurrentLinkedQueue<HostAction>()
        val events = LinkedBlockingQueue<TransportEvent>()
        val firstRun = CountDownLatch(1)

        override fun runOnce(nowMs: Long, events: List<TransportEvent>): HostStep {
            this.events.addAll(events)
            firstRun.countDown()
            val actions = ArrayList<HostAction>()
            while (true) actions += commands.poll() ?: break
            return HostStep(null, actions)
        }

        fun send(host: MqttHost, action: HostAction) {
            commands.add(action)
            host.wakeup()
        }

        fun awaitEvent(timeoutMs: Long = 5_000): TransportEvent =
            events.poll(timeoutMs, TimeUnit.MILLISECONDS) ?: fail("нет события транспорта за $timeoutMs мс")
    }

    private val toClose: MutableList<AutoCloseable> = java.util.Collections.synchronizedList(ArrayList())
    private val hosts = ArrayList<MqttHost>()

    @AfterTest
    fun tearDown() {
        runBlocking { hosts.forEach { it.close(5_000) } }
        synchronized(toClose) { toClose.toList() }.forEach { runCatching { it.close() } }
    }

    private fun resource(name: String): InputStream =
        javaClass.classLoader.getResourceAsStream(name) ?: fail("нет ресурса $name")

    private val serverPem: String by lazy {
        resource("tls/server.pem").use { it.readBytes().decodeToString() }.replace("\r\n", "\n")
    }

    private fun startedHost(driver: TestDriver, settings: TLSClientSettings, verifyHostname: Boolean = true): MqttHost {
        val host = NioActorHost(
            nextHostThreadName(),
            MqttClock.Monotonic,
            NioTransportFactory.tls(settings, verifyHostname),
            NioActorHost.DEFAULT_READ_BUFFER
        )
        hosts += host
        host.start(driver)
        assertTrue(driver.firstRun.await(5, TimeUnit.SECONDS), "первый runOnce")
        return host
    }

    /** TLS-сервер на loopback с ключом из `tls/server.p12` (SAN только `dns:localhost`). */
    private fun tlsServer(): SSLServerSocket {
        val keyStore = KeyStore.getInstance("PKCS12")
        resource("tls/server.p12").use { keyStore.load(it, KEYSTORE_PASSWORD) }
        val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
        kmf.init(keyStore, KEYSTORE_PASSWORD)
        val context = SSLContext.getInstance("TLS")
        context.init(kmf.keyManagers, null, null)
        val server = context.serverSocketFactory
            .createServerSocket(0, 50, InetAddress.getLoopbackAddress()) as SSLServerSocket
        toClose += server
        return server
    }

    private fun plainServer(): ServerSocket {
        val server = ServerSocket()
        server.bind(InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 50)
        toClose += server
        return server
    }

    /** Принять соединение и провести рукопожатие в фоне: результат — сокет или исключение рукопожатия. */
    private fun acceptAsync(server: SSLServerSocket): LinkedBlockingQueue<Any> {
        val result = LinkedBlockingQueue<Any>()
        thread(isDaemon = true, name = "tls-test-accept") {
            try {
                val socket = server.accept() as SSLSocket
                toClose += socket
                socket.soTimeout = 10_000
                try {
                    socket.startHandshake()
                    result.put(socket)
                } catch (e: Exception) {
                    result.put(e)
                }
            } catch (e: Exception) {
                result.put(e)
            }
        }
        return result
    }

    private fun connected(
        host: String = "localhost",
        settings: TLSClientSettings = TLSClientSettings(serverCertificate = serverPem)
    ): Triple<TestDriver, MqttHost, SSLSocket> {
        val server = tlsServer()
        val accepted = acceptAsync(server)
        val driver = TestDriver()
        val mqttHost = startedHost(driver, settings)
        driver.send(mqttHost, HostAction.Connect(host, server.localPort, 10_000))
        assertEquals(TransportEvent.Connected, driver.awaitEvent(10_000))
        val peer = assertIs<SSLSocket>(accepted.poll(10, TimeUnit.SECONDS))
        return Triple(driver, mqttHost, peer)
    }

    private fun TestDriver.readBytes(count: Int, timeoutMs: Long = 10_000): ByteArray {
        val out = java.io.ByteArrayOutputStream()
        while (out.size() < count) out.write(assertIs<TransportEvent.Bytes>(awaitEvent(timeoutMs)).bytes)
        return out.toByteArray()
    }

    private fun InputStream.readExactly(count: Int): ByteArray {
        val out = ByteArray(count)
        var read = 0
        while (read < count) {
            val n = read(out, read, count - read)
            if (n < 0) fail("EOF после $read из $count")
            read += n
        }
        return out
    }

    @Test
    fun handshakeWithPinnedPemAndExchange() {
        val (driver, host, peer) = connected()
        assertTrue(peer.session.protocol == "TLSv1.3" || peer.session.protocol == "TLSv1.2", peer.session.protocol)

        driver.send(host, HostAction.Write("hello".encodeToByteArray()))
        assertContentEquals("hello".encodeToByteArray(), peer.inputStream.readExactly(5))

        peer.outputStream.write("world".encodeToByteArray())
        peer.outputStream.flush()
        assertEquals("world", driver.readBytes(5).decodeToString())
    }

    @Test
    fun trustAllConnectsByIpWithoutHostnameCheck() {
        val (driver, host, peer) = connected("127.0.0.1", TLSClientSettings(checkServerCertificate = false))
        driver.send(host, HostAction.Write(byteArrayOf(1, 2, 3)))
        assertContentEquals(byteArrayOf(1, 2, 3), peer.inputStream.readExactly(3))
    }

    @Test
    fun hostnameMismatchFailsWithoutDetails() {
        // Сертификат выдан на localhost, подключаемся по IP: проверка имени хоста включена по умолчанию
        val failed = failedHandshake("127.0.0.1", TLSClientSettings(serverCertificate = serverPem))
        assertNoDetails(failed)
    }

    @Test
    fun hostnameCheckCanBeDisabledExplicitly() {
        val server = tlsServer()
        val accepted = acceptAsync(server)
        val driver = TestDriver()
        val host = startedHost(driver, TLSClientSettings(serverCertificate = serverPem), verifyHostname = false)
        driver.send(host, HostAction.Connect("127.0.0.1", server.localPort, 10_000))
        assertEquals(TransportEvent.Connected, driver.awaitEvent(10_000))
        assertIs<SSLSocket>(accepted.poll(10, TimeUnit.SECONDS))
    }

    @Test
    fun untrustedCertificateFailsWithoutDetails() {
        // Самоподписанный сертификат против системного хранилища доверия
        val failed = failedHandshake("localhost", TLSClientSettings())
        assertNoDetails(failed)
    }

    private fun failedHandshake(hostName: String, settings: TLSClientSettings): TransportEvent.Failed {
        val server = tlsServer()
        val accepted = acceptAsync(server)
        val driver = TestDriver()
        val host = startedHost(driver, settings)
        driver.send(host, HostAction.Connect(hostName, server.localPort, 10_000))
        val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent(10_000))
        val io = assertIs<MqttError.Io>(failed.error)
        assertEquals("ssl", io.kind)
        assertIs<SSLHandshakeException>(io.cause)
        // Сервер видит отказ клиента (alert или закрытие), а не висящее соединение
        val serverSide = accepted.poll(10, TimeUnit.SECONDS)
        assertTrue(serverSide is Exception, "сервер: рукопожатие не завершилось ($serverSide)")
        assertNull(driver.events.poll(300, TimeUnit.MILLISECONDS), "после Failed событий нет")
        return failed
    }

    private fun assertNoDetails(failed: TransportEvent.Failed) {
        val io = failed.error as MqttError.Io
        val texts = listOf(failed.toString(), io.toString(), io.cause?.message.orEmpty(), io.cause?.toString().orEmpty())
        for (text in texts) {
            for (secret in listOf("127.0.0.1", "localhost", "kmqtt test", "CN=", "BEGIN CERTIFICATE")) {
                assertFalse(text.contains(secret), "«$secret» в «$text»")
            }
        }
        assertNull(io.cause?.cause, "исходное исключение (с адресом и именами сертификата) не передаётся")
    }

    @Test
    fun invalidCertificateOpensNoConnection() {
        val server = plainServer()
        server.soTimeout = 500
        val driver = TestDriver()
        val host = startedHost(driver, TLSClientSettings(serverCertificate = "missing-certificate.pem"))
        driver.send(host, HostAction.Connect("127.0.0.1", server.localPort, 5_000))
        val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent())
        val io = assertIs<MqttError.Io>(failed.error)
        assertEquals("ssl-config", io.kind)
        assertFalse(io.cause?.message.orEmpty().contains("missing-certificate"), "путь не в ошибке")
        val accepted = try {
            server.accept()
        } catch (_: SocketTimeoutException) {
            null
        }
        assertNull(accepted, "сокет не открывался")
    }

    @Test
    fun tlsBelow12IsRejected() {
        val engine = createEngine(TLSClientSettings(), "localhost", 8883, verifyHostname = true)
        assertTrue(engine.enabledProtocols.isNotEmpty())
        assertTrue(engine.enabledProtocols.all { it == "TLSv1.2" || it == "TLSv1.3" }, engine.enabledProtocols.toList().toString())
        assertEquals("HTTPS", engine.sslParameters.endpointIdentificationAlgorithm)
        val trustAll = createEngine(TLSClientSettings(checkServerCertificate = false), "localhost", 8883, true)
        assertNull(trustAll.sslParameters.endpointIdentificationAlgorithm)
        runCatching { createEngine(TLSClientSettings(version = "TLSv1.1"), "localhost", 8883, true) }
            .let { assertTrue(it.isFailure, "TLS 1.1 не допускается") }
    }

    @Test
    fun largeWritesAndReadsInParts() {
        val (driver, host, peer) = connected()
        val size = 100 * 1024
        val payload = ByteArray(size) { (it * 31 + 7).toByte() }
        // Клиент → сервер: одна запись 100 КиБ (несколько TLS-записей), сервер читает медленно мелкими частями
        driver.send(host, HostAction.Write(payload))
        val received = java.io.ByteArrayOutputStream()
        val chunk = ByteArray(1500)
        while (received.size() < size) {
            val n = peer.inputStream.read(chunk)
            assertTrue(n > 0, "EOF после ${received.size()}")
            received.write(chunk, 0, n)
            if (received.size() < 20_000) Thread.sleep(1)
        }
        assertContentEquals(payload, received.toByteArray())
        // Несколько записей подряд в одном шаге
        val parts = (0 until 10).map { i -> ByteArray(10 * 1024 + i) { (i + it).toByte() } }
        parts.forEach { driver.commands.add(HostAction.Write(it)) }
        host.wakeup()
        val all = parts.fold(ByteArray(0)) { acc, b -> acc + b }
        assertContentEquals(all, peer.inputStream.readExactly(all.size))
        // Сервер → клиент: 100 КиБ частями
        thread(isDaemon = true) {
            for (offset in 0 until size step 7_000) {
                peer.outputStream.write(payload, offset, minOf(7_000, size - offset))
                peer.outputStream.flush()
            }
        }
        assertContentEquals(payload, driver.readBytes(size))
    }

    @Test
    fun pauseReadHoldsDataUntilResumed() {
        val (driver, host, peer) = connected()
        driver.send(host, HostAction.PauseRead(true))
        Thread.sleep(100)
        peer.outputStream.write("paused".encodeToByteArray())
        peer.outputStream.flush()
        assertNull(driver.events.poll(300, TimeUnit.MILLISECONDS), "чтение приостановлено")
        driver.send(host, HostAction.PauseRead(false))
        assertEquals("paused", driver.readBytes(6).decodeToString())
    }

    @Test
    fun closeSendsCloseNotify() {
        val (driver, host, peer) = connected()
        driver.send(host, HostAction.Write("x".encodeToByteArray()))
        assertContentEquals("x".encodeToByteArray(), peer.inputStream.readExactly(1))
        driver.send(host, HostAction.CloseTransport)
        assertEquals(-1, peer.inputStream.read(), "штатный конец потока")
    }

    /**
     * close_notify виден на проводе: сервер только TLS 1.2 (там alert — запись типа 21 в открытом заголовке), между
     * клиентом и сервером — TCP-ретранслятор, записывающий поток клиента. `SSLSocket` JDK возвращает -1 и на FIN без
     * close_notify, поэтому проверка по чтению сервера не различает эти случаи.
     */
    @Test
    fun closeNotifyIsTheLastRecordOnTheWire() {
        val server = tlsServer()
        server.enabledProtocols = arrayOf("TLSv1.2")
        val accepted = acceptAsync(server)
        val relay = plainServer()
        val fromClient = java.io.ByteArrayOutputStream()
        val clientDone = CountDownLatch(1)
        thread(isDaemon = true, name = "tls-test-relay") {
            val down = relay.accept().also { toClose += it }
            val up = Socket(InetAddress.getLoopbackAddress(), server.localPort).also { toClose += it }
            thread(isDaemon = true) { runCatching { up.getInputStream().copyTo(down.getOutputStream()) } }
            val buffer = ByteArray(4096)
            try {
                while (true) {
                    val n = down.getInputStream().read(buffer)
                    if (n < 0) break
                    synchronized(fromClient) { fromClient.write(buffer, 0, n) }
                    up.getOutputStream().write(buffer, 0, n)
                }
            } catch (_: Exception) {
            }
            clientDone.countDown()
            runCatching { up.shutdownOutput() }
        }
        val driver = TestDriver()
        val host = startedHost(driver, TLSClientSettings(serverCertificate = serverPem))
        driver.send(host, HostAction.Connect("localhost", relay.localPort, 10_000))
        assertEquals(TransportEvent.Connected, driver.awaitEvent(10_000))
        val peer = assertIs<SSLSocket>(accepted.poll(10, TimeUnit.SECONDS))
        assertEquals("TLSv1.2", peer.session.protocol)
        driver.send(host, HostAction.Write("x".encodeToByteArray()))
        assertContentEquals("x".encodeToByteArray(), peer.inputStream.readExactly(1))
        driver.send(host, HostAction.CloseTransport)
        assertTrue(clientDone.await(5, TimeUnit.SECONDS), "клиент закрыл соединение")
        val stream = synchronized(fromClient) { fromClient.toByteArray() }
        var offset = 0
        var lastType = -1
        while (offset + 5 <= stream.size) {
            lastType = stream[offset].toInt() and 0xFF
            val length = ((stream[offset + 3].toInt() and 0xFF) shl 8) or (stream[offset + 4].toInt() and 0xFF)
            offset += 5 + length
        }
        assertEquals(stream.size, offset, "поток клиента — целые TLS-записи")
        assertEquals(21, lastType, "последняя запись клиента — alert (close_notify)")
        assertEquals(-1, peer.inputStream.read())
    }

    @Test
    fun serverCloseNotifyGivesEofThenCloseWithoutReset() {
        val (driver, host, peer) = connected()
        peer.outputStream.write("bye".encodeToByteArray())
        peer.outputStream.flush()
        peer.shutdownOutputQuietly()
        assertEquals("bye", driver.readBytes(3).decodeToString())
        assertEquals(TransportEvent.Eof, driver.awaitEvent())
        driver.send(host, HostAction.CloseTransport)
        assertEquals(-1, peer.inputStream.read(), "клиент закрыл штатно")
    }

    /** close_notify со стороны сервера при открытом чтении (TLS 1.3 — полузакрытие). */
    private fun SSLSocket.shutdownOutputQuietly() {
        try {
            shutdownOutput()
        } catch (_: UnsupportedOperationException) {
            // TLS 1.2 у старых JDK: полузакрытия нет — закрываем целиком (close_notify всё равно уходит)
            close()
        }
    }

    @Test
    fun closeDuringHandshake() {
        // Сервер принимает TCP, но на ClientHello не отвечает
        val server = plainServer()
        val driver = TestDriver()
        val host = startedHost(driver, TLSClientSettings(serverCertificate = serverPem))
        driver.send(host, HostAction.Connect("localhost", server.localPort, 30_000))
        val peer = server.accept().also { toClose += it }
        peer.soTimeout = 5_000
        assertTrue(peer.getInputStream().read() >= 0, "ClientHello пришёл")
        driver.send(host, HostAction.CloseTransport)
        val buffer = ByteArray(4096)
        while (true) {
            val n = peer.getInputStream().read(buffer)
            if (n < 0) break
        }
        assertNull(driver.events.poll(300, TimeUnit.MILLISECONDS), "ни Connected, ни Failed после CloseTransport")
    }

    @Test
    fun hostCloseDuringHandshakeIsFast() {
        val server = plainServer()
        val driver = TestDriver()
        val host = startedHost(driver, TLSClientSettings(serverCertificate = serverPem))
        driver.send(host, HostAction.Connect("localhost", server.localPort, 30_000))
        val peer = server.accept().also { toClose += it }
        peer.soTimeout = 5_000
        assertTrue(peer.getInputStream().read() >= 0, "ClientHello пришёл")
        val started = System.nanoTime()
        assertTrue(runBlocking { host.close(5_000) })
        assertTrue((System.nanoTime() - started) / 1_000_000 < 1_000, "close быстрый")
        val buffer = ByteArray(4096)
        while (peer.getInputStream().read(buffer) >= 0) Unit
    }

    @Test
    fun handshakeTimeoutIsConnectTimeout() {
        val server = plainServer()
        val driver = TestDriver()
        val host = startedHost(driver, TLSClientSettings(serverCertificate = serverPem))
        driver.send(host, HostAction.Connect("localhost", server.localPort, 500))
        val peer = server.accept().also { toClose += it }
        val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent(5_000))
        assertEquals(MqttError.ConnectTimeout, failed.error)
        peer.soTimeout = 5_000
        val buffer = ByteArray(4096)
        while (peer.getInputStream().read(buffer) >= 0) Unit
    }

    @Test
    fun serverEofDuringHandshakeFails() {
        val server = plainServer()
        val driver = TestDriver()
        val host = startedHost(driver, TLSClientSettings(serverCertificate = serverPem))
        driver.send(host, HostAction.Connect("localhost", server.localPort, 10_000))
        val peer = server.accept()
        peer.soTimeout = 5_000
        assertTrue(peer.getInputStream().read() >= 0)
        peer.shutdownOutput()
        val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent())
        assertEquals("ssl", assertIs<MqttError.Io>(failed.error).kind)
        peer.close()
    }

    /** EOF TCP после рукопожатия без close_notify (ретранслятор закрывает поток к клиенту): Eof, затем закрытие без RST. */
    @Test
    fun tcpEofWithoutCloseNotifyGivesEofThenCloseWithoutReset() {
        val server = tlsServer()
        val accepted = acceptAsync(server)
        val relay = plainServer()
        val downstream = LinkedBlockingQueue<Socket>()
        val clientEnd = LinkedBlockingQueue<Any>()
        thread(isDaemon = true, name = "tls-test-relay") {
            val down = relay.accept().also { toClose += it }
            val up = Socket(InetAddress.getLoopbackAddress(), server.localPort).also { toClose += it }
            thread(isDaemon = true) { runCatching { up.getInputStream().copyTo(down.getOutputStream()) } }
            downstream.put(down)
            val buffer = ByteArray(4096)
            try {
                while (true) {
                    val n = down.getInputStream().read(buffer)
                    if (n < 0) break
                    up.getOutputStream().write(buffer, 0, n)
                }
                clientEnd.put(-1)
            } catch (e: Exception) {
                clientEnd.put(e)
            }
        }
        val driver = TestDriver()
        val host = startedHost(driver, TLSClientSettings(serverCertificate = serverPem))
        driver.send(host, HostAction.Connect("localhost", relay.localPort, 10_000))
        assertEquals(TransportEvent.Connected, driver.awaitEvent(10_000))
        val peer = assertIs<SSLSocket>(accepted.poll(10, TimeUnit.SECONDS))
        peer.outputStream.write("data".encodeToByteArray())
        peer.outputStream.flush()
        assertEquals("data", driver.readBytes(4).decodeToString())
        // FIN к клиенту без close_notify
        assertNotNull(downstream.poll(5, TimeUnit.SECONDS)).shutdownOutput()
        assertEquals(TransportEvent.Eof, driver.awaitEvent())
        driver.send(host, HostAction.CloseTransport)
        assertEquals(-1, clientEnd.poll(5, TimeUnit.SECONDS), "клиент закрыл штатно (FIN, не RST)")
    }

    @Test
    fun alpnAndExplicitSniFromSettingsReachTheEngine() {
        val engine = createEngine(
            TLSClientSettings(
                checkServerCertificate = false,
                alpnProtocols = listOf("mqtt"),
                serverNameIndications = "broker.example"
            ),
            "127.0.0.1",
            8883,
            verifyHostname = true
        )
        assertEquals(listOf("mqtt"), engine.sslParameters.applicationProtocols.toList())
        assertEquals(
            listOf("broker.example"),
            engine.sslParameters.serverNames.map { (it as javax.net.ssl.SNIHostName).asciiName }
        )
    }

    @Test
    fun withoutAlpnAndSniSettingsTheEngineKeepsTheDefaults() {
        val engine = createEngine(TLSClientSettings(checkServerCertificate = false), "127.0.0.1", 8883, true)
        assertTrue(engine.sslParameters.applicationProtocols.isEmpty())
    }

    private companion object {
        val KEYSTORE_PASSWORD = "kmqtt-test".toCharArray()
    }
}
