@file:OptIn(MqttCoreApi::class)

package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import kotlinx.coroutines.runBlocking
import org.junit.Assume.assumeTrue
import java.io.InputStream
import java.lang.management.ManagementFactory
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/** S06b-T04a: JVM-хост `NioActorHost` + `NioTransport` против локальных сокетов (loopback). */
class NioActorHostTest {

    /** Драйвер теста: действия — из очереди теста, события и моменты вызовов `runOnce` — в очереди теста. */
    private class TestDriver : HostDriver {
        val commands = ConcurrentLinkedQueue<HostAction>()
        val events = LinkedBlockingQueue<TransportEvent>()
        val runs = LinkedBlockingQueue<Long>()
        val stopped = CountDownLatch(1)

        @Volatile
        var deadline: Long? = null

        @Volatile
        var thread: Thread? = null

        @Volatile
        var stopCause: Throwable? = null

        @Volatile
        var onRun: (() -> Unit)? = null

        @Volatile
        var throwOnRun: Throwable? = null

        override fun runOnce(nowMs: Long, events: List<TransportEvent>): HostStep {
            thread = Thread.currentThread()
            this.events.addAll(events)
            onRun?.invoke()
            runs.offer(System.nanoTime())
            throwOnRun?.let { throw it }
            val actions = ArrayList<HostAction>()
            while (true) actions += commands.poll() ?: break
            return HostStep(deadline, actions)
        }

        override fun onStopped(cause: Throwable?) {
            stopCause = cause
            stopped.countDown()
        }

        fun send(host: MqttHost, action: HostAction) {
            commands.add(action)
            host.wakeup()
        }

        fun awaitEvent(timeoutMs: Long = 5_000): TransportEvent =
            events.poll(timeoutMs, TimeUnit.MILLISECONDS) ?: fail("нет события транспорта за $timeoutMs мс")
    }

    private val toClose = ArrayList<AutoCloseable>()
    private val hosts = ArrayList<MqttHost>()

    @AfterTest
    fun tearDown() {
        runBlocking { hosts.forEach { it.close(5_000) } }
        toClose.forEach { runCatching { it.close() } }
    }

    private fun host(factory: NioTransportFactory = NioTransportFactory.Tcp): NioActorHost =
        NioActorHost(nextHostThreadName(), MqttClock.Monotonic, factory, NioActorHost.DEFAULT_READ_BUFFER)
            .also { hosts += it }

    private fun server(backlog: Int = 50, receiveBuffer: Int? = null): ServerSocket {
        val server = ServerSocket()
        receiveBuffer?.let { server.receiveBufferSize = it }
        server.bind(InetSocketAddress(InetAddress.getLoopbackAddress(), 0), backlog)
        toClose += server
        return server
    }

    private fun startedHost(driver: TestDriver, factory: NioTransportFactory = NioTransportFactory.Tcp): NioActorHost {
        val host = host(factory)
        host.start(driver)
        assertNotNull(driver.runs.poll(5, TimeUnit.SECONDS), "первый runOnce")
        return host
    }

    private fun kmqttThreads(): Int {
        val bean = ManagementFactory.getThreadMXBean()
        return bean.getThreadInfo(bean.allThreadIds).count {
            it != null && it.threadName.startsWith(NioActorHost.THREAD_NAME_PREFIX)
        }
    }

    private fun openFds(): Long? =
        (ManagementFactory.getOperatingSystemMXBean() as? com.sun.management.UnixOperatingSystemMXBean)
            ?.openFileDescriptorCount

    @Test
    fun threadIsNamedDaemonAndNumberGrows() {
        val d1 = TestDriver()
        val h1 = startedHost(d1)
        val d2 = TestDriver()
        startedHost(d2)
        val t1 = assertNotNull(d1.thread)
        val t2 = assertNotNull(d2.thread)
        val n1 = Regex("kmqtt-io-(\\d+)").matchEntire(t1.name)?.groupValues?.get(1)?.toLong()
        val n2 = Regex("kmqtt-io-(\\d+)").matchEntire(t2.name)?.groupValues?.get(1)?.toLong()
        assertNotNull(n1, t1.name)
        assertNotNull(n2, t2.name)
        assertTrue(n2 > n1, "номер растёт глобально: $n1 → $n2")
        assertTrue(t1.isDaemon && t2.isDaemon, "daemon")
        assertEquals(h1.name, t1.name)
        assertTrue(runBlocking { h1.close() })
        assertTrue(d1.stopped.await(1, TimeUnit.SECONDS))
        assertNull(d1.stopCause)
        assertTrue(!t1.isAlive, "поток вышел")
    }

    @Test
    fun startTwiceAndAfterCloseFails() {
        val host = host()
        host.start(TestDriver())
        runCatching { host.start(TestDriver()) }.let { assertIs<IllegalStateException>(it.exceptionOrNull()) }
        assertTrue(runBlocking { host.close() })
        assertTrue(runBlocking { host.close() }, "повторный close безвреден")
        val unused = host()
        assertTrue(runBlocking { unused.close() }, "close без start")
        runCatching { unused.start(TestDriver()) }.let { assertIs<IllegalStateException>(it.exceptionOrNull()) }
    }

    @Test
    fun connectWriteReadEofAndCloseWithoutReset() {
        val server = server()
        val driver = TestDriver()
        val host = startedHost(driver)
        driver.send(host, HostAction.Connect("localhost", server.localPort, 5_000))
        val peer = server.accept().also { toClose += it }
        peer.soTimeout = 5_000
        assertEquals(TransportEvent.Connected, driver.awaitEvent())

        driver.send(host, HostAction.Write("hello".encodeToByteArray()))
        assertContentEquals("hello".encodeToByteArray(), peer.getInputStream().readExactly(5))

        peer.getOutputStream().write("world".encodeToByteArray())
        peer.getOutputStream().flush()
        val received = StringBuilder()
        while (received.length < 5) {
            received.append(assertIs<TransportEvent.Bytes>(driver.awaitEvent()).bytes.decodeToString())
        }
        assertEquals("world", received.toString())

        // H1 со стороны хоста: брокер закрыл поток → Eof, канал открыт до CloseTransport → сервер видит FIN, не RST
        peer.shutdownOutput()
        assertEquals(TransportEvent.Eof, driver.awaitEvent())
        driver.send(host, HostAction.CloseTransport)
        assertEquals(-1, peer.getInputStream().read(), "штатный EOF (без Connection reset)")
    }

    @Test
    fun connectRefusedIsIoWithExceptionClassName() {
        val port = ServerSocket(0, 1, InetAddress.getLoopbackAddress()).use { it.localPort }
        val driver = TestDriver()
        val host = startedHost(driver)
        driver.send(host, HostAction.Connect("127.0.0.1", port, 10_000))
        val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent(10_000))
        val io = assertIs<MqttError.Io>(failed.error)
        assertEquals("ConnectException", io.kind)
    }

    @Test
    fun unknownHostForInvalidName() {
        val driver = TestDriver()
        val host = startedHost(driver)
        // Срок с запасом: резолвер Windows перебирает суффиксы поиска, NXDOMAIN может идти секунды
        driver.send(host, HostAction.Connect("nonexistent.invalid", 1883, 30_000))
        val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent(35_000))
        assertEquals(MqttError.UnknownHost, failed.error)
    }

    @Test
    fun hangingDnsHitsConnectTimeoutWithoutBlockingHostThread() {
        val release = CountDownLatch(1)
        try {
            val factory = NioTransportFactory { NioTransport(it) { _ -> release.await(); throw IllegalStateException() } }
            val driver = TestDriver()
            val host = startedHost(driver, factory)
            val started = System.nanoTime()
            driver.send(host, HostAction.Connect("blocked.test", 1883, 150))
            val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent(5_000))
            val elapsedMs = (System.nanoTime() - started) / 1_000_000
            assertEquals(MqttError.ConnectTimeout, failed.error)
            assertTrue(elapsedMs in 100..2_000, "дедлайн подключения: $elapsedMs мс")
        } finally {
            release.countDown()
        }
    }

    /** H5, детерминированно: подключение «висит» на резолве; close() < 200 мс. */
    @Test
    fun closeDuringHangingResolveIsFast() {
        val release = CountDownLatch(1)
        val entered = CountDownLatch(1)
        try {
            val factory = NioTransportFactory {
                NioTransport(it) { _ ->
                    entered.countDown()
                    release.await()
                    throw IllegalStateException()
                }
            }
            val driver = TestDriver()
            val host = startedHost(driver, factory)
            driver.send(host, HostAction.Connect("blocked.test", 1883, 60_000))
            assertTrue(entered.await(5, TimeUnit.SECONDS), "резолв начат")
            assertCloseFast(host, driver)
        } finally {
            release.countDown()
        }
    }

    /** H5 на настоящем «висящем» TCP-подключении (если среда его даёт; иначе — пропуск с пометкой). */
    @Test
    fun closeDuringHangingTcpConnectIsFast() {
        val target = findBlackHole()
        assumeTrue("нет «чёрной дыры»: loopback с полным backlog и 10.255.255.1 дают отказ, а не зависание", target != null)
        println("NioActorHostTest: «чёрная дыра» — $target")
        val driver = TestDriver()
        val host = startedHost(driver)
        driver.send(host, HostAction.Connect(target!!.hostString, target.port, 60_000))
        Thread.sleep(300)
        assumeTrue("подключение к $target завершилось за 300 мс — не «чёрная дыра»", driver.events.isEmpty())
        assertCloseFast(host, driver)
    }

    private fun assertCloseFast(host: NioActorHost, driver: TestDriver) {
        val thread = assertNotNull(driver.thread)
        val started = System.nanoTime()
        val closed = runBlocking { host.close() }
        val elapsedMs = (System.nanoTime() - started) / 1_000_000
        assertTrue(closed, "close() дождался выхода потока")
        assertTrue(elapsedMs < 200, "close() во время подключения: $elapsedMs мс")
        assertTrue(!thread.isAlive)
        assertTrue(driver.stopped.await(1, TimeUnit.SECONDS))
        assertNull(driver.stopCause)
        assertTrue(driver.events.none { it == TransportEvent.Connected })
    }

    /** Адрес, подключение к которому висит: сначала loopback с заполненным backlog, затем 10.255.255.1. */
    private fun findBlackHole(): InetSocketAddress? {
        val server = server(backlog = 1)
        val target = InetSocketAddress(InetAddress.getLoopbackAddress(), server.localPort)
        repeat(20) {
            val socket = Socket().also { toClose += it }
            try {
                socket.connect(target, 300)
            } catch (_: SocketTimeoutException) {
                return target
            } catch (_: Exception) {
                return@repeat
            }
        }
        val remote = InetSocketAddress("10.255.255.1", 1883)
        return try {
            Socket().use { it.connect(remote, 300) }
            null
        } catch (_: SocketTimeoutException) {
            remote
        } catch (_: Exception) {
            null
        }
    }

    @Test
    fun thousandStartCloseCyclesLeakNoThreadsOrFds() {
        val server = server()
        val acceptor = thread(name = "test-acceptor", isDaemon = true) {
            while (!server.isClosed) {
                try {
                    server.accept().close()
                } catch (_: Exception) {
                }
            }
        }
        // Прогрев: пул Dispatchers.IO, классы NIO
        repeat(5) { cycle(server.localPort) }
        Thread.sleep(200)
        val threadsBefore = kmqttThreads()
        val fdsBefore = openFds()
        val started = System.nanoTime()
        repeat(1_000) { cycle(server.localPort) }
        val elapsedMs = (System.nanoTime() - started) / 1_000_000
        Thread.sleep(300)
        val threadsAfter = kmqttThreads()
        val fdsAfter = openFds()
        println("NioActorHostTest: 1000 циклов за $elapsedMs мс; потоки kmqtt-io $threadsBefore → $threadsAfter; " +
            "fd ${fdsBefore ?: "н/д (не Unix)"} → ${fdsAfter ?: "н/д"}")
        assertEquals(threadsBefore, threadsAfter, "потоки kmqtt-io не растут")
        if (fdsBefore != null && fdsAfter != null) {
            assertTrue(fdsAfter <= fdsBefore + 5, "открытые fd: $fdsBefore → $fdsAfter")
        }
        server.close()
        acceptor.join(1_000)
    }

    private fun cycle(port: Int) {
        val driver = TestDriver()
        val host = NioActorHost()
        host.start(driver)
        driver.send(host, HostAction.Connect("127.0.0.1", port, 5_000))
        assertEquals(TransportEvent.Connected, driver.awaitEvent())
        assertTrue(runBlocking { host.close() }, "close() дождался потока")
        assertTrue(driver.stopped.await(1, TimeUnit.SECONDS))
    }

    @Test
    fun partialWriteDoesNotBlockAndCompletesAfterPeerReads() {
        val server = server(receiveBuffer = 4 * 1024)
        var transport: NioTransport? = null
        val factory = NioTransportFactory { ctx -> NioTransport(ctx).also { transport = it } }
        val driver = TestDriver()
        val pendingRef = AtomicLong(-1L)
        driver.onRun = { pendingRef.set(transport?.pendingWriteBytes ?: -1L) }
        val host = startedHost(driver, factory)
        driver.send(host, HostAction.Connect("127.0.0.1", server.localPort, 5_000))
        val peer = server.accept().also { toClose += it }
        assertEquals(TransportEvent.Connected, driver.awaitEvent())

        val size = 8 * 1024 * 1024
        val payload = ByteArray(size) { (it * 31 + 7).toByte() }
        driver.send(host, HostAction.Write(payload))
        Thread.sleep(300)
        // Поток хоста не заблокирован записью: wakeup → runOnce сразу, и остаток ещё не ушёл
        driver.runs.clear()
        host.wakeup()
        assertNotNull(driver.runs.poll(1, TimeUnit.SECONDS), "runOnce при висящей записи")
        val pending = pendingRef.get()
        assertTrue(pending > 0, "запись неполная (остаток $pending байт) — сервер не читает")

        val received = ByteArray(size)
        peer.soTimeout = 10_000
        var offset = 0
        val input = peer.getInputStream()
        while (offset < size) {
            val n = input.read(received, offset, size - offset)
            assertTrue(n > 0, "данные дошли до конца")
            offset += n
        }
        assertContentEquals(payload, received)
        val deadline = System.currentTimeMillis() + 5_000
        while (System.currentTimeMillis() < deadline) {
            host.wakeup()
            Thread.sleep(10)
            if (pendingRef.get() == 0L) break
        }
        assertEquals(0L, pendingRef.get(), "очередь записи опустела")
    }

    /**
     * S06b-T05f: `Write(8 МиБ)` + `Write(DISCONNECT)` + `FlushAndClose` при `SO_RCVBUF` 4 КиБ — запись частичная;
     * `host.close()` во время дописывания ждёт его; сервер получает всё, включая хвост, затем FIN (не RST). События
     * отданного на дописывание транспорта драйверу не доставляются.
     */
    @Test
    fun flushAndCloseDeliversWholeTailThenFinAndHostCloseWaitsForIt() {
        val server = server(receiveBuffer = 4 * 1024)
        var transport: NioTransport? = null
        val factory = NioTransportFactory { ctx -> NioTransport(ctx).also { transport = it } }
        val driver = TestDriver()
        val pendingRef = AtomicLong(-1L)
        driver.onRun = { pendingRef.set(transport?.pendingWriteBytes ?: -1L) }
        val host = startedHost(driver, factory)
        driver.send(host, HostAction.Connect("127.0.0.1", server.localPort, 5_000))
        val peer = server.accept().also { toClose += it }
        assertEquals(TransportEvent.Connected, driver.awaitEvent())

        val size = 8 * 1024 * 1024
        val payload = ByteArray(size) { (it * 31 + 7).toByte() }
        val disconnect = byteArrayOf(0xE0.toByte(), 0x00)
        driver.commands.add(HostAction.Write(payload))
        driver.commands.add(HostAction.Write(disconnect))
        driver.commands.add(HostAction.FlushAndClose(10_000))
        host.wakeup()
        Thread.sleep(300)
        driver.runs.clear()
        host.wakeup()
        assertNotNull(driver.runs.poll(1, TimeUnit.SECONDS), "поток хоста не заблокирован дописыванием")
        assertTrue(pendingRef.get() > 0, "запись частичная (остаток ${pendingRef.get()})")
        peer.getOutputStream().write("late".encodeToByteArray()) // входящее во время дописывания
        peer.getOutputStream().flush()

        val closeResult = AtomicReference<Boolean?>(null)
        val closer = thread(isDaemon = true) { closeResult.set(runBlocking { host.close(15_000) }) }
        Thread.sleep(200)
        assertNull(closeResult.get(), "host.close() ждёт дописывания")

        peer.soTimeout = 10_000
        val input = peer.getInputStream()
        assertContentEquals(payload + disconnect, input.readExactly(size + disconnect.size))
        assertEquals(-1, input.read(), "FIN после всего остатка (без Connection reset)")
        closer.join(5_000)
        assertEquals(true, closeResult.get(), "хост остановлен после дописывания")
        assertTrue(driver.events.none { it is TransportEvent.Bytes || it == TransportEvent.Eof }, "события после FlushAndClose не доставляются")
    }

    @Test
    fun flushAndCloseDeadlineClosesWithoutBlockingHostWhenPeerNeverReads() {
        val server = server(receiveBuffer = 4 * 1024)
        var transport: NioTransport? = null
        val factory = NioTransportFactory { ctx -> NioTransport(ctx).also { transport = it } }
        val driver = TestDriver()
        val pendingRef = AtomicLong(-1L)
        driver.onRun = { pendingRef.set(transport?.pendingWriteBytes ?: -1L) }
        val host = startedHost(driver, factory)
        driver.send(host, HostAction.Connect("127.0.0.1", server.localPort, 5_000))
        server.accept().also { toClose += it } // сервер не читает
        assertEquals(TransportEvent.Connected, driver.awaitEvent())

        driver.commands.add(HostAction.Write(ByteArray(8 * 1024 * 1024)))
        val t0 = System.nanoTime()
        driver.commands.add(HostAction.FlushAndClose(300))
        host.wakeup()
        Thread.sleep(100)
        driver.runs.clear()
        host.wakeup()
        assertNotNull(driver.runs.poll(1, TimeUnit.SECONDS))
        assertTrue(pendingRef.get() > 0, "остаток висит")
        // Поток хоста отвечает, а по сроку транспорт закрыт (очередь сброшена) — без ожидания читателя
        val deadline = System.currentTimeMillis() + 5_000
        while (pendingRef.get() != 0L && System.currentTimeMillis() < deadline) {
            driver.runs.clear()
            host.wakeup()
            assertNotNull(driver.runs.poll(1, TimeUnit.SECONDS), "runOnce во время дописывания")
            Thread.sleep(10)
        }
        val elapsedMs = (System.nanoTime() - t0) / 1_000_000
        assertEquals(0L, pendingRef.get(), "транспорт закрыт по сроку")
        assertTrue(elapsedMs in 250..3_000, "закрытие по сроку через $elapsedMs мс")
        val started = System.nanoTime()
        assertTrue(runBlocking { host.close(5_000) })
        assertTrue((System.nanoTime() - started) / 1_000_000 < 1_000, "close() без дописывающихся — сразу")
    }

    @Test
    fun flushAndCloseWithEmptyQueueClosesAtOnce() {
        val server = server()
        val driver = TestDriver()
        val host = startedHost(driver)
        driver.send(host, HostAction.Connect("127.0.0.1", server.localPort, 5_000))
        val peer = server.accept().also { toClose += it }
        assertEquals(TransportEvent.Connected, driver.awaitEvent())
        driver.send(host, HostAction.Write("hi".encodeToByteArray()))
        peer.soTimeout = 2_000
        val input = peer.getInputStream()
        assertContentEquals("hi".encodeToByteArray(), input.readExactly(2))
        val t0 = System.nanoTime()
        driver.send(host, HostAction.FlushAndClose(10_000))
        assertEquals(-1, input.read(), "FIN сразу")
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 1_000)
        assertTrue(runBlocking { host.close(1_000) })
    }

    @Test
    fun wakeupInterruptsSelectImmediately() {
        val driver = TestDriver()
        driver.deadline = null
        val host = startedHost(driver)
        val latencies = ArrayList<Long>()
        repeat(60) {
            Thread.sleep(3)
            driver.runs.clear()
            val t0 = System.nanoTime()
            host.wakeup()
            val t1 = driver.runs.poll(1, TimeUnit.SECONDS) ?: fail("runOnce не вызван после wakeup")
            latencies += t1 - t0
        }
        val sorted = latencies.drop(10).sorted()
        val medianUs = sorted[sorted.size / 2] / 1_000
        val p95Us = sorted[(sorted.size * 95) / 100] / 1_000
        println("NioActorHostTest: wakeup → runOnce медиана $medianUs мкс, p95 $p95Us мкс, макс ${sorted.last() / 1_000} мкс")
        assertTrue(medianUs < 5_000, "медиана wakeup → runOnce $medianUs мкс")
        assertTrue(p95Us < 5_000, "p95 wakeup → runOnce $p95Us мкс")
    }

    @Test
    fun driverDeadlineWakesHost() {
        val driver = TestDriver()
        val host = startedHost(driver)
        driver.runs.clear()
        val t0 = System.nanoTime()
        driver.deadline = MqttClock.Monotonic.nowMs() + 100
        host.wakeup()
        assertNotNull(driver.runs.poll(1, TimeUnit.SECONDS))
        driver.deadline = null
        val next = assertNotNull(driver.runs.poll(1, TimeUnit.SECONDS), "runOnce по дедлайну")
        val elapsedMs = (next - t0) / 1_000_000
        assertTrue(elapsedMs in 90..1_000, "проснулся по дедлайну через $elapsedMs мс")
    }

    @Test
    fun pauseReadStopsReadingUntilResumed() {
        val server = server()
        val driver = TestDriver()
        val host = startedHost(driver)
        driver.send(host, HostAction.Connect("127.0.0.1", server.localPort, 5_000))
        val peer = server.accept().also { toClose += it }
        assertEquals(TransportEvent.Connected, driver.awaitEvent())
        driver.send(host, HostAction.PauseRead(true))
        Thread.sleep(50)
        peer.getOutputStream().write(byteArrayOf(1, 2, 3))
        peer.getOutputStream().flush()
        Thread.sleep(200)
        assertTrue(driver.events.isEmpty(), "на паузе не читаем")
        driver.send(host, HostAction.PauseRead(false))
        val bytes = assertIs<TransportEvent.Bytes>(driver.awaitEvent())
        assertContentEquals(byteArrayOf(1, 2, 3), bytes.bytes)
    }

    @Test
    fun driverExceptionStopsHostAndReportsCause() {
        val driver = TestDriver()
        val host = startedHost(driver)
        val thread = assertNotNull(driver.thread)
        val boom = IllegalStateException("boom")
        driver.throwOnRun = boom
        host.wakeup()
        assertTrue(driver.stopped.await(2, TimeUnit.SECONDS))
        assertEquals(boom, driver.stopCause)
        thread.join(1_000)
        assertTrue(!thread.isAlive)
        assertTrue(runBlocking { host.close() })
    }

    private fun InputStream.readExactly(count: Int): ByteArray {
        val out = ByteArray(count)
        var offset = 0
        while (offset < count) {
            val n = read(out, offset, count - offset)
            if (n < 0) fail("EOF после $offset байт")
            offset += n
        }
        return out
    }
}
