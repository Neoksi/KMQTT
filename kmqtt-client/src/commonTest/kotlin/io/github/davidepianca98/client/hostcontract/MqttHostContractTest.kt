@file:OptIn(MqttCoreApi::class)

package io.github.davidepianca98.client.hostcontract

import io.github.davidepianca98.client.ConnectResult
import io.github.davidepianca98.client.ConnectionState
import io.github.davidepianca98.client.HostAction
import io.github.davidepianca98.client.MqttClient
import io.github.davidepianca98.client.MqttClientOptions
import io.github.davidepianca98.client.MqttClock
import io.github.davidepianca98.client.MqttError
import io.github.davidepianca98.client.MqttHost
import io.github.davidepianca98.client.PublishResult
import io.github.davidepianca98.client.ReconnectPolicy
import io.github.davidepianca98.client.TransportEvent
import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.TestResult
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * SEVEN-250: контракт [MqttHost] + TCP-транспорта — одни сценарии для каждой реализации хоста (JVM `NioActorHost`,
 * JS Node, mingwX64 WinSock). Сценарии повторяют `NioActorHostTest` (JVM) без платформенных деталей: имени класса
 * исключения в `MqttError.Io.kind`, имён потоков, счётчиков дескрипторов, подмены резолвера — они остаются в тестах
 * платформ. Все ожидания идут в реальном времени (`withContext(Dispatchers.Default)` внутри `runTest`).
 */
internal abstract class MqttHostContractTest {

    /** Новый хост с TCP-транспортом; [probe] получает каждый созданный транспорт. */
    protected abstract fun newHost(probe: PendingProbe? = null): MqttHost

    /** Сервер на 127.0.0.1; [smallReceiveBuffer] — маленький буфер приёма (4 КиБ), где платформа его даёт. */
    protected abstract suspend fun newServer(smallReceiveBuffer: Boolean = false): ContractServer

    /** Имя платформы для сообщений. */
    protected abstract val platform: String

    /** Объём записи, которая заведомо не уходит целиком, пока сервер не читает. */
    protected open val bigPayload: Int = 8 * 1024 * 1024

    /**
     * Видно ли неотданный остаток записи (`pendingWriteBytes > 0`), пока сервер не читает. `false` — у Node на Windows:
     * libuv пишет через IOCP, и ядро принимает запись любого размера целиком (`writableLength` сразу 0). Тогда
     * сценарии частичной записи проверяют только доставку и FIN, а сценарий срока дописывания пропускается.
     */
    protected open val writeBackpressureVisible: Boolean = true

    private class Scope(val hosts: MutableList<MqttHost>, val servers: MutableList<ContractServer>)

    private fun contract(block: suspend CoroutineScope.(Scope) -> Unit): TestResult = runTest(timeout = 120.seconds) {
        withContext(Dispatchers.Default) {
            val scope = Scope(ArrayList(), ArrayList())
            try {
                block(scope)
            } finally {
                for (host in scope.hosts) host.close(5_000)
                for (server in scope.servers) runCatching { server.close() }
            }
        }
    }

    private fun Scope.host(probe: PendingProbe? = null): MqttHost = newHost(probe).also { hosts += it }

    private suspend fun Scope.server(smallReceiveBuffer: Boolean = false): ContractServer =
        newServer(smallReceiveBuffer).also { servers += it }

    private suspend fun Scope.startedHost(driver: ContractDriver, probe: PendingProbe? = null): MqttHost {
        val host = host(probe)
        host.start(driver)
        driver.awaitRun(5_000)
        return host
    }

    private fun elapsedMs(start: TimeSource.Monotonic.ValueTimeMark): Long = start.elapsedNow().inWholeMilliseconds

    @Test
    fun startTwiceAndAfterCloseFails() = contract { s ->
        val host = s.host()
        host.start(ContractDriver())
        assertFailsWith<IllegalStateException> { host.start(ContractDriver()) }
        assertTrue(host.close(), "$platform: close() дождался остановки")
        assertTrue(host.close(), "повторный close безвреден")
        val unused = s.host()
        assertTrue(unused.close(), "close без start")
        assertFailsWith<IllegalStateException> { unused.start(ContractDriver()) }
    }

    @Test
    fun closeStopsLoopAndReportsNullCause() = contract { s ->
        val driver = ContractDriver()
        val host = s.startedHost(driver)
        assertTrue(host.close())
        assertNull(withTimeout(1_000) { driver.stopped.await() }, "штатная остановка — cause = null")
        driver.clearRuns()
        host.wakeup()
        delay(100)
        assertTrue(driver.runs.tryReceive().isFailure, "после close() runOnce не вызывается")
    }

    /** H1 со стороны хоста: брокер закрыл поток → Eof, канал открыт до CloseTransport → сервер видит FIN, не RST. */
    @Test
    fun connectWriteReadEofAndCloseWithoutReset() = contract { s ->
        val server = s.server()
        val driver = ContractDriver()
        val host = s.startedHost(driver)
        driver.send(host, HostAction.Connect("localhost", server.port, 5_000))
        val peer = server.accept()
        assertEquals(TransportEvent.Connected, driver.awaitEvent())

        driver.send(host, HostAction.Write("hello".encodeToByteArray()))
        assertContentEquals("hello".encodeToByteArray(), peer.readExactly(5))

        peer.write("world".encodeToByteArray())
        val received = StringBuilder()
        while (received.length < 5) {
            received.append(assertIs<TransportEvent.Bytes>(driver.awaitEvent()).bytes.decodeToString())
        }
        assertEquals("world", received.toString())

        peer.shutdownOutput()
        assertEquals(TransportEvent.Eof, driver.awaitEvent())
        driver.send(host, HostAction.Write("tail".encodeToByteArray()))
        assertContentEquals("tail".encodeToByteArray(), peer.readExactly(4), "после Eof канал открыт на запись")
        driver.send(host, HostAction.CloseTransport)
        assertEquals(EndOfStream(PeerEnd.Fin, 0), peer.awaitEnd(), "$platform: штатный FIN (без Connection reset)")
    }

    @Test
    fun writeBeforeConnectedIsQueued() = contract { s ->
        val server = s.server()
        val driver = ContractDriver()
        val host = s.startedHost(driver)
        driver.commands.trySend(HostAction.Connect("127.0.0.1", server.port, 5_000))
        driver.commands.trySend(HostAction.Write("early".encodeToByteArray()))
        host.wakeup()
        val peer = server.accept()
        assertEquals(TransportEvent.Connected, driver.awaitEvent())
        assertContentEquals("early".encodeToByteArray(), peer.readExactly(5))
    }

    @Test
    fun connectRefusedIsIo() = contract { s ->
        val closed = newServer()
        val port = closed.port
        closed.close()
        val driver = ContractDriver()
        val host = s.startedHost(driver)
        driver.send(host, HostAction.Connect("127.0.0.1", port, 10_000))
        val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent(15_000))
        val io = assertIs<MqttError.Io>(failed.error, "$platform: отказ подключения — Io")
        assertTrue(io.kind.isNotEmpty())
        println("MqttHostContractTest[$platform]: отказ подключения — $io")
    }

    @Test
    fun unknownHostForInvalidName() = contract { s ->
        val driver = ContractDriver()
        val host = s.startedHost(driver)
        // Срок с запасом: резолвер Windows перебирает суффиксы поиска, NXDOMAIN может идти секунды
        driver.send(host, HostAction.Connect("nonexistent.invalid", 1883, 30_000))
        val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent(35_000))
        assertEquals(MqttError.UnknownHost, failed.error)
    }

    @Test
    fun replacedTransportEventsAreNotDelivered() = contract { s ->
        val server = s.server()
        val driver = ContractDriver()
        val host = s.startedHost(driver)
        driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
        val first = server.accept()
        assertEquals(TransportEvent.Connected, driver.awaitEvent())
        driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
        val second = server.accept()
        assertEquals(TransportEvent.Connected, driver.awaitEvent())
        assertEquals(PeerEnd.Fin, first.awaitEnd().end, "прежний транспорт закрыт новым Connect")
        second.write(byteArrayOf(7))
        assertContentEquals(byteArrayOf(7), assertIs<TransportEvent.Bytes>(driver.awaitEvent()).bytes)
        delay(100)
        assertEquals(emptyList(), driver.drainEvents(), "событий прежнего транспорта нет")
    }

    @Test
    fun partialWriteDoesNotBlockAndCompletesAfterPeerReads() = contract { s ->
        val server = s.server(smallReceiveBuffer = true)
        val probe = PendingProbe()
        val driver = ContractDriver()
        val pending = LongCell(-1L)
        driver.onRun = { pending.value = probe.pending() }
        val host = s.startedHost(driver, probe)
        driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
        val peer = server.accept()
        assertEquals(TransportEvent.Connected, driver.awaitEvent())

        val payload = ByteArray(bigPayload) { (it * 31 + 7).toByte() }
        driver.send(host, HostAction.Write(payload))
        delay(300)
        driver.clearRuns()
        host.wakeup()
        driver.awaitRun(1_000)
        if (writeBackpressureVisible) {
            assertTrue(pending.value > 0, "$platform: запись неполная (остаток ${pending.value}) — сервер не читает")
        } else {
            println("MqttHostContractTest[$platform]: остаток записи не виден (ОС приняла всё), проверяется доставка")
        }

        assertContentEquals(payload, peer.readExactly(payload.size, 30_000))
        val waitStart = TimeSource.Monotonic.markNow()
        while (pending.value != 0L && elapsedMs(waitStart) < 5_000) {
            host.wakeup()
            delay(10)
        }
        assertEquals(0L, pending.value, "очередь записи опустела")
    }

    /**
     * `Write(большой)` + `Write(DISCONNECT)` + `FlushAndClose` — запись частичная; `host.close()` во время дописывания
     * ждёт его; сервер получает всё, затем FIN (не RST). События отданного на дописывание транспорта не доставляются.
     */
    @Test
    fun flushAndCloseDeliversWholeTailThenFinAndHostCloseWaitsForIt() = contract { s ->
        val server = s.server(smallReceiveBuffer = true)
        val probe = PendingProbe()
        val driver = ContractDriver()
        val pending = LongCell(-1L)
        driver.onRun = { pending.value = probe.pending() }
        val host = s.startedHost(driver, probe)
        driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
        val peer = server.accept()
        assertEquals(TransportEvent.Connected, driver.awaitEvent())

        val payload = ByteArray(bigPayload) { (it * 31 + 7).toByte() }
        val disconnect = byteArrayOf(0xE0.toByte(), 0x00)
        driver.commands.trySend(HostAction.Write(payload))
        driver.commands.trySend(HostAction.Write(disconnect))
        driver.commands.trySend(HostAction.FlushAndClose(20_000))
        host.wakeup()
        delay(300)
        driver.clearRuns()
        host.wakeup()
        driver.awaitRun(1_000)
        if (writeBackpressureVisible) assertTrue(pending.value > 0, "$platform: запись частичная (остаток ${pending.value})")
        // Входящее во время дописывания (хост читает и отбрасывает). Если ОС приняла всю запись сразу, дописывания нет:
        // транспорт уже закрыт, и входящее после закрытия дало бы RST — так и должно быть, сценарий не о том
        if (writeBackpressureVisible) peer.write("late".encodeToByteArray())

        val closing = async { host.close(25_000) }
        delay(200)
        if (writeBackpressureVisible) assertFalse(closing.isCompleted, "host.close() ждёт дописывания")

        assertContentEquals(payload + disconnect, peer.readExactly(payload.size + disconnect.size, 30_000))
        assertEquals(EndOfStream(PeerEnd.Fin, 0), peer.awaitEnd(), "$platform: FIN после всего остатка")
        assertEquals(true, withTimeoutOrNull(5_000) { closing.await() }, "хост остановлен после дописывания")
        assertTrue(
            driver.drainEvents().none { it is TransportEvent.Bytes || it == TransportEvent.Eof },
            "события после FlushAndClose не доставляются"
        )
    }

    @Test
    fun flushAndCloseDeadlineClosesWithoutBlockingHostWhenPeerNeverReads() = contract { s ->
        if (!writeBackpressureVisible) {
            println("MqttHostContractTest[$platform]: срок дописывания не проверяется — остаток записи не виден")
            return@contract
        }
        val server = s.server(smallReceiveBuffer = true)
        val probe = PendingProbe()
        val driver = ContractDriver()
        val pending = LongCell(-1L)
        driver.onRun = { pending.value = probe.pending() }
        val host = s.startedHost(driver, probe)
        driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
        server.accept() // сервер не читает
        assertEquals(TransportEvent.Connected, driver.awaitEvent())

        driver.commands.trySend(HostAction.Write(ByteArray(bigPayload)))
        val t0 = TimeSource.Monotonic.markNow()
        driver.commands.trySend(HostAction.FlushAndClose(300))
        host.wakeup()
        delay(100)
        driver.clearRuns()
        host.wakeup()
        driver.awaitRun(1_000)
        assertTrue(pending.value > 0, "остаток висит")
        while (pending.value != 0L && elapsedMs(t0) < 5_000) {
            driver.clearRuns()
            host.wakeup()
            driver.awaitRun(1_000)
            delay(10)
        }
        val elapsed = elapsedMs(t0)
        assertEquals(0L, pending.value, "транспорт закрыт по сроку")
        assertTrue(elapsed in 250..3_000, "$platform: закрытие по сроку через $elapsed мс")
        val closeStart = TimeSource.Monotonic.markNow()
        assertTrue(host.close(5_000))
        assertTrue(elapsedMs(closeStart) < 1_000, "close() без дописывающихся — сразу")
    }

    @Test
    fun flushAndCloseWithEmptyQueueClosesAtOnce() = contract { s ->
        val server = s.server()
        val driver = ContractDriver()
        val host = s.startedHost(driver)
        driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
        val peer = server.accept()
        assertEquals(TransportEvent.Connected, driver.awaitEvent())
        driver.send(host, HostAction.Write("hi".encodeToByteArray()))
        assertContentEquals("hi".encodeToByteArray(), peer.readExactly(2))
        val t0 = TimeSource.Monotonic.markNow()
        driver.send(host, HostAction.FlushAndClose(10_000))
        assertEquals(EndOfStream(PeerEnd.Fin, 0), peer.awaitEnd(2_000), "FIN сразу")
        assertTrue(elapsedMs(t0) < 1_000)
        assertTrue(host.close(1_000))
    }

    @Test
    fun wakeupInterruptsWaitImmediately() = contract { s ->
        val driver = ContractDriver()
        driver.deadline = null
        val host = s.startedHost(driver)
        val latencies = ArrayList<Long>()
        repeat(60) {
            delay(3)
            driver.clearRuns()
            val t0 = TimeSource.Monotonic.markNow()
            host.wakeup()
            val t1 = driver.awaitRun(1_000)
            latencies += (t1 - t0).inWholeMicroseconds
        }
        val sorted = latencies.drop(10).sorted()
        val medianUs = sorted[sorted.size / 2]
        val p95Us = sorted[(sorted.size * 95) / 100]
        println("MqttHostContractTest[$platform]: wakeup → runOnce медиана $medianUs мкс, p95 $p95Us мкс, макс ${sorted.last()} мкс")
        assertTrue(medianUs < 5_000, "медиана wakeup → runOnce $medianUs мкс")
        assertTrue(p95Us < 20_000, "p95 wakeup → runOnce $p95Us мкс")
    }

    @Test
    fun driverDeadlineWakesHost() = contract { s ->
        val driver = ContractDriver()
        val host = s.startedHost(driver)
        driver.clearRuns()
        val t0 = TimeSource.Monotonic.markNow()
        driver.deadline = MqttClock.Monotonic.nowMs() + 100
        host.wakeup()
        driver.awaitRun(1_000)
        driver.deadline = null
        val next = driver.awaitRun(1_000)
        val elapsed = (next - t0).inWholeMilliseconds
        assertTrue(elapsed in 90..1_000, "$platform: проснулся по дедлайну через $elapsed мс")
    }

    @Test
    fun pauseReadStopsReadingUntilResumed() = contract { s ->
        val server = s.server()
        val driver = ContractDriver()
        val host = s.startedHost(driver)
        driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
        val peer = server.accept()
        assertEquals(TransportEvent.Connected, driver.awaitEvent())
        driver.send(host, HostAction.PauseRead(true))
        delay(50)
        peer.write(byteArrayOf(1, 2, 3))
        delay(200)
        assertEquals(emptyList(), driver.drainEvents(), "на паузе не читаем")
        driver.send(host, HostAction.PauseRead(false))
        val bytes = assertIs<TransportEvent.Bytes>(driver.awaitEvent())
        assertContentEquals(byteArrayOf(1, 2, 3), bytes.bytes)
    }

    @Test
    fun driverExceptionStopsHostAndReportsCause() = contract { s ->
        val driver = ContractDriver()
        val host = s.startedHost(driver)
        val boom = IllegalStateException("boom")
        driver.throwOnRun = boom
        host.wakeup()
        assertEquals<Throwable?>(boom, withTimeout(2_000) { driver.stopped.await() })
        assertTrue(host.close())
    }

    @Test
    fun startConnectCloseCyclesDoNotFail() = contract { s ->
        val server = s.server()
        val t0 = TimeSource.Monotonic.markNow()
        repeat(CYCLES) {
            val driver = ContractDriver()
            val host = newHost()
            host.start(driver)
            driver.send(host, HostAction.Connect("127.0.0.1", server.port, 5_000))
            val peer = server.accept()
            assertEquals(TransportEvent.Connected, driver.awaitEvent(), "цикл $it")
            assertTrue(host.close(), "close() дождался остановки, цикл $it")
            assertNull(withTimeout(1_000) { driver.stopped.await() })
            peer.close()
        }
        println("MqttHostContractTest[$platform]: $CYCLES циклов start/connect/close за ${elapsedMs(t0)} мс")
    }

    /**
     * Фасад на этом хосте (H1/K8): `MqttClient(options, host)` против сырого «брокера» — CONNECT → CONNACK, PUBLISH,
     * `close()` → DISCONNECT, брокер закрывает соединение, клиент — тоже, сервер видит FIN, а не RST.
     */
    @Test
    fun facadeConnectPublishCloseOverThisHost() = contract { s ->
        val server = s.server()
        val host = s.host()
        val options = MqttClientOptions(
            host = "127.0.0.1",
            port = server.port,
            version = MQTTVersion.MQTT3_1_1,
            clientId = "contract",
            keepAlive = 0.seconds,
            reconnect = ReconnectPolicy.Never,
        )
        val client = MqttClient(options, host)
        val connecting = async { client.connect() }
        val peer = server.accept()
        assertEquals(1, peer.readPacketType(), "CONNECT")
        peer.write(byteArrayOf(0x20, 0x02, 0x00, 0x00))
        assertIs<ConnectResult.Connected>(withTimeout(5_000) { connecting.await() })
        assertIs<PublishResult.Sent>(
            withTimeout(5_000) { client.publish("contract/t", "v".encodeToByteArray(), Qos.AT_MOST_ONCE) }
        )
        assertEquals(3, peer.readPacketType(), "PUBLISH")
        val closing = async { client.close() }
        assertEquals(14, peer.readPacketType(), "DISCONNECT")
        peer.shutdownOutput() // брокер закрывает соединение после DISCONNECT
        assertEquals(EndOfStream(PeerEnd.Fin, 0), peer.awaitEnd(), "$platform: FIN после DISCONNECT")
        withTimeout(10_000) { closing.await() }
        assertEquals(ConnectionState.Closed(null), client.connection.value)
    }

    /** Прочитать один пакет MQTT и вернуть его тип (старшие 4 бита фиксированного заголовка). */
    private suspend fun ContractPeer.readPacketType(): Int {
        val header = readExactly(1)[0].toInt() and 0xFF
        var length = 0
        var multiplier = 1
        while (true) {
            val b = readExactly(1)[0].toInt() and 0xFF
            length += (b and 0x7F) * multiplier
            if (b and 0x80 == 0) break
            multiplier *= 128
            if (multiplier > 128 * 128 * 128) fail("длина пакета больше 4 байт")
        }
        if (length > 0) readExactly(length)
        return header ushr 4
    }

    private companion object {
        const val CYCLES = 200
    }
}
