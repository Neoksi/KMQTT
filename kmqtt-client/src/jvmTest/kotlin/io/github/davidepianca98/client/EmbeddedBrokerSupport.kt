package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.BrokerScript
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.broker.Broker
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.socket.tls.TLSSettings
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import java.lang.management.ManagementFactory
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.net.SocketTimeoutException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.fail
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

// Общая обвязка интеграционных тестов фасада `MqttClient` против встроенного брокера форка (S06b-T05c, SEVEN-228):
// брокер в своём потоке (как `shared/jvmTest/.../EmbeddedBroker.kt`), сырой TCP-сервер для проверки EOF/RST,
// базовый класс с учётом клиентов/брокеров/прокси и проверкой утечек потоков `kmqtt-io-*`. Только jvmTest, loopback.

/**
 * Настоящий брокер MQTT из форка на 127.0.0.1 в своём потоке: `step()` и `stop()` вызываются из него же. После [stop]
 * порт свободен для нового брокера («рестарт»).
 *
 * @param maximumQos максимальный QoS брокера (`null` — 2).
 */
internal class TestBroker(
    val port: Int = freePort(),
    private val maximumQos: Qos? = null,
    private val tlsSettings: TLSSettings? = null,
    private val maximumPacketSize: UInt = DEFAULT_MAX_PACKET,
) : AutoCloseable {
    @Volatile
    private var stopRequested = false

    @Volatile
    private var serverReference: String? = null
    private val worker: Thread

    init {
        val broker = bind()
        worker = thread(name = "test-broker-$port", isDaemon = true) {
            while (!stopRequested) broker.step()
            broker.stop(serverReference = serverReference)
        }
    }

    /** Остановка: клиентам MQTT 5 уходит DISCONNECT (SERVER_SHUTTING_DOWN, с [serverReference] — SERVER_MOVED). */
    fun stop(serverReference: String? = null) {
        this.serverReference = serverReference
        stopRequested = true
        worker.join(STOP_TIMEOUT_MS)
    }

    override fun close() = stop()

    // На Windows порт только что остановленного брокера освобождается не сразу
    private fun bind(): Broker {
        val deadline = System.currentTimeMillis() + BIND_TIMEOUT_MS
        while (true) {
            try {
                return Broker(
                    port = port,
                    host = HOST,
                    maximumQos = maximumQos,
                    tlsSettings = tlsSettings,
                    maximumPacketSize = maximumPacketSize
                )
            } catch (e: Exception) {
                if (System.currentTimeMillis() > deadline) throw e
                Thread.sleep(100)
            }
        }
    }

    companion object {
        const val HOST = "127.0.0.1"

        /**
         * Умолчание брокера (32 КиБ) = и предел пакета, и размер буфера отправки на соединение. Встроенный брокер
         * при переполнении буфера отправки (`kmqtt-common` `Socket.send`: `allocate(capacity + size)` без копирования
         * ещё не отправленного остатка) теряет остаток медленному получателю, поэтому тестам с очередью к
         * медленному клиенту нужен больший буфер: [LARGE_BUFFER].
         */
        const val DEFAULT_MAX_PACKET = 32_768u
        const val LARGE_BUFFER = 8_388_608u
        private const val BIND_TIMEOUT_MS = 5_000L
        private const val STOP_TIMEOUT_MS = 5_000L

        fun freePort(): Int = ServerSocket(0).use { it.localPort }
    }
}

/** Что сырой сервер увидел вместо закрытия соединения клиентом. */
internal enum class RawEnd {
    /** `read() == -1`: клиент закрыл соединение штатно (FIN). */
    EOF,

    /** `Connection reset` на чтении или записи: клиент закрыл сокет с непрочитанными входящими (RST). */
    RESET,

    /** Клиент не закрыл соединение за `soTimeout` сервера. */
    TIMEOUT,

    /** Ошибка самого сервера (разбор пакета и т. п.): соединение закрыто сервером, причина — [RawConnectionEnd.cause]. */
    ERROR,

    /** Сервер закрыт тестом ([RawMqttServer.close]) при живом соединении. */
    CLOSED_BY_TEST,
}

/** Итог одного соединения сырого сервера ([index] — порядковый номер принятого соединения с нуля). */
internal class RawConnectionEnd(val index: Int, val end: RawEnd, val atMs: Long, val cause: Throwable?) {
    override fun toString(): String = "#$index $end" + (cause?.let { " (${it.javaClass.simpleName}: ${it.message})" } ?: "")
}

/**
 * Сырой MQTT-сервер без брокера: отвечает CONNACK, на каждый PUBLISH QoS 1 отвечает PUBACK через [pubackDelayMs]
 * (писатель в отдельном потоке — PUBACK приходят уже после DISCONNECT клиента). Получив DISCONNECT, ждёт, пока писатель
 * отдаст все PUBACK, и (если [closeAfterDisconnect]) закрывает свою сторону `shutdownOutput()` — FIN, не `close()`;
 * затем читает, пока клиент не закроет соединение: [end] — `EOF` или `RESET`.
 *
 * Соединения (SEVEN-260): по умолчанию принимается одно; с [acceptReconnect] — и последующие (переподключение клиента
 * после разрыва не упирается в закрытый сервер). [end], [endAtMs], [finished], [connected] относятся к **первому**
 * соединению, итоги всех — в [ends] с причиной: разрыв клиентом (`EOF`/`RESET`), `TIMEOUT`, ошибка сервера (`ERROR` с
 * исключением — раньше она молча обрывала соединение без итога) и закрытие тестом (`CLOSED_BY_TEST`). [describe] —
 * сводка для сообщений о провале.
 */
internal class RawMqttServer(
    private val version: MQTTVersion,
    private val pubackDelayMs: Long = 100,
    private val closeAfterDisconnect: Boolean = true,
    private val soTimeoutMs: Int = 15_000,
    private val acceptReconnect: Boolean = false,
) : AutoCloseable {
    private val server = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
    private val script = BrokerScript(version)

    @Volatile
    private var socket: Socket? = null

    @Volatile
    private var closing = false

    val port: Int get() = server.localPort

    /** Идентификаторы принятых PUBLISH по порядку. */
    val publishIds = CopyOnWriteArrayList<Int>()

    /** Типы принятых пакетов по порядку (старший полубайт первого байта: 1 CONNECT, 3 PUBLISH, 14 DISCONNECT…). */
    val packetTypes = CopyOnWriteArrayList<Int>()

    /** Сколько соединений принято. */
    val connections = AtomicInteger()

    /** Итоги закончившихся соединений по порядку. */
    val ends = CopyOnWriteArrayList<RawConnectionEnd>()

    /** Итог первого соединения. */
    @Volatile
    var end: RawEnd? = null
        private set

    @Volatile
    var endAtMs: Long = 0
        private set

    @Volatile
    var disconnectAtMs: Long = 0
        private set

    /** Последняя ошибка записи PUBACK/PINGRESP (любого соединения). */
    @Volatile
    var writeFailure: Throwable? = null
        private set

    /** Первый CONNECT. */
    val connected = CountDownLatch(1)

    /** Первое соединение закончилось. */
    val finished = CountDownLatch(1)
    private val writeLock = Any()

    /** Сколько байт [push] уже записал в сокет (блокирующая запись — растёт, пока клиент читает). */
    val pushedBytes = java.util.concurrent.atomic.AtomicLong()

    /** Сколько PUBACK клиента получено (клиент подтверждает входящие QoS 1). */
    val receivedAcks = AtomicInteger()

    val pushDone = CountDownLatch(1)

    /** Состояние одного соединения: очередь ответов писателя и неотданные PUBACK — свои у каждого. */
    private class Connection(val index: Int, val client: Socket) {
        val outbox = LinkedBlockingQueue<Pair<Long, ByteArray>>()
        val pendingAcks = AtomicInteger()

        @Volatile
        var writeFailure: Throwable? = null
    }

    /**
     * Сервер как источник входящих: [count] PUBLISH QoS 1 по [payloadSize] байт блокирующей записью (после CONNACK) в
     * текущее соединение; первые два байта нагрузки — номер сообщения. Записывает столько, сколько клиент успевает читать.
     */
    fun push(count: Int, payloadSize: Int, topic: String = "push/x") {
        check(connected.await(10, TimeUnit.SECONDS)) { "клиент не подключился" }
        val out = checkNotNull(socket).getOutputStream()
        thread(name = "raw-mqtt-push-$port", isDaemon = true) {
            try {
                for (i in 0 until count) {
                    val payload = ByteArray(payloadSize).also {
                        it[0] = i.toByte()
                        it[1] = (i shr 8).toByte()
                    }
                    val bytes = script.publish(topic, payload, Qos.AT_LEAST_ONCE, packetId = i % 65_000 + 1)
                    synchronized(writeLock) {
                        out.write(bytes)
                        out.flush()
                    }
                    pushedBytes.addAndGet(bytes.size.toLong())
                }
            } catch (_: Exception) {
                // клиент закрылся
            } finally {
                pushDone.countDown()
            }
        }
    }

    private val acceptor = thread(name = "raw-mqtt-server-$port", isDaemon = true) {
        try {
            var index = 0
            do {
                val client = server.accept()
                serve(Connection(index++, client))
            } while (acceptReconnect && !closing)
        } catch (_: Throwable) {
            // сервер закрыт тестом (accept)
        } finally {
            finished.countDown()
        }
    }

    private fun serve(connection: Connection) {
        val client = connection.client
        socket = client
        connections.incrementAndGet()
        client.tcpNoDelay = true
        client.soTimeout = soTimeoutMs
        val writer = thread(name = "raw-mqtt-writer-$port-${connection.index}", isDaemon = true) { writeLoop(connection) }
        val input = client.getInputStream()
        val buffer = ByteArray(16 * 1024)
        var pending = ByteArray(0)
        try {
            while (true) {
                val read = input.read(buffer)
                if (read < 0) {
                    markEnd(connection, RawEnd.EOF, null)
                    break
                }
                pending += buffer.copyOf(read)
                while (true) {
                    val packet = takePacket(pending) ?: break
                    pending = pending.copyOfRange(packet.size, pending.size)
                    onPacket(connection, packet)
                }
            }
        } catch (e: SocketTimeoutException) {
            markEnd(connection, RawEnd.TIMEOUT, e)
        } catch (e: SocketException) {
            markEnd(connection, if (closing) RawEnd.CLOSED_BY_TEST else RawEnd.RESET, e)
        } catch (e: Exception) {
            markEnd(connection, RawEnd.ERROR, e)
        } finally {
            writer.interrupt()
            runCatching { client.close() }
            if (connection.index == 0) finished.countDown()
        }
    }

    private fun markEnd(connection: Connection, value: RawEnd, cause: Throwable?) {
        // Ошибка записи PUBACK в закрытый клиентом сокет — тот же RST, даже если чтение успело увидеть FIN
        val failure = connection.writeFailure
        val result = if (failure != null && value == RawEnd.EOF) RawEnd.RESET else value
        val at = System.currentTimeMillis()
        if (connection.index == 0) {
            end = result
            endAtMs = at
        }
        ends += RawConnectionEnd(connection.index, result, at, cause ?: failure)
    }

    private fun onPacket(connection: Connection, packet: ByteArray) {
        val client = connection.client
        val type = (packet[0].toInt() and 0xFF) shr 4
        packetTypes += type
        when (type) {
            TYPE_CONNECT -> {
                synchronized(writeLock) { client.getOutputStream().apply { write(script.connack()); flush() } }
                connected.countDown()
            }
            TYPE_PUBLISH -> {
                val qos = (packet[0].toInt() shr 1) and 3
                if (qos > 0) {
                    var index = 1
                    while (packet[index++].toInt() and 0x80 != 0) Unit // длина
                    val topicLength = ((packet[index].toInt() and 0xFF) shl 8) or (packet[index + 1].toInt() and 0xFF)
                    index += 2 + topicLength
                    val id = ((packet[index].toInt() and 0xFF) shl 8) or (packet[index + 1].toInt() and 0xFF)
                    publishIds += id
                    connection.pendingAcks.incrementAndGet()
                    connection.outbox.put((System.currentTimeMillis() + pubackDelayMs) to script.puback(id))
                }
            }
            TYPE_PUBACK -> receivedAcks.incrementAndGet()
            TYPE_PINGREQ -> connection.outbox.put(0L to script.pingresp())
            TYPE_DISCONNECT -> {
                disconnectAtMs = System.currentTimeMillis()
                val deadline = System.currentTimeMillis() + 5_000
                while (connection.pendingAcks.get() > 0 && System.currentTimeMillis() < deadline) Thread.sleep(5)
                if (closeAfterDisconnect) runCatching { client.shutdownOutput() }
            }
        }
    }

    private fun writeLoop(connection: Connection) {
        try {
            val out = connection.client.getOutputStream()
            while (true) {
                val (due, bytes) = connection.outbox.take()
                val wait = due - System.currentTimeMillis()
                if (wait > 0) Thread.sleep(wait)
                try {
                    synchronized(writeLock) {
                        out.write(bytes)
                        out.flush()
                    }
                } catch (e: Exception) {
                    connection.writeFailure = e
                    writeFailure = e
                } finally {
                    if (due != 0L) connection.pendingAcks.decrementAndGet()
                }
            }
        } catch (_: InterruptedException) {
            // конец соединения
        }
    }

    /** Ждёт, пока сервер увидит конец первого соединения. */
    fun awaitEnd(timeoutMs: Long): RawEnd = if (finished.await(timeoutMs, TimeUnit.MILLISECONDS)) {
        end ?: fail("сервер завершился без итога чтения; ${describe()}")
    } else {
        fail("сервер не увидел конца соединения за $timeoutMs мс; ${describe()}")
    }

    /** Сводка для сообщений о провале: соединения, их итоги, пакеты, ошибки записи. */
    fun describe(): String =
        "соединений ${connections.get()}, итоги $ends, пакеты $packetTypes, ошибка записи ${writeFailure?.javaClass?.simpleName}"

    override fun close() {
        closing = true
        runCatching { server.close() }
        runCatching { socket?.close() }
        acceptor.join(2_000)
    }

    private fun takePacket(bytes: ByteArray): ByteArray? {
        if (bytes.size < 2) return null
        var length = 0
        var shift = 0
        var index = 1
        while (true) {
            if (index >= bytes.size) return null
            val byte = bytes[index++].toInt() and 0xFF
            length = length or ((byte and 0x7F) shl shift)
            if (byte and 0x80 == 0) break
            shift += 7
        }
        val total = index + length
        return if (bytes.size < total) null else bytes.copyOfRange(0, total)
    }

    private companion object {
        const val TYPE_CONNECT = 1
        const val TYPE_PUBLISH = 3
        const val TYPE_PUBACK = 4
        const val TYPE_PINGREQ = 12
        const val TYPE_DISCONNECT = 14
    }
}

/** Накопитель входящих клиента: один сборщик `incoming` (опциональная пауза на элемент — «медленный потребитель»). */
internal class Collector(client: MqttClient, scope: CoroutineScope, perItemDelayMs: Long = 0) {
    val items = CopyOnWriteArrayList<MqttIncoming>()
    val messages: List<MqttIncoming.Message> get() = items.filterIsInstance<MqttIncoming.Message>()

    private val job = scope.launch {
        client.incoming.collect {
            items += it
            if (perItemDelayMs > 0) delay(perItemDelayMs)
        }
    }

    fun stop() = job.cancel()
}

/** Базовый класс: реестр клиентов, брокеров, прокси и проверка утечек потоков хоста после теста. */
internal abstract class BrokerTestBase {
    protected val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val clients = CopyOnWriteArrayList<MqttClient>()
    private val closeables = CopyOnWriteArrayList<AutoCloseable>()
    private var threadsBefore = 0
    private val clientCounter = AtomicInteger()

    @BeforeTest
    fun baseSetUp() {
        threadsBefore = BrokerTestKit.ioThreads()
    }

    @AfterTest
    fun baseTearDown() {
        runBlocking {
            clients.reversed().forEach { client ->
                withTimeoutOrNull(15_000) { runCatching { client.close() } }
                    ?: println("BrokerTestBase: client.close() не уложился в 15 с")
            }
        }
        scope.cancel()
        closeables.reversed().forEach { runCatching { it.close() } }
        BrokerTestKit.waitUntil(5_000, "потоки kmqtt-io вернулись к исходному числу ($threadsBefore)") {
            BrokerTestKit.ioThreads() <= threadsBefore
        }
    }

    protected fun <T : AutoCloseable> track(item: T): T = item.also { closeables += it }

    protected fun broker(
        maximumQos: Qos? = null,
        tlsSettings: TLSSettings? = null,
        maximumPacketSize: UInt = TestBroker.DEFAULT_MAX_PACKET,
    ) = track(TestBroker(maximumQos = maximumQos, tlsSettings = tlsSettings, maximumPacketSize = maximumPacketSize))

    protected fun proxy(port: Int) = track(TcpProxy(TestBroker.HOST, port))

    /** Параметры клиента: быстрый реконнект 100–500 мс без джиттера, чтобы тесты были короткими. */
    protected fun options(
        port: Int,
        version: MQTTVersion = MQTTVersion.MQTT5,
        tune: (MqttClientOptions) -> MqttClientOptions = { it },
    ): MqttClientOptions = tune(
        MqttClientOptions(
            host = TestBroker.HOST,
            port = port,
            version = version,
            clientId = "t05c-${clientCounter.incrementAndGet()}-${System.nanoTime() % 100_000}",
            keepAlive = 30.seconds,
            connectTimeout = 5.seconds,
            connackTimeout = 5.seconds,
            reconnect = ReconnectPolicy(initial = 100.milliseconds, max = 500.milliseconds, jitter = 0.0),
        )
    )

    /** Клиент на `NioActorHost` (TCP); закрывается в конце теста. */
    protected fun client(
        port: Int,
        version: MQTTVersion = MQTTVersion.MQTT5,
        tune: (MqttClientOptions) -> MqttClientOptions = { it },
    ): MqttClient = MqttClient(options(port, version, tune), NioActorHost()).also { clients += it }

    protected fun collect(client: MqttClient, perItemDelayMs: Long = 0) = Collector(client, scope, perItemDelayMs)

    protected fun trackClient(client: MqttClient): MqttClient = client.also { clients += it }
}

internal object BrokerTestKit {
    fun waitUntil(timeoutMs: Long, what: String, condition: () -> Boolean) = waitUntil(timeoutMs, { what }, condition)

    /** То же, но описание считается в момент провала (например, «сколько принято» — на момент таймаута, не начала). */
    fun waitUntil(timeoutMs: Long, what: () -> String, condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (!condition()) {
            if (System.currentTimeMillis() > deadline) fail("timeout $timeoutMs ms: ${what()}")
            Thread.sleep(5)
        }
    }

    /** Живые потоки хостов `kmqtt-io-*` (по имени). */
    fun ioThreads(): Int {
        val bean = ManagementFactory.getThreadMXBean()
        return bean.getThreadInfo(bean.allThreadIds).count {
            it != null && it.threadName.startsWith(NioActorHost.THREAD_NAME_PREFIX)
        }
    }

    /** Открытые дескрипторы процесса; `null` на Windows (нет `UnixOperatingSystemMXBean`). */
    fun openFds(): Long? =
        (ManagementFactory.getOperatingSystemMXBean() as? com.sun.management.UnixOperatingSystemMXBean)
            ?.openFileDescriptorCount

    /** Занятая куча после трёх `System.gc()`. */
    fun usedHeap(): Long {
        val runtime = Runtime.getRuntime()
        repeat(3) {
            System.gc()
            Thread.sleep(50)
        }
        return runtime.totalMemory() - runtime.freeMemory()
    }
}
