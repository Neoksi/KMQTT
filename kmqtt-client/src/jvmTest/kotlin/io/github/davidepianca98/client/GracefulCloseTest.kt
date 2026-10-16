package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.net.Socket
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration.Companion.seconds

/**
 * Закрытие без RST (H1/H2/K8, регрессия SEVEN-216) на настоящем сокете: после `close()` брокер не должен видеть потерю
 * хвоста. Клиент слал DISCONNECT и сразу закрывал сокет, пока в его буфере чтения лежали непрочитанные ответы брокера:
 * ОС отправляла RST, брокер терял хвост очереди и публиковал will. Новый клиент после DISCONNECT читает и отбрасывает
 * входящее, пока сервер не закроет соединение (или не истечёт `closeLinger`), и только потом закрывает сокет.
 */
internal class GracefulCloseTest : BrokerTestBase() {

    private fun <T> timed(limitMs: Long = 30_000, block: suspend CoroutineScope.() -> T): T =
        runBlocking { withTimeout(limitMs, block) }

    private suspend fun MqttClient.up(): ConnectResult.Connected =
        withTimeout(10_000) { connect() } as? ConnectResult.Connected ?: fail("клиент не подключился")

    private fun rawOptions(options: MqttClientOptions) = options.copy(keepAlive = 0.seconds)

    private suspend fun publishFifty(client: MqttClient) = repeat(PUBLISHES) {
        assertIs<PublishResult.Sent>(client.publish("raw/$it", byteArrayOf(it.toByte()), Qos.AT_LEAST_ONCE))
    }

    /** 50 PUBLISH QoS 1 + `close()`; PUBACK приходят через 100 мс — уже после DISCONNECT клиента. */
    private fun rawServerSeesEof(version: MQTTVersion, closeLinger: kotlin.time.Duration) = timed {
        // `closeLinger = 0`: клиент не ждёт закрытия брокером; пока PUBACK ещё в пути, RST допустим (известный предел,
        // SEVEN-252), но все PUBLISH и DISCONNECT должны дойти целиком
        val zeroLinger = closeLinger == 0.seconds
        val server = track(RawMqttServer(version, pubackDelayMs = 100, closeAfterDisconnect = true))
        val client = client(server.port, version) { rawOptions(it).copy(closeLinger = closeLinger) }
        client.up()
        publishFifty(client)
        val started = System.nanoTime()
        withTimeout(10_000) { client.close() }
        val closeMs = (System.nanoTime() - started) / 1_000_000
        println("GracefulCloseTest.rawEof($version, linger=$closeLinger): close() занял $closeMs мс")

        val end = server.awaitEnd(10_000)
        if (zeroLinger) {
            assertTrue(end == RawEnd.EOF || end == RawEnd.RESET, "конец соединения: $end")
        } else {
            assertEquals(RawEnd.EOF, end, "сервер увидел RST вместо EOF; пакеты ${server.packetTypes}")
            assertNull(server.writeFailure, "запись PUBACK в закрытый клиентом сокет: ${server.writeFailure}")
        }
        assertEquals(PUBLISHES, server.publishIds.size, "сервер получил все PUBLISH")
        // CONNECT, 50 PUBLISH, DISCONNECT — и ничего после
        assertEquals(listOf(1) + List(PUBLISHES) { 3 } + listOf(14), server.packetTypes.toList())
        assertEquals(ConnectionState.Closed(null), client.connection.value)
    }

    @Test
    fun serverReadsEofNotResetAfterFiftyPublishesMqtt5() = rawServerSeesEof(MQTTVersion.MQTT5, 3.seconds)

    @Test
    fun serverReadsEofNotResetAfterFiftyPublishesMqtt311() = rawServerSeesEof(MQTTVersion.MQTT3_1_1, 3.seconds)

    /**
     * `closeLinger = 0` (SEVEN-252: хост дописывает остаток записи и закрывает канал, не дожидаясь брокера): все
     * PUBLISH и DISCONNECT доходят целиком; конец соединения — EOF или RST (PUBACK ещё в пути — известный предел).
     */
    @Test
    fun serverReadsEofWithZeroLingerWhenServerClosesMqtt5() = rawServerSeesEof(MQTTVersion.MQTT5, 0.seconds)

    @Test
    fun serverReadsEofWithZeroLingerWhenServerClosesMqtt311() = rawServerSeesEof(MQTTVersion.MQTT3_1_1, 0.seconds)

    /** Сервер не закрывает соединение: клиент закрывает сам через `closeLinger` (3 с), без RST. */
    private fun lingerWhenServerStaysSilent(version: MQTTVersion) = timed {
        val server = track(RawMqttServer(version, pubackDelayMs = 100, closeAfterDisconnect = false))
        val client = client(server.port, version) { rawOptions(it).copy(closeLinger = 3.seconds) }
        client.up()
        publishFifty(client)
        val started = System.nanoTime()
        withTimeout(15_000) { client.close() }
        val closeMs = (System.nanoTime() - started) / 1_000_000
        println("GracefulCloseTest.linger($version): close() занял $closeMs мс")
        assertTrue(closeMs in 2_900..3_500, "close() ждёт closeLinger 3 с (ожидалось 3000–3500): $closeMs мс")

        assertEquals(RawEnd.EOF, server.awaitEnd(10_000), "RST вместо EOF после linger; пакеты ${server.packetTypes}")
        assertNull(server.writeFailure)
        assertEquals(PUBLISHES, server.publishIds.size)
        assertEquals(14, server.packetTypes.last(), "DISCONNECT — последний пакет")
    }

    @Test
    fun clientClosesItselfAfterCloseLingerWhenServerStaysSilentMqtt5() = lingerWhenServerStaysSilent(MQTTVersion.MQTT5)

    @Test
    fun clientClosesItselfAfterCloseLingerWhenServerStaysSilentMqtt311() =
        lingerWhenServerStaysSilent(MQTTVersion.MQTT3_1_1)

    /**
     * `closeLinger = 0` и молчащий сервер: клиент придерживает закрытие не дольше 500 мс (чтение включено), PUBACK
     * (100 мс) к этому времени прочитаны — RST нет.
     */
    @Test
    fun zeroLingerWithSilentServerStillReadsAcksBeforeClosing() = timed {
        val server = track(RawMqttServer(MQTTVersion.MQTT5, pubackDelayMs = 100, closeAfterDisconnect = false))
        val client = client(server.port) { rawOptions(it).copy(closeLinger = 0.seconds) }
        client.up()
        publishFifty(client)
        val started = System.nanoTime()
        withTimeout(10_000) { client.close() }
        println("GracefulCloseTest.zeroLingerSilent: close() занял ${(System.nanoTime() - started) / 1_000_000} мс")
        val end = server.awaitEnd(10_000)
        assertTrue(end == RawEnd.EOF || end == RawEnd.RESET, "конец соединения: $end; пакеты ${server.packetTypes}")
        assertEquals(PUBLISHES, server.publishIds.size)
        assertEquals(14, server.packetTypes.last(), "DISCONNECT — последний пакет")
    }

    /**
     * Контроль самого стенда: «наивный» клиент (DISCONNECT и немедленный `Socket.close()` при ещё не пришедших
     * PUBACK) приводит сервер к RST. Без этого контроля зелёный тест выше ничего бы не доказывал.
     */
    @Test
    fun controlNaiveCloseIsDetectedAsReset() = timed {
        val server = track(RawMqttServer(MQTTVersion.MQTT3_1_1, pubackDelayMs = 100, closeAfterDisconnect = true))
        Socket("127.0.0.1", server.port).use { socket ->
            val out = socket.getOutputStream()
            out.write(byteArrayOf(0x10, 15, 0, 4, 'M'.code.toByte(), 'Q'.code.toByte(), 'T'.code.toByte(), 'T'.code.toByte(), 4, 2, 0, 0, 0, 3) + "raw".encodeToByteArray())
            out.flush()
            socket.getInputStream().readNBytes(4) // CONNACK
            repeat(PUBLISHES) {
                val topic = "raw/$it".encodeToByteArray()
                out.write(byteArrayOf(0x32, (2 + topic.size + 2 + 1).toByte(), 0, topic.size.toByte()) + topic + byteArrayOf((it shr 8).toByte(), (it + 1).toByte(), 1))
            }
            out.write(byteArrayOf(0xE0.toByte(), 0))
            out.flush()
        } // close() сразу после DISCONNECT, PUBACK ещё в пути
        server.awaitEnd(10_000)
        assertTrue(
            server.end == RawEnd.RESET || server.writeFailure != null,
            "сервер не заметил обрыв наивного клиента: end=${server.end}, запись=${server.writeFailure}"
        )
    }

    // ---- порядок «публикации остановки → DISCONNECT» (H2) на настоящем брокере ----

    private fun stopPublicationsReachBrokerAndNoWill(version: MQTTVersion) = timed {
        val broker = broker()
        val observer = client(broker.port, version)
        val inbox = collect(observer)
        observer.up()
        observer.subscriptions.set("g", setOf("h2/#"), Qos.AT_LEAST_ONCE)
        withTimeout(10_000) { observer.subscriptions.awaitApplied("g") }

        val stopping = client(broker.port, version) {
            it.copy(will = MqttWill("h2/will", "gone".encodeToByteArray(), retain = false, qos = Qos.AT_MOST_ONCE))
        }
        stopping.up()
        repeat(PUBLISHES) { stopping.publish("h2/seq", it.toString().encodeToByteArray(), Qos.AT_LEAST_ONCE) }
        withTimeout(10_000) { stopping.close() }

        BrokerTestKit.waitUntil(10_000, "all stop publications, now ${inbox.messages.size}") {
            inbox.messages.count { it.topic == "h2/seq" } >= PUBLISHES
        }
        val marker = client(broker.port, version)
        marker.up()
        marker.publish("h2/marker", ByteArray(0), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        BrokerTestKit.waitUntil(10_000, "marker") { inbox.messages.any { it.topic == "h2/marker" } }
        assertEquals(
            (0 until PUBLISHES).toList(),
            inbox.messages.filter { it.topic == "h2/seq" }.map { it.payload.decodeToString().toInt() },
            "публикации остановки дошли все и по порядку"
        )
        assertTrue(inbox.messages.none { it.topic == "h2/will" }, "will после штатного close() не публикуется")
    }

    @Test
    fun stopPublicationsReachBrokerBeforeDisconnectAndWillIsNotPublishedMqtt5() =
        stopPublicationsReachBrokerAndNoWill(MQTTVersion.MQTT5)

    @Test
    fun stopPublicationsReachBrokerBeforeDisconnectAndWillIsNotPublishedMqtt311() =
        stopPublicationsReachBrokerAndNoWill(MQTTVersion.MQTT3_1_1)

    // ---- SEVEN-216: 25 циклов «включить -> выключить с очисткой», клиент подключён напрямую ----

    /**
     * Цикл: устройство публикует retained по 16 топикам (подключение A, штатное закрытие); затем «выключение с
     * очисткой» — пустые retained по всем топикам (`until = Sent`, чтобы хвост был под угрозой) и `close()` (B, с will
     * `retain = true`). Новый подписчик C после SUBACK получает retained брокера: не должно прийти ни одного топика
     * устройства и ни одного will; метку C публикует сам уже после SUBACK, поэтому она приходит после retained.
     */
    private fun clearRetainedCycles(version: MQTTVersion) = timed(60_000) {
        val broker = broker()
        val topics = List(DEVICE_TOPICS) { "dev/controls/c$it" } + "dev/meta"
        val started = System.currentTimeMillis()
        repeat(CYCLES) { cycle ->
            val on = client(broker.port, version)
            on.up()
            for (topic in topics) on.publish(topic, "v$cycle".encodeToByteArray(), Qos.AT_LEAST_ONCE, retain = true)
            withTimeout(10_000) { on.close() }

            val off = client(broker.port, version) {
                it.copy(will = MqttWill("dev/meta/error", "r".encodeToByteArray(), retain = true, qos = Qos.AT_MOST_ONCE))
            }
            off.up()
            for (topic in topics) off.publish(topic, ByteArray(0), Qos.AT_LEAST_ONCE, retain = true)
            withTimeout(10_000) { off.close() }

            val fresh = client(broker.port, version)
            val inbox = collect(fresh)
            fresh.up()
            fresh.subscriptions.set("g", setOf("dev/#", "mark/#"), Qos.AT_LEAST_ONCE)
            withTimeout(10_000) { fresh.subscriptions.awaitApplied("g") }
            fresh.publish("mark/m", ByteArray(0), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
            BrokerTestKit.waitUntil(10_000, "cycle $cycle: marker") { inbox.messages.any { it.topic == "mark/m" } }
            val leftovers = inbox.messages.filter { it.topic != "mark/m" }
            assertTrue(
                leftovers.isEmpty(),
                "цикл $cycle: на брокере остались топики устройства или will: ${leftovers.map { it.topic + "=" + it.payload.decodeToString() }}"
            )
            fresh.close()
        }
        println("GracefulCloseTest.cycles($version): $CYCLES циклов за ${System.currentTimeMillis() - started} мс")
    }

    @Test
    fun clearRetainedCyclesLeaveNothingOnBrokerMqtt5() = clearRetainedCycles(MQTTVersion.MQTT5)

    @Test
    fun clearRetainedCyclesLeaveNothingOnBrokerMqtt311() = clearRetainedCycles(MQTTVersion.MQTT3_1_1)

    private companion object {
        const val PUBLISHES = 50
        const val CYCLES = 25
        const val DEVICE_TOPICS = 16
    }
}
