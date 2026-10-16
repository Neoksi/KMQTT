package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration.Companion.seconds

/**
 * Интеграционные тесты фасада [MqttClient] против встроенного брокера форка (S06b-T05c, SEVEN-228): порядок,
 * обратное давление, исходы публикаций, эпохи при рестарте брокера, will (H3). Реальное время и сокеты на loopback;
 * каждый тест укладывается в 30 с.
 */
internal class MqttClientBrokerTest : BrokerTestBase() {

    private fun <T> timed(block: suspend CoroutineScope.() -> T): T = runBlocking { withTimeout(30_000, block) }

    private suspend fun MqttClient.up(): ConnectResult.Connected =
        withTimeout(10_000) { connect() } as? ConnectResult.Connected ?: fail("клиент не подключился")

    private suspend fun MqttClient.subscribed(group: String, vararg filters: String, qos: Qos = Qos.AT_LEAST_ONCE) {
        subscriptions.set(group, filters.toSet(), qos)
        withTimeout(10_000) { subscriptions.awaitApplied(group) }
    }

    // ---- порядок ----

    private fun orderedThousand(version: MQTTVersion, qos: Qos) = timed {
        val broker = broker()
        val sub = client(broker.port, version)
        val inbox = collect(sub)
        val epoch = sub.up().epoch
        sub.subscribed("g", "order/#")
        val pub = client(broker.port, version)
        pub.up()

        repeat(COUNT_1000) {
            assertIs<PublishResult.Sent>(pub.publish("order/x", it.toString().encodeToByteArray(), qos))
        }
        pub.publish("order/end", ByteArray(0), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        BrokerTestKit.waitUntil(10_000, "конец потока 1000 сообщений") { inbox.messages.any { it.topic == "order/end" } }

        assertEquals(MqttIncoming.SessionStarted(epoch), inbox.items.first())
        val numbers = inbox.messages.filter { it.topic == "order/x" }.map { it.payload.decodeToString().toInt() }
        assertEquals((0 until COUNT_1000).toList(), numbers, "порядок и ни одного пропуска")
        assertTrue(inbox.messages.all { it.epoch == epoch })
        assertEquals(COUNT_1000 + 1, inbox.messages.size, "ни одного лишнего сообщения")
    }

    @Test
    fun thousandMessagesKeepOrderQos1Mqtt5() = orderedThousand(MQTTVersion.MQTT5, Qos.AT_LEAST_ONCE)

    @Test
    fun thousandMessagesKeepOrderQos1Mqtt311() = orderedThousand(MQTTVersion.MQTT3_1_1, Qos.AT_LEAST_ONCE)

    @Test
    fun thousandMessagesKeepOrderQos0Mqtt5() = orderedThousand(MQTTVersion.MQTT5, Qos.AT_MOST_ONCE)

    @Test
    fun thousandMessagesKeepOrderQos0Mqtt311() = orderedThousand(MQTTVersion.MQTT3_1_1, Qos.AT_MOST_ONCE)

    // ---- обратное давление ----

    /** Номер сообщения из первых двух байт нагрузки. */
    private fun sequence(message: MqttIncoming.Message) =
        (message.payload[0].toInt() and 0xFF) or ((message.payload[1].toInt() and 0xFF) shl 8)

    private fun numbered(i: Int, size: Int) = ByteArray(size).also {
        it[0] = i.toByte()
        it[1] = (i shr 8).toByte()
    }

    /** Медленный потребитель: элемент раз в 5 мс (блокирующий сон — на Windows `delay(5)` округляется до ~15 мс). */
    private fun slowConsumer(client: MqttClient, seen: MutableList<Int>) = scope.launch {
        client.incoming.collect {
            if (it is MqttIncoming.Message) {
                seen += sequence(it)
                Thread.sleep(5)
            }
        }
    }

    private fun assertHeapBounded(before: Long, label: String) {
        val after = BrokerTestKit.usedHeap()
        println("MqttClientBrokerTest.$label: куча до ${before / 1024} КиБ, после ${after / 1024} КиБ")
        assertTrue(after - before < 2 * 1024 * 1024, "$label: рост кучи ${(after - before) / 1024} КиБ")
    }

    /**
     * Медленный потребитель (`incomingBuffer = 16`, элемент раз в 5 мс) при 2 000 публикациях QoS 1 через брокер форка:
     * ни одной потери, порядок сохранён, куча после GC не растёт больше чем на 2 МиБ. Нагрузка 1 КиБ и буфер отправки
     * брокера 8 МиБ: брокер форка теряет остаток очереди медленному получателю при переполнении буфера отправки
     * (32 КиБ по умолчанию) — дефект брокера, не клиента, см. [TestBroker.DEFAULT_MAX_PACKET].
     */
    @Test
    fun slowConsumerLosesNothingThroughBroker() = timed {
        val broker = broker(maximumPacketSize = TestBroker.LARGE_BUFFER)
        val sub = client(broker.port) { it.copy(limits = MqttLimits(incomingBuffer = 16)) }
        val seen = CopyOnWriteArrayList<Int>()
        slowConsumer(sub, seen)
        sub.up()
        sub.subscribed("g", "bp/#")
        val pub = client(broker.port)
        pub.up()

        // прогрев: JIT, буферы, первые соединения
        repeat(WARMUP) { pub.publish("bp/x", numbered(it, 1024), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered) }
        BrokerTestKit.waitUntil(10_000, "warmup delivered, now ${seen.size}") { seen.size == WARMUP }

        // Два раунда по 1 000 (всего 2 000 публикаций): утечка видна как рост кучи между раундами; разовый рост от
        // брокера в том же процессе (его структуры, классы) в сравнение не входит
        val heaps = ArrayList<Long>()
        val started = System.currentTimeMillis()
        repeat(2) { round ->
            seen.clear()
            repeat(TOTAL_BP / 2) { pub.publish("bp/x", numbered(it, 1024), Qos.AT_LEAST_ONCE) }
            BrokerTestKit.waitUntil(25_000, "round $round: consumer got all, now ${seen.size}") { seen.size >= TOTAL_BP / 2 }
            assertEquals((0 until TOTAL_BP / 2).toList(), seen.toList(), "раунд $round: без потерь и в порядке публикации")
            heaps += BrokerTestKit.usedHeap()
        }
        println("MqttClientBrokerTest.backpressure(broker): $TOTAL_BP сообщений за ${System.currentTimeMillis() - started} мс")
        println("MqttClientBrokerTest.backpressure(broker): куча после раундов ${heaps.map { it / 1024 }} КиБ")
        assertTrue(heaps[1] - heaps[0] < 2 * 1024 * 1024, "рост кучи между раундами ${(heaps[1] - heaps[0]) / 1024} КиБ")
    }

    /**
     * То же давление, но источник — сырой сервер с блокирующей записью 2 000 PUBLISH QoS 1 по 16 КиБ (32 МиБ): пока
     * потребитель прочитал ~100 сообщений, сервер не мог записать больше половины (клиент перестал читать сокет, и
     * давление дошло до отправителя через TCP); в конце потеряно ничего, порядок сохранён, клиент подтвердил PUBACK
     * на каждое сообщение, куча не растёт.
     */
    @Test
    fun slowConsumerBlocksTheSenderAndLosesNothing() = timed {
        val server = track(RawMqttServer(MQTTVersion.MQTT5, pubackDelayMs = 0, closeAfterDisconnect = true))
        val sub = client(server.port) { it.copy(limits = MqttLimits(incomingBuffer = 16), keepAlive = 0.seconds) }
        val seen = CopyOnWriteArrayList<Int>()
        slowConsumer(sub, seen)
        sub.up()

        val heapBefore = BrokerTestKit.usedHeap()
        val started = System.currentTimeMillis()
        server.push(TOTAL_BP, PAYLOAD_16K)
        BrokerTestKit.waitUntil(15_000, "consumer read 100, now ${seen.size}") { seen.size >= 100 }
        val pushed = server.pushedBytes.get()
        val total = TOTAL_BP.toLong() * PAYLOAD_16K
        println("MqttClientBrokerTest.backpressure(raw): при 100 прочитанных записано ${pushed / 1024} КиБ из ${total / 1024}")
        assertTrue(pushed < total / 2, "отправитель не заблокирован: записано $pushed из $total байт")

        BrokerTestKit.waitUntil(25_000, "consumer got all, now ${seen.size}") { seen.size >= TOTAL_BP }
        assertTrue(server.pushDone.await(5, java.util.concurrent.TimeUnit.SECONDS))
        println("MqttClientBrokerTest.backpressure(raw): $TOTAL_BP сообщений за ${System.currentTimeMillis() - started} мс")
        assertEquals((0 until TOTAL_BP).toList(), seen.toList(), "без потерь и в порядке записи")
        BrokerTestKit.waitUntil(5_000, "PUBACK на каждое сообщение, now ${server.receivedAcks.get()}") {
            server.receivedAcks.get() == TOTAL_BP
        }
        assertHeapBounded(heapBefore, "backpressure(raw)")
    }

    // ---- исходы публикации ----

    @Test
    fun sentAndDeliveredByRealPuback() = timed {
        val broker = broker()
        val client = client(broker.port)
        val epoch = client.up().epoch
        assertEquals(PublishResult.Sent(epoch), client.publish("res/a", byteArrayOf(1)))
        assertEquals(
            PublishResult.Delivered(epoch),
            client.publish("res/a", byteArrayOf(2), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        )
        // QoS 0 PUBACK не имеет: Delivered недостижим, исход — Sent
        assertEquals(
            PublishResult.Sent(epoch),
            client.publish("res/a", byteArrayOf(3), Qos.AT_MOST_ONCE, until = PublishStage.Delivered)
        )
    }

    @Test
    fun deliveredWithMqtt311() = timed {
        val broker = broker()
        val client = client(broker.port, MQTTVersion.MQTT3_1_1)
        val epoch = client.up().epoch
        assertEquals(
            PublishResult.Delivered(epoch),
            client.publish("res/a", byteArrayOf(2), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        )
    }

    @Test
    fun publishExpiresWithoutBrokerAndNeverReachesIt() = timed {
        val port = TestBroker.freePort() // никто не слушает
        val client = client(port)
        val started = System.currentTimeMillis()
        val result = client.publish("res/expired", byteArrayOf(1), ttl = 1.seconds)
        val elapsed = System.currentTimeMillis() - started
        assertEquals(PublishResult.Expired, result)
        assertTrue(elapsed in 900..3_000, "Expired через TTL, а не сразу и не поздно: $elapsed мс")

        // Брокер появился позже: просроченное сообщение до него не доходит
        val broker = track(TestBroker(port))
        val observer = client(broker.port)
        val inbox = collect(observer)
        observer.up()
        observer.subscribed("g", "res/#")
        client.up()
        client.publish("res/marker", byteArrayOf(2), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        BrokerTestKit.waitUntil(10_000, "метка") { inbox.messages.any { it.topic == "res/marker" } }
        assertTrue(inbox.messages.none { it.topic == "res/expired" }, "просроченная публикация дошла до брокера")
    }

    /** Брокер остановлен посреди сессии: публикация ждёт соединения TTL = 1 с и завершается `Expired`. */
    @Test
    fun publishExpiresAfterTheBrokerIsStopped() = timed {
        val broker = broker()
        val client = client(broker.port)
        val epoch = client.up().epoch
        assertEquals(PublishResult.Sent(epoch), client.publish("res/alive", byteArrayOf(1)))
        broker.stop()
        BrokerTestKit.waitUntil(10_000, "клиент потерял соединение") { client.connection.value !is ConnectionState.Connected }

        val started = System.currentTimeMillis()
        assertEquals(PublishResult.Expired, client.publish("res/dead", byteArrayOf(2), ttl = 1.seconds))
        val elapsed = System.currentTimeMillis() - started
        assertTrue(elapsed in 900..3_000, "Expired через TTL: $elapsed мс")
    }

    @Test
    fun pendingPublishFailsClosedOnCloseAndPublishAfterCloseToo() = timed {
        val client = client(TestBroker.freePort())
        val pending = async(Dispatchers.Default) {
            client.publish("res/closed", byteArrayOf(1), ttl = 60.seconds)
        }
        client.start()
        delay(300)
        assertTrue(pending.isActive, "публикация ждёт соединения")
        withTimeout(10_000) { client.close() }
        assertEquals(PublishResult.Failed(PublishFailure.Closed), withTimeout(5_000) { pending.await() })
        assertEquals(PublishResult.Failed(PublishFailure.Closed), client.publish("res/closed", byteArrayOf(2)))
    }

    @Test
    fun closeOfClientWithoutConnectionFailsPublishWithClosed() = timed {
        val client = client(TestBroker.freePort())
        withTimeout(10_000) { client.close() }
        assertEquals(ConnectionState.Closed(null), client.connection.value)
        assertEquals(PublishResult.Failed(PublishFailure.Closed), client.publish("res/closed", byteArrayOf(2)))
    }

    /**
     * Обрыв после записи и до PUBACK: PUBLISH дошёл до брокера, PUBACK задержан прокси (`holdDownstream`), прокси рвёт
     * соединение → `Failed(ConnectionLost)`; после переподключения публикация не досылается.
     */
    @Test
    fun connectionLostAfterWriteBeforePuback() = timed {
        val broker = broker()
        val proxy = proxy(broker.port)
        val observer = client(broker.port)
        val inbox = collect(observer)
        observer.up()
        observer.subscribed("g", "lost/#")
        val client = client(proxy.port)
        val epoch = client.up().epoch

        proxy.holdDownstream = true
        val result = async(Dispatchers.Default) {
            client.publish("lost/x", byteArrayOf(7), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        }
        BrokerTestKit.waitUntil(5_000, "PUBLISH дошёл до прокси") { proxy.connections.last().published.size == 1 }
        BrokerTestKit.waitUntil(5_000, "брокер получил PUBLISH") { inbox.messages.any { it.topic == "lost/x" } }
        proxy.cut()
        assertEquals(PublishResult.Failed(PublishFailure.ConnectionLost), withTimeout(10_000) { result.await() })

        proxy.holdDownstream = false
        proxy.restore()
        BrokerTestKit.waitUntil(10_000, "клиент переподключился") {
            (client.connection.value as? ConnectionState.Connected)?.epoch?.let { it > epoch } == true
        }
        client.publish("lost/end", ByteArray(0), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        BrokerTestKit.waitUntil(10_000, "метка") { inbox.messages.any { it.topic == "lost/end" } }
        assertEquals(1, inbox.messages.count { it.topic == "lost/x" }, "неподтверждённая публикация не досылается")
    }

    // ---- эпохи ----

    /**
     * Рестарт брокера: `SessionEnded(e1)` → `SessionStarted(e2 > e1)` → автоматическая переподписка → retained, который
     * нашёл новый брокер, приходит снова. Подписчик ходит через прокси: пока брокер переезжает, прокси отвергает
     * подключения, чтобы retained гарантированно лёг в новый брокер до переподписки (порядок детерминирован).
     */
    private fun brokerRestart(version: MQTTVersion) = timed {
        val first = TestBroker()
        val port = first.port
        val proxy = proxy(port)
        val sub = client(proxy.port, version)
        val inbox = collect(sub)
        val e1 = sub.up().epoch
        sub.subscribed("g", "ep/#")
        val retainer1 = client(port, version)
        retainer1.up()
        retainer1.publish("ep/r", "one".encodeToByteArray(), Qos.AT_LEAST_ONCE, retain = true, until = PublishStage.Delivered)
        BrokerTestKit.waitUntil(10_000, "первое сообщение") { inbox.messages.any { it.topic == "ep/r" } }
        retainer1.close()

        first.stop()
        BrokerTestKit.waitUntil(10_000, "SessionEnded(e1)") { inbox.items.any { it is MqttIncoming.SessionEnded } }
        proxy.cut() // до старта нового брокера: переподписка не успеет раньше retained
        track(TestBroker(port))
        val retainer2 = client(port, version)
        retainer2.up()
        retainer2.publish("ep/r", "two".encodeToByteArray(), Qos.AT_LEAST_ONCE, retain = true, until = PublishStage.Delivered)
        proxy.restore()

        BrokerTestKit.waitUntil(15_000, "retained нового брокера после переподписки") {
            inbox.messages.any { it.payload.decodeToString() == "two" }
        }
        val items = inbox.items.toList()
        val ended = items.indexOfFirst { it is MqttIncoming.SessionEnded }
        val started2 = items.indexOfLast { it is MqttIncoming.SessionStarted }
        assertTrue(ended in 0 until started2, "SessionEnded раньше нового SessionStarted: $items")
        assertEquals(e1, (items[ended] as MqttIncoming.SessionEnded).epoch)
        val e2 = (items[started2] as MqttIncoming.SessionStarted).epoch
        assertTrue(e2 > e1, "эпоха растёт: $e1 -> $e2")
        assertTrue(items.drop(ended + 1).filterIsInstance<MqttIncoming.Message>().all { it.epoch == e2 })
        val again = items.drop(started2).filterIsInstance<MqttIncoming.Message>().single()
        assertEquals("two", again.payload.decodeToString())
        // Флаг retain у брокера форка при доставке retained по новой подписке сброшен (нарушение MQTT-3.3.1-8) — не проверяем
        assertEquals(e2, again.epoch)
        // подписка восстановлена и живая
        assertEquals(mapOf<String, TopicState>("ep/#" to TopicState.Granted(Qos.AT_LEAST_ONCE)), sub.subscriptions.state.value)
        retainer2.publish("ep/live", "live".encodeToByteArray(), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        BrokerTestKit.waitUntil(10_000, "живое сообщение после переподписки") { inbox.messages.any { it.topic == "ep/live" } }
    }

    @Test
    fun brokerRestartStartsNewEpochAndResubscribesMqtt5() = brokerRestart(MQTTVersion.MQTT5)

    @Test
    fun brokerRestartStartsNewEpochAndResubscribesMqtt311() = brokerRestart(MQTTVersion.MQTT3_1_1)

    // ---- will (H3) ----

    private fun willOnConnectionLoss(version: MQTTVersion, qos: Qos, retain: Boolean) = timed {
        val broker = broker()
        val proxy = proxy(broker.port)
        val observer = client(broker.port, version)
        val inbox = collect(observer)
        observer.up()
        observer.subscribed("g", "will/#")
        val victim = client(proxy.port, version) {
            it.copy(
                keepAlive = 2.seconds,
                will = MqttWill("will/victim", "gone".encodeToByteArray(), retain, qos)
            )
        }
        victim.up()
        assertTrue(inbox.messages.isEmpty())

        proxy.cut()
        BrokerTestKit.waitUntil(10_000, "брокер опубликовал will") { inbox.messages.isNotEmpty() }
        val will = inbox.messages.single()
        assertEquals("will/victim", will.topic)
        assertEquals("gone", will.payload.decodeToString())
    }

    @Test
    fun willIsPublishedOnConnectionLossMqtt311() = willOnConnectionLoss(MQTTVersion.MQTT3_1_1, Qos.AT_MOST_ONCE, false)

    @Test
    fun willIsPublishedOnConnectionLossMqtt5() = willOnConnectionLoss(MQTTVersion.MQTT5, Qos.AT_MOST_ONCE, false)

    @Test
    fun willQos1RetainedIsPublishedOnConnectionLossMqtt5() =
        willOnConnectionLoss(MQTTVersion.MQTT5, Qos.AT_LEAST_ONCE, true)

    private fun noWillOnGracefulClose(version: MQTTVersion) = timed {
        val broker = broker()
        val observer = client(broker.port, version)
        val inbox = collect(observer)
        observer.up()
        observer.subscribed("g", "will/#")
        val keepAlive = 1.seconds
        val victim = client(broker.port, version) {
            it.copy(keepAlive = keepAlive, will = MqttWill("will/victim", "gone".encodeToByteArray(), false, Qos.AT_MOST_ONCE))
        }
        victim.up()
        withTimeout(10_000) { victim.close() }

        // 2 · keepAlive: брокер за это время уже решил бы, что клиент пропал
        delay(2 * keepAlive.inWholeMilliseconds + 300)
        assertTrue(inbox.messages.isEmpty(), "will при штатном close() не публикуется: ${inbox.messages}")
        // наблюдатель жив и получил бы сообщение
        val marker = client(broker.port, version)
        marker.up()
        marker.publish("will/marker", ByteArray(0), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
        BrokerTestKit.waitUntil(10_000, "метка") { inbox.messages.any { it.topic == "will/marker" } }
        assertEquals(listOf("will/marker"), inbox.messages.map { it.topic })
    }

    @Test
    fun noWillOnGracefulCloseMqtt311() = noWillOnGracefulClose(MQTTVersion.MQTT3_1_1)

    @Test
    fun noWillOnGracefulCloseMqtt5() = noWillOnGracefulClose(MQTTVersion.MQTT5)

    private companion object {
        const val COUNT_1000 = 1_000
        const val TOTAL_BP = 2_000
        const val WARMUP = 50
        const val PAYLOAD_16K = 16 * 1024
    }
}
