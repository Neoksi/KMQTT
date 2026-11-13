package io.github.davidepianca98.client

import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Внешний источник эпох `MqttClientOptions.epochSource` на виртуальном
 * времени: эпоха соединения, входящих и исходов публикаций берётся из источника, один вызов на CONNACK; повторяющиеся
 * значения клиент не ломают; без источника — прежний счётчик процесса (его закрепляют тесты `MqttClientVirtualTest`).
 */
class EpochSourceTest {

    private fun options(epochSource: (() -> Long)?) = MqttClientOptions(
        host = "broker.test",
        port = 1883,
        clientId = "epochs",
        keepAlive = 0.seconds,
        reconnect = ReconnectPolicy(initial = 1.seconds, jitter = 0.0),
        epochSource = epochSource,
    )

    private class Rig(val client: MqttClient, val broker: FakeBroker, val items: List<MqttIncoming>)

    private fun TestScope.rig(epochSource: (() -> Long)?, broker: FakeBroker = FakeBroker()): Rig {
        val host = VirtualHost(this, broker)
        val client = MqttClient(options(epochSource), host, host)
        val items = ArrayList<MqttIncoming>()
        backgroundScope.launch(start = CoroutineStart.UNDISPATCHED) { client.incoming.collect { items += it } }
        return Rig(client, broker, items)
    }

    /** Две сессии: сообщение и публикации QoS 0/1 в каждой, обрыв между ними, затем закрытие. */
    private suspend fun TestScope.twoSessions(rig: Rig): List<List<Any>> {
        val sessions = ArrayList<List<Any>>()
        repeat(2) { index ->
            val connected = if (index == 0) {
                assertIs<ConnectResult.Connected>(rig.client.connect()).epoch
            } else {
                advanceTimeBy(1_000)
                runCurrent()
                assertIs<ConnectionState.Connected>(rig.client.connection.value).epoch
            }
            val state = assertIs<ConnectionState.Connected>(rig.client.connection.value).epoch
            rig.broker.send(rig.broker.script.publish("t", byteArrayOf(index.toByte())))
            runCurrent()
            val sent = rig.client.publish("a", byteArrayOf(1))
            val delivered = rig.client.publish("a", byteArrayOf(2), until = PublishStage.Delivered)
            sessions += listOf(connected, state, sent, delivered)
            if (index == 0) {
                rig.broker.eof()
                runCurrent()
                assertIs<ConnectionState.Waiting>(rig.client.connection.value)
            }
        }
        rig.client.close()
        runCurrent()
        return sessions
    }

    @Test
    fun epochsComeFromSourceForStateIncomingAndPublishOutcomes() = runTest {
        var next = 100L
        var calls = 0
        val rig = rig(epochSource = {
            calls++
            next += 10
            next
        })
        val sessions = twoSessions(rig)

        assertEquals(2, calls, "один вызов источника на CONNACK")
        assertEquals(
            listOf(110L, 110L, PublishResult.Sent(110), PublishResult.Delivered(110)),
            sessions[0],
        )
        assertEquals(
            listOf(120L, 120L, PublishResult.Sent(120), PublishResult.Delivered(120)),
            sessions[1],
        )
        val items = rig.items
        assertEquals(6, items.size, "$items")
        assertEquals(MqttIncoming.SessionStarted(110), items[0])
        assertEquals(110L, assertIs<MqttIncoming.Message>(items[1]).epoch)
        assertEquals(110L, assertIs<MqttIncoming.SessionEnded>(items[2]).epoch)
        assertEquals(MqttIncoming.SessionStarted(120), items[3])
        assertEquals(120L, assertIs<MqttIncoming.Message>(items[4]).epoch)
        assertEquals(MqttIncoming.SessionEnded(120, null), items[5])
    }

    @Test
    fun attemptFailedBeforeConnackTakesNoNumber() = runTest {
        val taken = ArrayList<Long>()
        var next = 0L
        val broker = FakeBroker().apply { connect = FakeBroker.ConnectBehavior.Fail(MqttError.Io("connect", null)) }
        val rig = rig(epochSource = { (++next).also { taken += it } }, broker = broker)
        rig.client.start()
        runCurrent()
        assertIs<ConnectionState.Waiting>(rig.client.connection.value)
        assertTrue(taken.isEmpty(), "неудачная попытка до CONNACK номер не берёт")

        broker.connect = FakeBroker.ConnectBehavior.Accept
        advanceTimeBy(1_000)
        runCurrent()
        assertEquals(1L, assertIs<ConnectionState.Connected>(rig.client.connection.value).epoch)
        assertEquals(listOf(1L), taken)
        assertEquals(MqttIncoming.SessionStarted(1), rig.items.single())
        rig.client.close()
        runCurrent()
    }

    @Test
    fun repeatedEpochsDoNotBreakClient() = runTest {
        val rig = rig(epochSource = { 7L })
        val sessions = twoSessions(rig)

        for (session in sessions) {
            assertEquals(listOf(7L, 7L, PublishResult.Sent(7), PublishResult.Delivered(7)), session)
        }
        assertEquals(
            listOf<MqttIncoming>(MqttIncoming.SessionStarted(7), MqttIncoming.SessionStarted(7)),
            rig.items.filterIsInstance<MqttIncoming.SessionStarted>(),
        )
        assertEquals(listOf(0, 1), rig.items.filterIsInstance<MqttIncoming.Message>().map { it.payload.single().toInt() })
        assertEquals(MqttIncoming.SessionEnded(7, null), rig.items.last())
        assertEquals(6, rig.items.size, "${rig.items}")
    }

    @Test
    fun toStringNamesOnlyKindOfSource() {
        assertTrue("epochSource=process" in options(null).toString())
        assertTrue("epochSource=custom" in options { 1L }.toString())
    }
}
