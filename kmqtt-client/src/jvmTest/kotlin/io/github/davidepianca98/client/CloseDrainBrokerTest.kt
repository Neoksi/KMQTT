package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.packets.Qos
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration.Companion.seconds

/**
 * Закрытие с очередью сверх предела неподтверждённых на настоящем клиенте и встроенном брокере: продолжаемая
 * сессия держит в полёте не больше `MqttLimits.maxUnacknowledged` (64) публикаций QoS 1, остальные ждут в очереди.
 * `close()` досылает их по мере PUBACK и только потом пишет DISCONNECT — брокер получает все (так приложение стирает
 * retained-топики большого устройства при выключении «Панель в MQTT»).
 */
internal class CloseDrainBrokerTest : BrokerTestBase() {

    private fun <T> timed(block: suspend CoroutineScope.() -> T): T = runBlocking { withTimeout(30_000, block) }

    @Test
    fun closeSendsEveryQueuedPublicationBeyondTheUnacknowledgedLimit() = timed {
        val broker = broker()
        val proxy = proxy(broker.port)
        val observer = client(broker.port)
        val inbox = collect(observer)
        withTimeout(10_000) { observer.connect() } as? ConnectResult.Connected ?: fail("наблюдатель не подключился")
        observer.subscriptions.set("g", setOf("dev/#"), Qos.AT_LEAST_ONCE)
        withTimeout(10_000) { observer.subscriptions.awaitApplied("g") }

        val client = client(proxy.port) { it.copy(persistentSession = true, sessionExpiry = 60.seconds) }
        withTimeout(10_000) { client.connect() } as? ConnectResult.Connected ?: fail("клиент не подключился")

        // PUBACK задержаны: в полёте 64, остальные ждут окна в очереди клиента
        proxy.holdDownstream = true
        val results = (0 until COUNT).map { i ->
            async(Dispatchers.Default, start = CoroutineStart.UNDISPATCHED) {
                client.publish("dev/$i", byteArrayOf(1), Qos.AT_LEAST_ONCE, retain = true)
            }
        }
        BrokerTestKit.waitUntil(5_000, { "в полёте ${proxy.connections.last().published.size}" }) {
            proxy.connections.last().published.size >= WINDOW
        }
        delay(100)
        assertEquals(WINDOW, proxy.connections.last().published.size, "сверх предела ничего не записано")

        val closing = async(Dispatchers.Default) { client.close() }
        delay(200)
        proxy.holdDownstream = false
        withTimeout(10_000) { closing.await() }

        val outcomes = results.awaitAll()
        assertTrue(outcomes.all { it is PublishResult.Sent }, "все записаны: ${outcomes.groupingBy { it }.eachCount()}")
        BrokerTestKit.waitUntil(5_000, { "брокер принял ${inbox.messages.map { it.topic }.toSet().size} из $COUNT" }) {
            inbox.messages.map { it.topic }.toSet().size == COUNT
        }
        assertEquals(ConnectionState.Closed(null), client.connection.value)
    }

    private companion object {
        const val COUNT = 100
        const val WINDOW = 64
    }
}
