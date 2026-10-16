package io.github.davidepianca98.integration

import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// SEVEN-254: retained, доставленный по новой подписке, идёт с RETAIN = 1 (MQTT-3.3.1-8, MQTT 3.1.1 и 5 §3.3.1.3), а
// обычная публикация другому подписчику - с RETAIN = 0.
class RetainedFlagTest {

    private val brokers = mutableListOf<RawTestBroker>()

    @AfterTest
    fun tearDown() {
        brokers.forEach { it.close() }
        brokers.clear()
    }

    @Test
    fun retainedOnNewSubscriptionHasRetainFlagMqtt4() = retainedFlags(4, optionsByte = 0)

    @Test
    fun retainedOnNewSubscriptionHasRetainFlagMqtt5() = retainedFlags(5, optionsByte = 0)

    // Retain As Published (бит 3 параметров подписки MQTT 5) влияет только на последующие публикации, не на retained
    @Test
    fun retainedOnNewSubscriptionHasRetainFlagWithRetainAsPublishedMqtt5() = retainedFlags(5, optionsByte = 0x08)

    private fun retainedFlags(version: Int, optionsByte: Int) {
        val broker = RawTestBroker().also { brokers += it }
        RawClient(broker.port, version, "pub").use { publisher ->
            publisher.publish("ret/1", "kept".toByteArray(), retain = true)
            // QoS 0 без подтверждения: даём брокеру сохранить сообщение до подписки
            Thread.sleep(300)
            RawClient(broker.port, version, "sub").use { subscriber ->
                subscriber.subscribe("ret/#", optionsByte)
                val retained = subscriber.readPacket()
                assertEquals(3, retained.type, "PUBLISH")
                assertTrue(retained.retain, "MQTT-3.3.1-8: retained по новой подписке идёт с RETAIN = 1")

                // обычная публикация другому подписчику: RETAIN = 0
                publisher.publish("ret/2", "live".toByteArray(), retain = false)
                val live = subscriber.readPacket()
                assertEquals(3, live.type, "PUBLISH")
                assertEquals(false, live.retain, "живая публикация идёт с RETAIN = 0")
            }
        }
    }
}
