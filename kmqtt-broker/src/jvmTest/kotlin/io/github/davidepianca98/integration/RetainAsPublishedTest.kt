package io.github.davidepianca98.integration

import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals

// Флаг RETAIN у живой публикации, пересылаемой по уже установленной подписке:
// - MQTT 3.1.1 [MQTT-3.3.1-9]: всегда 0, как бы флаг ни был выставлен у публикатора;
// - MQTT 5 [MQTT-3.3.1-12]: при Retain As Published = 0 (бит 3 параметров подписки) - 0;
// - MQTT 5 [MQTT-3.3.1-13]: при Retain As Published = 1 - как у публикатора.
// Retained, отправленный в ответ на новую подписку, всегда идёт с RETAIN = 1 [MQTT-3.3.1-8].
class RetainAsPublishedTest {

    private val brokers = mutableListOf<RawTestBroker>()

    @AfterTest
    fun tearDown() {
        brokers.forEach { it.close() }
        brokers.clear()
    }

    @Test
    fun liveRetainedPublishHasRetainFlagClearedMqtt4() =
        liveFlags(4, "rap/#", optionsByte = 0, expectedForRetained = false)

    @Test
    fun liveRetainedPublishHasRetainFlagClearedWithoutRetainAsPublishedMqtt5() =
        liveFlags(5, "rap/#", optionsByte = 0x00, expectedForRetained = false)

    @Test
    fun liveRetainedPublishKeepsRetainFlagWithRetainAsPublishedMqtt5() =
        liveFlags(5, "rap/#", optionsByte = 0x08, expectedForRetained = true)

    @Test
    fun liveRetainedPublishHasRetainFlagClearedOnSharedSubscriptionMqtt5() =
        liveFlags(5, "\$share/g/rap/#", optionsByte = 0x00, expectedForRetained = false)

    @Test
    fun liveRetainedPublishKeepsRetainFlagOnSharedSubscriptionWithRetainAsPublishedMqtt5() =
        liveFlags(5, "\$share/g/rap/#", optionsByte = 0x08, expectedForRetained = true)

    @Test
    fun retainedForNewSubscriptionAfterLivePublishHasRetainFlagMqtt4() = retainedAfterLive(4, optionsByte = 0)

    @Test
    fun retainedForNewSubscriptionAfterLivePublishHasRetainFlagMqtt5() = retainedAfterLive(5, optionsByte = 0x00)

    /** Живые публикации с retain = 1 и retain = 0 одному подписчику с параметрами [optionsByte]. */
    private fun liveFlags(version: Int, filter: String, optionsByte: Int, expectedForRetained: Boolean) {
        val broker = RawTestBroker().also { brokers += it }
        RawClient(broker.port, version, "pub").use { publisher ->
            RawClient(broker.port, version, "sub").use { subscriber ->
                subscriber.subscribe(filter, optionsByte)

                publisher.publish("rap/1", "kept".toByteArray(), retain = true)
                val retained = subscriber.readPacket()
                assertEquals(3, retained.type, "PUBLISH")
                assertEquals(expectedForRetained, retained.retain, "RETAIN живой публикации с retain = 1")

                publisher.publish("rap/2", "live".toByteArray(), retain = false)
                val live = subscriber.readPacket()
                assertEquals(3, live.type, "PUBLISH")
                assertEquals(false, live.retain, "RETAIN живой публикации с retain = 0")
            }
        }
    }

    /** Сообщение, опубликованное с retain = 1 при живом подписчике, новый подписчик получает с RETAIN = 1. */
    private fun retainedAfterLive(version: Int, optionsByte: Int) {
        val broker = RawTestBroker().also { brokers += it }
        RawClient(broker.port, version, "pub").use { publisher ->
            RawClient(broker.port, version, "live").use { liveSubscriber ->
                liveSubscriber.subscribe("rap/#", optionsByte)
                publisher.publish("rap/1", "kept".toByteArray(), retain = true)
                val live = liveSubscriber.readPacket()
                assertEquals(3, live.type, "PUBLISH")
                assertEquals(false, live.retain, "живая публикация по установленной подписке")

                RawClient(broker.port, version, "fresh").use { fresh ->
                    fresh.subscribe("rap/#", optionsByte)
                    val retained = fresh.readPacket()
                    assertEquals(3, retained.type, "PUBLISH")
                    assertEquals(true, retained.retain, "retained по новой подписке")
                }
            }
        }
    }
}
