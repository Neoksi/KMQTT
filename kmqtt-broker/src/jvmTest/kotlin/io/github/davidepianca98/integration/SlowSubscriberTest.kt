package io.github.davidepianca98.integration

import java.io.ByteArrayOutputStream
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals

// при частичной записи подписчику брокер оставлял интерес сокета только OP_WRITE и переставал читать
// клиента (PUBACK/PINGREQ) до полной записи хвоста; время последнего принятого пакета не обновлялось, и брокер
// отключал живого, но медленного подписчика по keep-alive.
class SlowSubscriberTest {

    private val brokers = mutableListOf<RawTestBroker>()

    @AfterTest
    fun tearDown() {
        brokers.forEach { it.close() }
        brokers.clear()
    }

    @Test
    fun brokerKeepsReadingSlowSubscriberAndKeepsItAlive() {
        val broker = RawTestBroker().also { brokers += it }
        // keep-alive 2 с: брокер отключает клиента без пакетов 3 с
        RawClient(broker.port, 4, "slow-sub", keepAliveSeconds = 2).use { subscriber ->
            RawClient(broker.port, 4, "pub").use { publisher ->
                subscriber.subscribe("burst/#")
                val all = ByteArrayOutputStream()
                for (i in 0 until COUNT) {
                    all.write(publisher.publishBytes("burst/$i", ByteArray(PAYLOAD_SIZE) { (i + it).toByte() }, retain = false))
                }
                publisher.writeRaw(all.toByteArray())
                // подписчик ничего не читает (брокер упёрся в буфер сокета), но шлёт PINGREQ (сразу и каждые 0,5 с)
                // дольше, чем 3 с окна keep-alive
                subscriber.ping()
                repeat(PINGS - 1) {
                    Thread.sleep(PING_INTERVAL_MS)
                    subscriber.ping()
                }
                // соединение живо: доставлены все PUBLISH и пришли ответы на все PINGREQ (порядок между ними не важен)
                var publishes = 0
                var pongs = 0
                while (publishes < COUNT || pongs < PINGS) {
                    when (val type = subscriber.readPacket().type) {
                        3 -> publishes++
                        13 -> pongs++
                        else -> error("неожиданный пакет типа $type")
                    }
                }
                assertEquals(COUNT, publishes)
                assertEquals(PINGS, pongs)
            }
        }
    }

    private companion object {
        const val COUNT = 200
        const val PAYLOAD_SIZE = 16 * 1024
        const val PINGS = 11
        const val PING_INTERVAL_MS = 500L
    }
}
