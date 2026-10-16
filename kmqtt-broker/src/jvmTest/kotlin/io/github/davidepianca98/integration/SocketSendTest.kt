package io.github.davidepianca98.integration

import java.io.ByteArrayOutputStream
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals

// SEVEN-254: пачка 50 x 16 КиБ QoS 0 подписчику, который начинает читать, когда брокер уже упёрся в буфер сокета.
// maximumPacketSize по умолчанию (32 КиБ) = буфер отправки брокера. До исправления Socket.send терял накопленный,
// но ещё не записанный остаток при росте буфера (читатель получал около 230 КиБ), а TLSSocket.send отправлял только
// первую запись TLS (16 КиБ) каждого вызова.
class SocketSendTest {

    private val brokers = mutableListOf<RawTestBroker>()

    @AfterTest
    fun tearDown() {
        brokers.forEach { it.close() }
        brokers.clear()
    }

    @Test
    fun largeBurstToSlowSubscriberIsDeliveredCompletely() = burst(tls = false)

    @Test
    fun largeBurstToSlowSubscriberIsDeliveredCompletelyOverTls() = burst(tls = true)

    private fun burst(tls: Boolean) {
        val broker = RawTestBroker(tls = tls).also { brokers += it }
        RawClient(broker.port, 4, "sub", tls).use { subscriber ->
            RawClient(broker.port, 4, "pub", tls).use { publisher ->
                subscriber.subscribe("burst/#")
                val all = ByteArrayOutputStream()
                for (i in 0 until COUNT) {
                    val payload = ByteArray(PAYLOAD_SIZE) { (i + it).toByte() }
                    all.write(publisher.publishBytes("burst/$i", payload, retain = false))
                }
                publisher.writeRaw(all.toByteArray())
                // читатель молчит, пока брокер не накопит неотправленное
                Thread.sleep(1_500)
                var bytes = 0L
                for (received in 0 until COUNT) {
                    val packet = subscriber.readPacket()
                    assertEquals(3, packet.type, "PUBLISH")
                    val topicLength = ((packet.body[0].toInt() and 0xFF) shl 8) or (packet.body[1].toInt() and 0xFF)
                    assertEquals("burst/$received", String(packet.body, 2, topicLength, Charsets.UTF_8), "порядок пакетов")
                    val payloadStart = 2 + topicLength
                    assertEquals(PAYLOAD_SIZE, packet.body.size - payloadStart)
                    assertEquals(received.toByte(), packet.body[payloadStart], "начало нагрузки")
                    assertEquals((received + PAYLOAD_SIZE - 1).toByte(), packet.body.last(), "конец нагрузки")
                    bytes += PAYLOAD_SIZE
                }
                assertEquals(COUNT.toLong() * PAYLOAD_SIZE, bytes)
            }
        }
    }

    private companion object {
        const val COUNT = 50
        const val PAYLOAD_SIZE = 16 * 1024
    }
}
