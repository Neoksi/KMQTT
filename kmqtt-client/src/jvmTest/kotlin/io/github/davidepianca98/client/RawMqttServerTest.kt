package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.MQTTVersion
import java.net.InetAddress
import java.net.Socket
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** Тестовая обвязка `RawMqttServer`: причины конца соединения различаются, переподключение принимается. */
class RawMqttServerTest {
    @Test
    fun reconnectsAreAcceptedAndEachEndHasItsCause() {
        val server = RawMqttServer(MQTTVersion.MQTT5, acceptReconnect = true)
        try {
            // #0: обрезанный PUBLISH QoS 1 — ошибка разбора на сервере (раньше: обрыв без итога)
            Socket(InetAddress.getLoopbackAddress(), server.port).use { socket ->
                socket.getOutputStream().apply { write(byteArrayOf(0x32, 0x01, 0x00)); flush() }
                socket.soTimeout = 5_000
                assertEquals(-1, socket.getInputStream().read(), "сервер закрывает соединение после ошибки")
            }
            assertEquals(RawEnd.ERROR, server.awaitEnd(5_000), server.describe())
            // #1: переподключение принято, клиент закрывает штатно
            Socket(InetAddress.getLoopbackAddress(), server.port).close()
            waitFor { server.ends.size == 2 }
            // #2: живое соединение, сервер закрыт тестом
            val last = Socket(InetAddress.getLoopbackAddress(), server.port)
            waitFor { server.connections.get() == 3 }
            server.close()
            last.close()
            waitFor { server.ends.size == 3 }
            assertEquals(listOf(RawEnd.ERROR, RawEnd.EOF, RawEnd.CLOSED_BY_TEST), server.ends.map { it.end }, server.describe())
            assertIs<ArrayIndexOutOfBoundsException>(server.ends.first().cause)
            assertTrue(server.describe().contains("соединений 3"))
        } finally {
            server.close()
        }
    }

    @Test
    fun singleConnectionByDefault() {
        val server = RawMqttServer(MQTTVersion.MQTT5)
        try {
            Socket(InetAddress.getLoopbackAddress(), server.port).close()
            assertEquals(RawEnd.EOF, server.awaitEnd(5_000))
            assertEquals(1, server.connections.get())
        } finally {
            server.close()
        }
    }

    private fun waitFor(condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + 5_000
        while (!condition()) {
            check(System.currentTimeMillis() < deadline) { "условие не выполнилось за 5 с" }
            Thread.sleep(5)
        }
    }
}
