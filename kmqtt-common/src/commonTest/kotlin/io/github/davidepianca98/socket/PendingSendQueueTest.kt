package io.github.davidepianca98.socket

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Порядок и целостность исходящих данных native-сокетов. Модель сокета: принимает не больше [capacity] байтов за попытку. */
class PendingSendQueueTest {

    private class FakeSocket(var capacity: Int) {
        val received = mutableListOf<Byte>()
        var failure: Throwable? = null
        var attempts = 0

        fun attempt(data: UByteArray): Int {
            attempts++
            failure?.let { throw it }
            val n = minOf(capacity, data.size)
            for (i in 0 until n) received.add(data[i].toByte())
            capacity -= n
            return n
        }
    }

    private fun bytes(vararg v: Int) = UByteArray(v.size) { v[it].toUByte() }

    private fun FakeSocket.text() = received.map { it.toInt() }

    @Test
    fun freeSocketSendsEverythingAtOnce() {
        val socket = FakeSocket(100)
        val queue = PendingSendQueue()

        assertTrue(queue.submit(bytes(1, 2, 3), socket::attempt))
        assertTrue(queue.submit(bytes(4, 5), socket::attempt))

        assertEquals(listOf(1, 2, 3, 4, 5), socket.text())
        assertTrue(queue.isEmpty())
    }

    @Test
    fun newDataDoesNotOvertakePartiallySentData() {
        val socket = FakeSocket(2)
        val queue = PendingSendQueue()

        // 5 байтов, принято 2: остаток 3 4 5 ждёт
        assertFalse(queue.submit(bytes(1, 2, 3, 4, 5), socket::attempt))
        assertEquals(listOf(1, 2), socket.text())

        // Сокет освободился на 10 байтов: новые данные идут строго за остатком
        socket.capacity = 10
        assertTrue(queue.submit(bytes(6, 7), socket::attempt))

        assertEquals(listOf(1, 2, 3, 4, 5, 6, 7), socket.text())
        assertTrue(queue.isEmpty())
    }

    @Test
    fun newDataWaitsBehindQueueWhileSocketStaysBlocked() {
        val socket = FakeSocket(0)
        val queue = PendingSendQueue()

        assertFalse(queue.submit(bytes(1, 2), socket::attempt))
        assertFalse(queue.submit(bytes(3), socket::attempt))
        assertFalse(queue.submit(bytes(4, 5), socket::attempt))
        assertEquals(3, queue.size)
        assertTrue(socket.received.isEmpty())

        socket.capacity = 100
        assertTrue(queue.drain(socket::attempt))

        assertEquals(listOf(1, 2, 3, 4, 5), socket.text())
    }

    @Test
    fun drainSendsInOrderAndKeepsOnlyUnsentRemainder() {
        val socket = FakeSocket(0)
        val queue = PendingSendQueue()
        queue.submit(bytes(1, 2, 3), socket::attempt)
        queue.submit(bytes(4, 5, 6), socket::attempt)

        socket.capacity = 4 // первый кусок целиком и 1 байт второго
        assertFalse(queue.drain(socket::attempt))
        assertEquals(listOf(1, 2, 3, 4), socket.text())
        assertEquals(1, queue.size)

        socket.capacity = 1
        assertFalse(queue.drain(socket::attempt))
        assertEquals(listOf(1, 2, 3, 4, 5), socket.text())

        socket.capacity = 1
        assertTrue(queue.drain(socket::attempt))
        assertEquals(listOf(1, 2, 3, 4, 5, 6), socket.text()) // без потерь и дублей
    }

    @Test
    fun partialAcceptanceNeverDuplicatesBytes() {
        val socket = FakeSocket(1)
        val queue = PendingSendQueue()
        val expected = (1..20).toList()

        queue.submit(UByteArray(20) { (it + 1).toUByte() }, socket::attempt)
        repeat(40) {
            socket.capacity = 1
            queue.drain(socket::attempt)
        }

        assertEquals(expected, socket.text())
        assertTrue(queue.isEmpty())
    }

    @Test
    fun queuedDataIsCopiedSoCallerMayReuseItsBuffer() {
        val socket = FakeSocket(0)
        val queue = PendingSendQueue()
        val buffer = bytes(1, 2, 3)

        queue.submit(buffer, socket::attempt)
        buffer[0] = 99u // вызывающий переиспользовал буфер
        socket.capacity = 10
        queue.drain(socket::attempt)

        assertEquals(listOf(1, 2, 3), socket.text())
    }

    @Test
    fun drainOnEmptyQueueDoesNotTouchSocket() {
        val socket = FakeSocket(10)
        val queue = PendingSendQueue()

        assertTrue(queue.drain(socket::attempt))
        assertEquals(0, socket.attempts)
    }

    @Test
    fun errorKeepsUnsentHeadInQueue() {
        val socket = FakeSocket(0)
        val queue = PendingSendQueue()
        queue.submit(bytes(1, 2), socket::attempt)

        socket.failure = IOException("boom")
        assertFailsWith<IOException> { queue.submit(bytes(3), socket::attempt) }
        assertEquals(2, queue.size) // ничего не потеряно и не переставлено

        socket.failure = null
        socket.capacity = 10
        assertTrue(queue.drain(socket::attempt))
        assertEquals(listOf(1, 2, 3), socket.text())
    }

    @Test
    fun reentrantSubmitFromAttemptStaysBehindCurrentData() {
        // TLS-запись по WANT_READ читает и может записать из того же стека: вложенные данные идут за текущими
        val socket = FakeSocket(100)
        val queue = PendingSendQueue()
        var nested = false
        val attempt: (UByteArray) -> Int = { data ->
            if (!nested) {
                nested = true
                assertFalse(queue.submit(bytes(9), socket::attempt)) // внутри отправки только ставится в очередь
            }
            socket.attempt(data)
        }

        assertTrue(queue.submit(bytes(1, 2), attempt))

        assertEquals(listOf(1, 2, 9), socket.text())
        assertTrue(queue.isEmpty())
    }

    @Test
    fun emptyChunkIsDroppedWithoutBlockingQueue() {
        val socket = FakeSocket(0)
        val queue = PendingSendQueue()

        assertTrue(queue.submit(UByteArray(0), socket::attempt))
        assertTrue(queue.isEmpty())
    }
}
