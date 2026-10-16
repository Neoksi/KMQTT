package io.github.davidepianca98.client.internal

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * Детерминированные чередования автомата [IncomingGate] (SEVEN-260, разбор W6 §4.2 Г1–Г4): владелец (`offer`/`drain`)
 * и сборщик (`tryTake`) вызываются в заданном порядке на одном потоке; межпоточное окно «между неудачным `trySend` и
 * флагом паузы» открывается тестовым хуком `beforePauseFlag`.
 *
 * Мутационная проверка: убрать повтор переноса удержанных после флага в `IncomingGate.offer` —
 * [consumerEmptiesChannelBeforePauseFlagAndStillWakesOwner], [randomInterleavingsKeepOrderAndNeverStall] и оба теста
 * [IncomingGateStressTest] краснеют (проверено при SEVEN-260: стресс встаёт в первом же раунде).
 */
class IncomingGateTest {
    private class Rig(capacity: Int) {
        var wakes = 0
        var pauses = 0
        var hook: () -> Unit = {}
        val gate = IncomingGate<Int>(capacity, wake = { wakes++ }, onPause = { pauses++ }, beforePauseFlag = { hook() })
    }

    @Test
    fun capacityNTakesBurstWithoutPause() {
        val rig = Rig(3)
        repeat(3) { rig.gate.offer(it) }
        assertFalse(rig.gate.pausing)
        assertFalse(rig.gate.paused)
        assertEquals(0, rig.gate.heldCount)
        assertEquals(listOf(0, 1, 2), List(3) { rig.gate.tryTake() })
        assertEquals(0, rig.wakes)
        assertFalse(rig.gate.drain())
    }

    @Test
    fun fullChannelHoldsInOrderPausesOnceAndResumesWhenDrained() {
        val rig = Rig(1)
        repeat(4) { rig.gate.offer(it) }
        assertTrue(rig.gate.pausing)
        assertTrue(rig.gate.paused)
        assertEquals(3, rig.gate.heldCount)
        assertEquals(1, rig.pauses)
        val taken = ArrayList<Int>()
        var resumed = false
        while (!resumed) {
            val wakesBefore = rig.wakes
            taken += rig.gate.tryTake() ?: fail("канал пуст при удержанных ${rig.gate.heldCount}")
            assertEquals(wakesBefore + 1, rig.wakes, "каждое взятие на паузе будит владельца")
            resumed = rig.gate.drain()
        }
        assertEquals(0, rig.gate.heldCount)
        assertFalse(rig.gate.paused)
        assertEquals(listOf(0, 1, 2), taken)
        assertEquals(3, rig.gate.tryTake(), "последний удержанный — в канале")
        assertEquals(1, rig.pauses)
    }

    /** Г1 (D5): сборщик опустошил канал между неудачным `trySend` и флагом — без пробуждения. */
    @Test
    fun consumerEmptiesChannelBeforePauseFlagAndStillWakesOwner() {
        val rig = Rig(1)
        rig.gate.offer(0)
        val taken = ArrayList<Int>()
        rig.hook = { taken += checkNotNull(rig.gate.tryTake()) }
        rig.gate.offer(1)
        rig.hook = {}
        assertEquals(listOf(0), taken)
        assertEquals(0, rig.wakes, "флага ещё не было — взятие не будило")
        // Живость: владелец уснёт до пробуждения; сборщику должно быть что взять, иначе пробуждения не будет никогда
        assertEquals(0, rig.gate.heldCount, "повтор после флага должен переложить удержанный в освободившийся канал")
        assertFalse(rig.gate.channelEmpty)
        // Г4: удержанных нет, но пауза стоит — следующее взятие будит, ближайший drain снимает паузу
        assertTrue(rig.gate.pausing)
        assertEquals(1, rig.gate.tryTake())
        assertEquals(1, rig.wakes)
        assertTrue(rig.gate.drain())
        assertFalse(rig.gate.paused)
    }

    /** Г3: пауза снята и в том же шаге поставлена снова. */
    @Test
    fun resumeAndPauseAgainInOneStep() {
        val rig = Rig(1)
        rig.gate.offer(0)
        rig.gate.offer(1)
        assertEquals(0, rig.gate.tryTake())
        assertTrue(rig.gate.drain(), "удержанный переложен — пауза снята")
        assertFalse(rig.gate.paused)
        rig.gate.offer(2)
        assertEquals(2, rig.pauses)
        assertTrue(rig.gate.paused)
        assertEquals(1, rig.gate.heldCount)
        assertEquals(1, rig.gate.tryTake())
        assertTrue(rig.gate.drain())
        assertEquals(2, rig.gate.tryTake())
        assertNull(rig.gate.tryTake())
    }

    @Test
    fun drainWithoutPauseDoesNothing() {
        val rig = Rig(2)
        assertFalse(rig.gate.drain())
        rig.gate.offer(0)
        assertFalse(rig.gate.drain())
        assertEquals(0, rig.pauses)
    }

    /**
     * Модель хоста «`select()` без срока»: на паузе владелец делает шаг только по пробуждению; шаг с чтением отдаёт
     * залп целиком (пауза посреди залпа его не обрывает — так ведёт себя `runOnce`). Сборщик берёт в случайные моменты,
     * в том числе внутри окна «до флага». Провал — состояние, из которого никто не продвинется, или нарушение порядка.
     */
    @Test
    fun randomInterleavingsKeepOrderAndNeverStall() {
        for (seed in 1..40) runModel(seed, total = 5_000)
    }

    private fun runModel(seed: Int, total: Int) {
        val random = Random(seed)
        val capacity = 1 + random.nextInt(4)
        var wakePending = false
        var reading = true
        val taken = ArrayList<Int>(total)
        lateinit var gate: IncomingGate<Int>
        gate = IncomingGate(
            capacity,
            wake = { wakePending = true },
            onPause = { reading = false },
            // окно «до флага»: сборщик успевает взять 0..2 элемента, не будя владельца
            beforePauseFlag = { repeat(random.nextInt(3)) { gate.tryTake()?.let { taken += it } } },
        )
        var next = 0
        var ops = 0
        while (taken.size < total) {
            if (++ops > total * 50) fail("seed $seed: нет продвижения, взято ${taken.size}")
            val ownerCanRun = reading || wakePending
            val consumerCanRun = !gate.channelEmpty
            if (!ownerCanRun && !consumerCanRun) {
                fail(
                    "seed $seed (capacity $capacity): остановка — чтение на паузе, пробуждения нет, канал пуст; " +
                        "удержано ${gate.heldCount}, отдано $next, взято ${taken.size}"
                )
            }
            if (ownerCanRun && (!consumerCanRun || random.nextBoolean())) {
                wakePending = false
                if (gate.drain()) reading = true
                if (reading) repeat(1 + random.nextInt(6)) { if (next < total) gate.offer(next++) }
                if (next >= total && gate.heldCount == 0 && !gate.pausing) reading = false
            } else {
                taken += checkNotNull(gate.tryTake())
            }
        }
        assertEquals((0 until total).toList(), taken, "seed $seed: порядок")
    }
}
