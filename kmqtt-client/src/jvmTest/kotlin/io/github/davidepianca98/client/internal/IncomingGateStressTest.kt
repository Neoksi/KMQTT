package io.github.davidepianca98.client.internal

import kotlinx.coroutines.runBlocking
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertNull
import kotlin.test.fail

/**
 * Стресс [IncomingGate] настоящими потоками (SEVEN-260, W6 §4.4 п. 2): поток-владелец ведёт себя как `NioActorHost`
 * без дедлайнов (`keepAlive = 0`) — на паузе ждёт пробуждения **без срока** (как `select()`), в шаге с чтением отдаёт
 * залп целиком; сборщик — `consume` в `runBlocking` на своём потоке, иногда медлит. `capacity = 1`; в окно «до флага
 * паузы» владелец иногда уступает процессор (`Thread.yield()`), чтобы гонка Г1 случалась на деле. Сторожевой таймер:
 * нет продвижения [STALL_MS] — провал (потеря пробуждения = остановка приёма). Проверяется живость и порядок.
 *
 * Повторы: [ROUNDS] раундов по [ITEMS] элементов; переопределяются переменными окружения `KMQTT_GATE_STRESS_ROUNDS` и
 * `KMQTT_GATE_STRESS_ITEMS` (Gradle передаёт окружение тестовой JVM).
 */
class IncomingGateStressTest {
    @Test
    fun realThreadsCapacityOneNeverStallAndKeepOrder() {
        val rounds = System.getenv("KMQTT_GATE_STRESS_ROUNDS")?.toIntOrNull() ?: ROUNDS
        val items = System.getenv("KMQTT_GATE_STRESS_ITEMS")?.toIntOrNull() ?: ITEMS
        repeat(rounds) { round(it, items, capacity = 1) }
    }

    @Test
    fun realThreadsSmallBufferNeverStallAndKeepOrder() {
        repeat(2) { round(it, ITEMS, capacity = 4) }
    }

    private fun round(round: Int, total: Int, capacity: Int) {
        val wakeups = Semaphore(0)
        val progress = AtomicInteger()
        val failure = AtomicReference<Throwable?>(null)
        val offered = AtomicInteger()
        var reading = true // поток владельца
        val noise = Random(round) // поток владельца
        val gate = IncomingGate<Int>(
            capacity,
            wake = { wakeups.release() },
            onPause = { reading = false },
            // Шум в окне «неудачный trySend → флаг паузы» (W6 §4.2 Г1): без него окно — наносекунды, и потерю
            // пробуждения настоящие потоки почти не ловят
            beforePauseFlag = { if (noise.nextInt(4) == 0) Thread.yield() },
        )
        val consumer = thread(name = "gate-stress-consumer", isDaemon = true) {
            val random = Random(round * 31 + 7)
            try {
                runBlocking {
                    var expected = 0
                    gate.consume { item ->
                        if (item != expected) error("порядок: ждали $expected, пришло $item")
                        expected++
                        progress.incrementAndGet()
                        when (random.nextInt(64)) {
                            0 -> Thread.sleep(1)
                            1, 2, 3 -> Thread.yield()
                            else -> Unit
                        }
                    }
                    if (expected != total) error("сборщик завершился на $expected из $total")
                }
            } catch (t: Throwable) {
                failure.compareAndSet(null, t)
            }
        }
        val owner = thread(name = "gate-stress-owner", isDaemon = true) {
            val random = Random(round * 17 + 3)
            var next = 0
            try {
                while (true) {
                    if (gate.drain()) reading = true
                    if (next >= total && gate.heldCount == 0) break
                    if (reading && next < total) {
                        repeat(1 + random.nextInt(8)) { if (next < total) gate.offer(next++) }
                        offered.set(next)
                    }
                    if (!reading) {
                        // Шаг закончен на паузе — «select()» без срока: следующий шаг только по пробуждению сборщика;
                        // несколько пробуждений = одно (липкий флаг `Selector.wakeup()`)
                        wakeups.acquire()
                        wakeups.drainPermits()
                    }
                }
                gate.close()
            } catch (_: InterruptedException) {
                // сторожевой таймер
            } catch (t: Throwable) {
                failure.compareAndSet(null, t)
            }
        }
        var last = -1
        var lastChangeAt = System.currentTimeMillis()
        while (consumer.isAlive) {
            consumer.join(50)
            failure.get()?.let { throw AssertionError("раунд $round: ${it.message}", it) }
            val now = System.currentTimeMillis()
            val seen = progress.get()
            if (seen != last) {
                last = seen
                lastChangeAt = now
            } else if (now - lastChangeAt > STALL_MS) {
                owner.interrupt()
                gate.closeChannel()
                fail(
                    "раунд $round (capacity $capacity): приём встал на $seen из $total; отдано ${offered.get()}, " +
                        "пауза ${gate.paused}"
                )
            }
        }
        owner.join(5_000)
        assertNull(failure.get(), "раунд $round")
        if (progress.get() != total) fail("раунд $round: взято ${progress.get()} из $total")
    }

    private companion object {
        const val ROUNDS = 5
        const val ITEMS = 20_000
        const val STALL_MS = 3_000L
    }
}
