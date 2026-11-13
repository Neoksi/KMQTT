@file:OptIn(MqttCoreApi::class)

package io.github.davidepianca98.client.hostcontract

import io.github.davidepianca98.client.HostAction
import io.github.davidepianca98.client.HostDriver
import io.github.davidepianca98.client.HostStep
import io.github.davidepianca98.client.MqttHost
import io.github.davidepianca98.client.MqttTransport
import io.github.davidepianca98.client.TransportEvent
import io.github.davidepianca98.client.core.MqttCoreApi
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.fail
import kotlin.time.TimeSource

/**
 * общий набор сценариев хоста ([MqttHostContractTest]) — платформенная часть теста. Сервер на loopback и его
 * сторона соединения («брокер») реализуются в тестах каждой платформы (jvmTest, jsTest, mingwX64Test): так
 * `commonTest` не требует `expect`/`actual` во всех source set'ах (linux, Apple), где сервера нет.
 */
internal interface ContractServer {
    /** Порт на 127.0.0.1. */
    val port: Int

    /** Принять одно соединение. Сторона сервера не читает, пока тест не попросит ([ContractPeer.readExactly]). */
    suspend fun accept(timeoutMs: Long = 10_000): ContractPeer

    fun close()
}

/** Как закончился поток со стороны сервера: штатный FIN (чтение вернуло конец потока) или сброс (RST). */
internal enum class PeerEnd { Fin, Reset, Timeout }

/** Конец потока и сколько байтов пришло до него (их прочли и отбросили). */
internal data class EndOfStream(val end: PeerEnd, val extraBytes: Int)

/** Серверная сторона одного соединения. */
internal interface ContractPeer {
    suspend fun readExactly(count: Int, timeoutMs: Long = 10_000): ByteArray

    suspend fun write(bytes: ByteArray)

    /** Закрыть свою сторону на запись (FIN), чтение остаётся. */
    suspend fun shutdownOutput()

    /** Читать и отбрасывать до конца потока. */
    suspend fun awaitEnd(timeoutMs: Long = 10_000): EndOfStream

    fun close()
}

/** Доступ к `pendingWriteBytes` последнего созданного транспорта; читать только на потоке хоста (из `runOnce`). */
internal class PendingProbe {
    private val transport = atomic<MqttTransport?>(null)

    fun attach(created: MqttTransport) {
        transport.value = created
    }

    fun pending(): Long = transport.value?.pendingWriteBytes ?: -1L
}

/** Драйвер теста: действия — из очереди теста, события и отметки вызовов `runOnce` — в каналы теста. */
internal class ContractDriver : HostDriver {
    val commands = Channel<HostAction>(Channel.UNLIMITED)
    val events = Channel<TransportEvent>(Channel.UNLIMITED)
    val runs = Channel<TimeSource.Monotonic.ValueTimeMark>(Channel.UNLIMITED)
    val stopped = CompletableDeferred<Throwable?>()

    private val deadlineRef = atomic<Long?>(null)
    private val onRunRef = atomic<(() -> Unit)?>(null)
    private val throwRef = atomic<Throwable?>(null)

    var deadline: Long?
        get() = deadlineRef.value
        set(value) {
            deadlineRef.value = value
        }

    var onRun: (() -> Unit)?
        get() = onRunRef.value
        set(value) {
            onRunRef.value = value
        }

    var throwOnRun: Throwable?
        get() = throwRef.value
        set(value) {
            throwRef.value = value
        }

    override fun runOnce(nowMs: Long, events: List<TransportEvent>): HostStep {
        // Дедлайн — до отметки вызова: тест, увидев отметку, может сменить его, и этот шаг должен вернуть прежний
        val nextDeadline = deadline
        for (event in events) this.events.trySend(event)
        onRun?.invoke()
        runs.trySend(TimeSource.Monotonic.markNow())
        throwOnRun?.let { throw it }
        val actions = ArrayList<HostAction>()
        while (true) actions += commands.tryReceive().getOrNull() ?: break
        return HostStep(nextDeadline, actions)
    }

    override fun onStopped(cause: Throwable?) {
        stopped.complete(cause)
    }

    fun send(host: MqttHost, action: HostAction) {
        commands.trySend(action)
        host.wakeup()
    }

    suspend fun awaitEvent(timeoutMs: Long = 5_000): TransportEvent =
        withTimeoutOrNull(timeoutMs) { events.receive() } ?: fail("нет события транспорта за $timeoutMs мс")

    suspend fun awaitRun(timeoutMs: Long = 1_000): TimeSource.Monotonic.ValueTimeMark =
        withTimeoutOrNull(timeoutMs) { runs.receive() } ?: fail("runOnce не вызван за $timeoutMs мс")

    fun clearRuns() {
        do {
            val taken = runs.tryReceive().isSuccess
        } while (taken)
    }

    /** Забрать накопленные события, не дожидаясь новых. */
    fun drainEvents(): List<TransportEvent> {
        val out = ArrayList<TransportEvent>()
        while (true) out += events.tryReceive().getOrNull() ?: break
        return out
    }
}

/** Длинное число, которое пишет поток хоста и читает тест (atomicfu не допускает локальных атомиков). */
internal class LongCell(initial: Long) {
    private val cell = atomic(initial)

    var value: Long
        get() = cell.value
        set(v) {
            cell.value = v
        }
}
