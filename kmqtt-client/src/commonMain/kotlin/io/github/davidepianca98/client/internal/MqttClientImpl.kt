package io.github.davidepianca98.client.internal

import io.github.davidepianca98.client.ConnectResult
import io.github.davidepianca98.client.ConnectionState
import io.github.davidepianca98.client.HostAction
import io.github.davidepianca98.client.HostDriver
import io.github.davidepianca98.client.HostStep
import io.github.davidepianca98.client.MqttClient
import io.github.davidepianca98.client.MqttClientOptions
import io.github.davidepianca98.client.MqttClock
import io.github.davidepianca98.client.MqttError
import io.github.davidepianca98.client.MqttHost
import io.github.davidepianca98.client.MqttIncoming
import io.github.davidepianca98.client.MqttSubscriptions
import io.github.davidepianca98.client.MqttSubscriptionsImpl
import io.github.davidepianca98.client.PublishFailure
import io.github.davidepianca98.client.PublishResult
import io.github.davidepianca98.client.PublishStage
import io.github.davidepianca98.client.TransportEvent
import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.engine.EngineAction
import io.github.davidepianca98.client.engine.EngineCommand
import io.github.davidepianca98.client.engine.EngineLogEvent
import io.github.davidepianca98.client.engine.MqttClientEngine
import io.github.davidepianca98.client.engine.PublishTicket
import io.github.davidepianca98.mqtt.packets.Qos
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.locks.reentrantLock
import kotlinx.atomicfu.locks.withLock
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration

private val epochCounter = atomic(0L)
private val temporaryCounter = atomic(0L)

/**
 * Фасад [MqttClient] (S06b-T05a): актор-клей между [MqttClientEngine] и [MqttHost]. Вызывающие потоки только кладут
 * [ClientRequest] в очередь и будят хост (H4); всё остальное — на потоке хоста в [runOnce]: запросы → движок,
 * события транспорта → движок, тик → движок, действия движка → `HostAction` / входящие / состояние / исходы.
 *
 * Дописывание DISCONNECT (находка T03a, S06b-T05f): `HostAction.CloseTransport` отбрасывает недописанный остаток, а
 * движок выдаёт `Write(DISCONNECT)` и сразу `CloseTransport` (DISCONNECT(code) после ошибки протокола,
 * `closeLinger = 0`). Поэтому `EngineAction.CloseTransport` после записи в том же шаге или при штатном закрытии
 * (`close()`) переводится в `HostAction.FlushAndClose(`[CLOSE_FLUSH_MS]`)`: хост закрывает канал, когда остаток
 * записи ушёл в ОС, или по сроку. Пути ошибок без записи (обрыв, ping-таймаут, зависший потребитель) закрывают сразу.
 *
 * Безопасность (решение владельца 2026-10-16, S06b-T05b): при `config.tls != null` клиент создаётся только с хостом,
 * который сам применяет TLS ([hostAppliesTls] — его ставит jvm-фабрика `MqttClient(options)`); иначе — отказ
 * `IllegalArgumentException` при создании, до запуска хоста: никогда не подключаться открытым текстом.
 */
@OptIn(MqttCoreApi::class)
internal class MqttClientImpl(
    override val config: MqttClientOptions,
    private val host: MqttHost,
    @Suppress("unused") private val clock: MqttClock,
    hostAppliesTls: Boolean = false,
) : MqttClient, HostDriver {
    init {
        require(config.tls == null || hostAppliesTls) { TLS_HOST_REQUIRED }
    }

    private val engine = MqttClientEngine(config.toEngineOptions(), nextEpoch = { epochCounter.incrementAndGet() })
    private val requests = Channel<ClientRequest>(Channel.UNLIMITED)
    private val incomingChannel = Channel<MqttIncoming>(config.limits.incomingBuffer)
    private val stateFlow = MutableStateFlow<ConnectionState>(ConnectionState.Idle)
    private val closedSignal = CompletableDeferred<Unit>()
    private val drained = CompletableDeferred<Unit>()
    private val subs = MqttSubscriptionsImpl(::submit, closedSignal)

    private val lock = reentrantLock()
    private var lifecycle = NEW // под lock
    private val collected = atomic(false)
    private val readPaused = atomic(false)
    private val tickets = atomic(0L)
    private val finalized = atomic(false)

    // ---- только поток хоста ----
    private val publishes = HashMap<PublishTicket, CompletableDeferred<PublishResult>>()
    private val connectWaiters = ArrayList<CompletableDeferred<ConnectResult>>()
    private val overflow = ArrayDeque<MqttIncoming>()
    private var pauseRequested = false
    private var closeRequested = false
    private var closeArmed = false
    private var wroteThisStep = false
    private var rejectedGroup: String? = null
    private var now = 0L

    override val connection: StateFlow<ConnectionState> = stateFlow.asStateFlow()
    override val subscriptions: MqttSubscriptions get() = subs

    override val incoming: Flow<MqttIncoming> = flow {
        check(collected.compareAndSet(false, true)) { "MqttClient.incoming: only one collector" }
        for (item in incomingChannel) {
            if (readPaused.value) host.wakeup()
            emit(item)
        }
    }

    override fun start() {
        submit(ClientRequest.Command(EngineCommand.Start))
    }

    override suspend fun connect(): ConnectResult {
        val result = CompletableDeferred<ConnectResult>()
        if (!submit(ClientRequest.AwaitConnect(result))) return ConnectResult.Failed(closedError())
        start()
        return result.await()
    }

    override suspend fun publish(
        topic: String,
        payload: ByteArray,
        qos: Qos,
        retain: Boolean,
        ttl: Duration,
        until: PublishStage,
    ): PublishResult {
        val ticket = PublishTicket(tickets.incrementAndGet())
        val ttlMs = ttl.inWholeMilliseconds.coerceAtLeast(0)
        val result = CompletableDeferred<PublishResult>()
        val command = EngineCommand.Publish(ticket, topic, payload, qos, retain, ttlMs, until)
        if (!submit(ClientRequest.Publish(command, result))) return PublishResult.Failed(PublishFailure.Closed)
        try {
            return result.await()
        } catch (e: CancellationException) {
            submit(ClientRequest.Command(EngineCommand.Cancel(ticket)))
            throw e
        }
    }

    override suspend fun close() {
        val previous = lock.withLock {
            val was = lifecycle
            if (was == NEW) lifecycle = CLOSED
            if (was == RUNNING) {
                lifecycle = CLOSING
                requests.trySend(ClientRequest.Close)
            }
            was
        }
        when (previous) {
            NEW -> {
                host.close()
                finish(null)
            }
            RUNNING -> {
                host.wakeup()
                val limit = config.closeLinger.inWholeMilliseconds + CLOSE_FLUSH_MS + CLOSE_MARGIN_MS
                withTimeoutOrNull(limit) { drained.await() }
                // Хост доводит дописывание (FlushAndClose) не дольше CLOSE_FLUSH_MS, затем останавливается
                host.close(CLOSE_FLUSH_MS + HOST_CLOSE_TIMEOUT_MS)
                finish(null)
            }
            else -> closedSignal.await()
        }
    }

    /** Положить запрос в очередь и разбудить хост (первый запрос запускает хост); `false` — клиент закрывается. */
    private fun submit(request: ClientRequest): Boolean {
        lock.withLock {
            if (lifecycle != NEW && lifecycle != RUNNING) return false
            requests.trySend(request)
            if (lifecycle == NEW) {
                lifecycle = RUNNING
                try {
                    host.start(this)
                } catch (e: Throwable) {
                    lifecycle = CLOSED
                    finish(MqttError.Io("host", e))
                    throw e
                }
            }
        }
        host.wakeup()
        return true
    }

    // ---- HostDriver: поток хоста ----

    override fun runOnce(nowMs: Long, events: List<TransportEvent>): HostStep {
        now = nowMs
        wroteThisStep = false
        if (closeArmed) {
            // Шаг, выдавший последние действия закрытия, хост уже исполнил
            drained.complete(Unit)
            return HostStep.Idle
        }
        val out = ArrayList<HostAction>()
        drainOverflow(out)
        while (true) handle(requests.tryReceive().getOrNull() ?: break, out)
        for (event in events) {
            engine.onTransport(event, nowMs)
            process(out)
        }
        engine.onTick(nowMs)
        process(out)
        var deadline = engine.nextDeadline()
        if (closeRequested && engine.state is ConnectionState.Closed) {
            closeArmed = true
            deadline = nowMs
        }
        return HostStep(deadline, out)
    }

    override fun onStopped(cause: Throwable?) {
        finish(cause?.let { MqttError.Io("host", it) })
    }

    private fun handle(request: ClientRequest, out: MutableList<HostAction>) {
        when (request) {
            is ClientRequest.Command -> {
                val command = request.command
                if (command is EngineCommand.Cancel) publishes.remove(command.ticket)
                rejectedGroup = null
                engine.onCommand(command, now)
                process(out)
                if (command is EngineCommand.SetGroup) subs.onSetGroup(command, rejectedGroup == command.group)
            }
            is ClientRequest.Publish -> {
                publishes[request.command.ticket] = request.result
                engine.onCommand(request.command, now)
                process(out)
            }
            is ClientRequest.AwaitConnect -> when (val state = engine.state) {
                is ConnectionState.Connected -> request.result.complete(ConnectResult.Connected(state.epoch, state.server))
                is ConnectionState.Closed -> request.result.complete(ConnectResult.Failed(state.error ?: closedError()))
                else -> connectWaiters += request.result
            }
            is ClientRequest.AwaitApplied -> {
                subs.addWaiter(request)
                if (engine.state is ConnectionState.Closed) subs.failWaiters()
            }
            is ClientRequest.TemporaryStart -> {
                subs.temporaryStarted(request.group, request.refused)
                handle(ClientRequest.Command(EngineCommand.SetGroup(request.group, request.filters, request.qos)), out)
            }
            is ClientRequest.TemporaryEnd -> {
                subs.temporaryEnded(request.group)
                handle(ClientRequest.Command(EngineCommand.SetGroup(request.group, emptySet(), request.qos)), out)
            }
            ClientRequest.Close -> {
                closeRequested = true
                engine.onCommand(EngineCommand.Close(), now)
                process(out)
            }
        }
    }

    /** Все действия движка (в т. ч. порождённые обработкой действий) — по порядку. */
    private fun process(out: MutableList<HostAction>) {
        while (true) {
            val actions = engine.takeActions()
            if (actions.isEmpty()) return
            for (action in actions) apply(action, out)
        }
    }

    private fun apply(action: EngineAction, out: MutableList<HostAction>) {
        when (action) {
            is EngineAction.Connect -> {
                wroteThisStep = false
                out += HostAction.Connect(action.host, action.port, action.timeoutMs)
            }
            is EngineAction.Write -> {
                wroteThisStep = true
                out += HostAction.Write(action.bytes)
            }
            EngineAction.CloseTransport -> {
                // После записи (DISCONNECT, DISCONNECT(code)) или при штатном закрытии — дописать, затем закрыть;
                // хост закроет сразу, если очередь записи пуста
                out += if (wroteThisStep || closeRequested) {
                    HostAction.FlushAndClose(CLOSE_FLUSH_MS)
                } else {
                    HostAction.CloseTransport
                }
                wroteThisStep = false
            }
            is EngineAction.PauseRead -> out += HostAction.PauseRead(action.paused)
            is EngineAction.Emit -> deliver(action.incoming)
            is EngineAction.State -> onState(action.state)
            is EngineAction.Complete -> publishes.remove(action.ticket)?.complete(action.result)
            is EngineAction.Log -> log(action.event)
            is EngineAction.TopicStates -> subs.onTopicStates(action.states)
            is EngineAction.SubscriptionRefused -> subs.onRefused(action.refusal)
            is EngineAction.GroupRejected -> {
                rejectedGroup = action.group
                safeLog { groupRejected(action.filters, action.limit) }
            }
        }
    }

    /** Входящее — в буфер; нет места — держать у себя по порядку и приостановить чтение (W6 §4.5). */
    private fun deliver(item: MqttIncoming) {
        if (overflow.isEmpty() && incomingChannel.trySend(item).isSuccess) return
        overflow.addLast(item)
        if (!pauseRequested) {
            pauseRequested = true
            readPaused.value = true
            engine.onCommand(EngineCommand.SetReadPaused(true), now)
            // D5 ревью G1 (SEVEN-255): потребитель мог опустошить канал между неудачным trySend и флагом и уснуть
            // без пробуждения хоста. Флаг уже стоит: переложить ещё раз — всё, что он возьмёт дальше, разбудит хост
            while (overflow.isNotEmpty() && incomingChannel.trySend(overflow.first()).isSuccess) overflow.removeFirst()
        }
    }

    private fun drainOverflow(out: MutableList<HostAction>) {
        while (overflow.isNotEmpty() && incomingChannel.trySend(overflow.first()).isSuccess) overflow.removeFirst()
        if (overflow.isEmpty() && pauseRequested) {
            pauseRequested = false
            readPaused.value = false
            engine.onCommand(EngineCommand.SetReadPaused(false), now)
            process(out)
        }
    }

    private fun onState(state: ConnectionState) {
        stateFlow.value = state
        val result = when (state) {
            is ConnectionState.Connected -> ConnectResult.Connected(state.epoch, state.server)
            is ConnectionState.Closed -> ConnectResult.Failed(state.error ?: closedError())
            else -> return
        }
        for (waiter in connectWaiters) waiter.complete(result)
        connectWaiters.clear()
        // Клиент закрылся сам (Never/maxAttempts): SUBACK больше не будет — awaitApplied не должен висеть
        if (state is ConnectionState.Closed) subs.failWaiters()
    }

    private fun log(event: EngineLogEvent) = safeLog {
        when (event) {
            is EngineLogEvent.Connected -> connected(event.epoch)
            is EngineLogEvent.Disconnected -> disconnected(event.error)
            is EngineLogEvent.ReconnectIn -> reconnectIn(event.delayMs)
            is EngineLogEvent.Dropped -> dropped(event.reason)
            is EngineLogEvent.Subscribe -> subscribe(event.count)
            is EngineLogEvent.Refused -> refused(event.count)
            is EngineLogEvent.PublishRefused -> publishRefused(event.code)
        }
    }

    private inline fun safeLog(block: io.github.davidepianca98.client.MqttLogger.() -> Unit) {
        try {
            config.logger.block()
        } catch (_: Throwable) {
            // Исключение журнала не должно останавливать поток хоста
        }
    }

    /**
     * Клиент закончен: незавершённые запросы и ожидания — с отказом, `incoming` завершается (буфер дочитывается).
     * Один раз: из [onStopped] (поток хоста) или из [close] (хост не запускался либо не остановился за таймаут).
     */
    private fun finish(error: MqttError?) {
        if (!finalized.compareAndSet(false, true)) return
        lock.withLock { lifecycle = CLOSED }
        while (true) reject(requests.tryReceive().getOrNull() ?: break)
        requests.close()
        for (pending in publishes.values.toList()) pending.complete(PublishResult.Failed(PublishFailure.Closed))
        publishes.clear()
        val failed = ConnectResult.Failed(error ?: closedError())
        for (waiter in connectWaiters.toList()) waiter.complete(failed)
        connectWaiters.clear()
        subs.failAll()
        if (error != null || stateFlow.value !is ConnectionState.Closed) stateFlow.value = ConnectionState.Closed(error)
        incomingChannel.close()
        drained.complete(Unit)
        closedSignal.complete(Unit)
    }

    private fun reject(request: ClientRequest) {
        when (request) {
            is ClientRequest.Publish -> request.result.complete(PublishResult.Failed(PublishFailure.Closed))
            is ClientRequest.AwaitConnect -> request.result.complete(ConnectResult.Failed(closedError()))
            is ClientRequest.AwaitApplied -> request.result.completeExceptionally(IllegalStateException("MqttClient closed"))
            is ClientRequest.TemporaryStart -> request.refused.completeExceptionally(IllegalStateException("MqttClient closed"))
            else -> Unit
        }
    }

    internal companion object {
        private const val NEW = 0
        private const val RUNNING = 1
        private const val CLOSING = 2
        private const val CLOSED = 3

        /**
         * Предельный срок дописывания остатка записи перед закрытием (`HostAction.FlushAndClose`). Константа, а не
         * `closeLinger`: при `closeLinger = 0` DISCONNECT тоже должен дойти, а большой `closeLinger` не должен
         * растягивать остановку хоста. Пустая очередь закрывается сразу — срок тратится только на настоящий остаток.
         */
        const val CLOSE_FLUSH_MS: Long = 1_000

        /** Запас к `closeLinger` на штатное закрытие, прежде чем остановить хост принудительно. */
        const val CLOSE_MARGIN_MS: Long = 2_000
        const val HOST_CLOSE_TIMEOUT_MS: Long = 5_000

        /** Текст отказа при `tls != null` и хосте без TLS (без адресов и секретов). */
        const val TLS_HOST_REQUIRED: String =
            "MqttClientOptions.tls is set, but the host does not apply TLS: use MqttClient(options) on JVM"

        /** Номер временной группы — растёт в процессе. */
        fun nextTemporary(): Long = temporaryCounter.incrementAndGet()

        private fun closedError(): MqttError = MqttError.Io("closed", null)
    }
}
