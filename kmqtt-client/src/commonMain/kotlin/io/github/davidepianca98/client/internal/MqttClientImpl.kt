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
import io.github.davidepianca98.client.MqttLogger
import io.github.davidepianca98.client.PacketDirection
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
import io.github.davidepianca98.mqtt.packets.MQTTControlPacketType
import io.github.davidepianca98.mqtt.packets.Qos
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.locks.reentrantLock
import kotlinx.atomicfu.locks.withLock
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration

/**
 * Счётчик эпох сессий по умолчанию: один на весь процесс, а не на клиента. Эпохи одного клиента растут, но
 * не подряд: между ними бывают эпохи других клиентов процесса; сравнивать эпохи разных клиентов или рассчитывать на `+1`
 * нельзя. С `MqttClientOptions.epochSource` эпохи берутся из источника вызывающего, этот счётчик не тратится.
 */
private val epochCounter = atomic(0L)
private val temporaryCounter = atomic(0L)

/**
 * Фасад [MqttClient]: актор-клей между [MqttClientEngine] и [MqttHost]. Вызывающие потоки только кладут
 * [ClientRequest] в очередь и будят хост; всё остальное — на потоке хоста в [runOnce]: запросы → движок,
 * события транспорта → движок, тик → движок, действия движка → `HostAction` / входящие / состояние / исходы.
 *
 * Дописывание DISCONNECT: `HostAction.CloseTransport` отбрасывает недописанный остаток, а
 * движок выдаёт `Write(DISCONNECT)` и сразу `CloseTransport` (DISCONNECT(code) после ошибки протокола,
 * `closeLinger = 0`). Поэтому `EngineAction.CloseTransport` после записи в том же шаге или при штатном закрытии
 * (`close()`) переводится в `HostAction.FlushAndClose(`[CLOSE_FLUSH_MS]`)`: хост закрывает канал, когда остаток
 * записи ушёл в ОС, или по сроку. Пути ошибок без записи (обрыв, ping-таймаут, зависший потребитель) закрывают сразу.
 *
 * Безопасность: при `config.tls != null` клиент создаётся только с хостом,
 * который сам применяет TLS ([hostAppliesTls] — его ставят платформенные фабрики `MqttClient(options)` JVM, JS, mingwX64 и
 * Linux); иначе — отказ
 * `IllegalArgumentException` при создании, до запуска хоста: никогда не подключаться открытым текстом.
 * То же для `config.webSocket != null` ([hostAppliesWebSocket]): WebSocket применяют только хосты фабрик JVM и
 * JS (Node и браузер).
 * Так же при создании отказывает комбинация `tls.checkServerCertificate = false` и `verifyHostname = true`
 * ([TRUST_ALL_NEEDS_NO_HOSTNAME]): проверка в одном месте, через которое проходит любая фабрика.
 *
 * Расширенная аутентификация (`config.enhancedAuth`): `EngineAction.AuthChallenge` запускает
 * `MqttEnhancedAuth.respond` в корутине на [authContext] (не на потоке хоста: провайдер — `suspend`, может ходить в сеть
 * или к хранилищу ключей); ответ возвращается запросом `EngineCommand.AuthResponse` через ту же очередь (будит хост).
 * Исключение провайдера → отказ (`failed = true`), его текст нигде не сохраняется (H11). Новый вызов, `Connect` и
 * `CloseTransport` отменяют ещё не ответившего провайдера; позднее завершение прошлого подключения движок отбрасывает
 * по номеру подключения. Область провайдеров отменяется при завершении клиента.
 * Повторная аутентификация посреди сессии ([MqttClient.reauthenticate]) — запрос
 * [ClientRequest.Reauthenticate] той же очереди: один обмен за раз ([reauthWaiter]), исход —
 * `EngineAction.ReauthComplete` движка; вызовы брокера в обмене обслуживает тот же [startAuth].
 *
 * `MqttLogger.packet` — здесь, а не в ядре: исходящие — по каждому `HostAction.Write` (ядро
 * склеивает в нём целые пакеты, границы совпадают с началом записи), входящие — по потоку `TransportEvent.Bytes` того
 * подключения, которое кормит ядро ([PacketScanner], сброс на каждый `Connect`/`Connected`). С [MqttLogger.None] не
 * считается ничего.
 */
@OptIn(MqttCoreApi::class)
internal class MqttClientImpl(
    override val config: MqttClientOptions,
    private val host: MqttHost,
    @Suppress("unused") private val clock: MqttClock,
    hostAppliesTls: Boolean = false,
    hostAppliesWebSocket: Boolean = false,
    authContext: CoroutineContext = Dispatchers.Default,
) : MqttClient, HostDriver {
    init {
        require(config.tls == null || hostAppliesTls) { TLS_HOST_REQUIRED }
        // Решение: «доверять всем» не проверяет имя хоста — это
        // должно быть сказано явно (`verifyHostname = false`), а не молча при умолчании `true`
        require(config.tls?.checkServerCertificate != false || !config.verifyHostname) { TRUST_ALL_NEEDS_NO_HOSTNAME }
        // чужой хост не обязан говорить WebSocket — MQTT напрямую в WebSocket-порт молча не работал бы
        require(config.webSocket == null || hostAppliesWebSocket) { WEBSOCKET_HOST_REQUIRED }
    }

    private val engine = MqttClientEngine(
        config.toEngineOptions(),
        nextEpoch = config.epochSource ?: { epochCounter.incrementAndGet() },
    )
    private val requests = Channel<ClientRequest>(Channel.UNLIMITED)
    private val gate: IncomingGate<MqttIncoming> = IncomingGate(
        capacity = config.limits.incomingBuffer,
        wake = { host.wakeup() },
        onPause = {
            pausedSince = now
            safeLog { readPaused(gate.depth, config.limits.incomingBuffer) }
            engine.onCommand(EngineCommand.SetReadPaused(true), now)
        },
        onDepth = { depth -> safeLog { incomingBuffer(depth, config.limits.incomingBuffer) } },
    )
    private val stateFlow = MutableStateFlow<ConnectionState>(ConnectionState.Idle)
    private val closedSignal = CompletableDeferred<Unit>()
    private val drained = CompletableDeferred<Unit>()
    private val subs = MqttSubscriptionsImpl(::submit, closedSignal)

    private val lock = reentrantLock()
    private var lifecycle = NEW // под lock
    private val collected = atomic(false)
    private val tickets = atomic(0L)

    /** Публикации в очереди запросов, ещё не взятые потоком хоста (`MqttLimits.commandQueue`). */
    private val queuedPublishes = atomic(0)
    private val finalized = atomic(false)

    /**
     * Замок шага: под ним [runOnce] и завершение ([finish]) — всё состояние потока хоста
     * (`publishes`, `connectWaiters`, удержанные входящие [gate], подписки). Без соперничества (обычный случай — только
     * поток хоста) стоит одну атомарную операцию. Аварийный путь [close] берёт его только через `tryLock`.
     */
    private val stepLock = reentrantLock()
    private val hostFinished = atomic(false) // часть завершения под stepLock выполнена
    private val finishError = atomic<MqttError?>(null)

    // ---- только поток хоста ----
    private val publishes = HashMap<PublishTicket, CompletableDeferred<PublishResult>>()
    private val connectWaiters = ArrayList<CompletableDeferred<ConnectResult>>()
    private var closeRequested = false
    private var closeArmed = false
    private var wroteThisStep = false
    private var rejectedGroup: String? = null
    private var now = 0L
    private var pausedSince = 0L

    // Расширенная аутентификация: область провайдера ответов, текущий вызов — только поток хоста
    private val authScope: CoroutineScope? = config.enhancedAuth?.let { CoroutineScope(SupervisorJob() + authContext) }
    private var authJob: Job? = null

    /** Ждущий исхода обмен повторной аутентификации — только поток хоста; один обмен за раз. */
    private var reauthWaiter: CompletableDeferred<Boolean>? = null

    private val packetLog = config.logger !== MqttLogger.None
    private val incomingPackets = PacketScanner { type, size -> logPacket(PacketDirection.Incoming, type, size) }
    private val outgoingPackets = PacketScanner { type, size -> logPacket(PacketDirection.Outgoing, type, size) }

    override val connection: StateFlow<ConnectionState> = stateFlow.asStateFlow()
    override val subscriptions: MqttSubscriptions get() = subs

    override val incoming: Flow<MqttIncoming> = flow {
        check(collected.compareAndSet(false, true)) { "MqttClient.incoming: only one collector" }
        gate.consume { emit(it) }
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
        resend: Boolean,
    ): PublishResult {
        val ticket = PublishTicket(tickets.incrementAndGet())
        val ttlMs = ttl.inWholeMilliseconds.coerceAtLeast(0)
        val result = CompletableDeferred<PublishResult>()
        val command = EngineCommand.Publish(ticket, topic, payload, qos, retain, ttlMs, until, resend)
        submitOrRefuse(ClientRequest.Publish(command, result))?.let { return PublishResult.Failed(it) }
        try {
            return result.await()
        } catch (e: CancellationException) {
            submit(ClientRequest.Command(EngineCommand.Cancel(ticket)))
            throw e
        }
    }

    override suspend fun reauthenticate(): Boolean {
        check(config.enhancedAuth != null) { "reauthenticate() needs MqttClientOptions.enhancedAuth" }
        val result = CompletableDeferred<Boolean>()
        if (!submit(ClientRequest.Reauthenticate(result))) return false
        return result.await()
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
                // Досылка очереди перед DISCONNECT (closeDrain), затем ожидание FIN брокера (closeLinger)
                val limit = config.closeDrain.inWholeMilliseconds + config.closeLinger.inWholeMilliseconds +
                    CLOSE_FLUSH_MS + CLOSE_MARGIN_MS
                withTimeoutOrNull(limit) { drained.await() }
                // Хост доводит дописывание (FlushAndClose) не дольше CLOSE_FLUSH_MS, затем останавливается
                val stopped = host.close(CLOSE_FLUSH_MS + HOST_CLOSE_TIMEOUT_MS)
                // Остановился — onStopped уже всё закончил; нет — аварийный путь: поток хоста ещё может быть в шаге
                if (stopped) finish(null) else finishFromForeignThread(null)
            }
            else -> closedSignal.await()
        }
    }

    /** Положить запрос в очередь и разбудить хост (первый запрос запускает хост); `false` — клиент закрывается. */
    private fun submit(request: ClientRequest): Boolean = submitOrRefuse(request) == null

    /**
     * [submit] с причиной отказа: `null` — принято; [PublishFailure.Closed] — клиент закрывается;
     * [PublishFailure.QueueFull] — публикация, а в очереди уже `MqttLimits.commandQueue` публикаций, которые поток хоста
     * ещё не взял (отказ сразу, вызывающий не блокируется; служебные запросы не ограничиваются).
     */
    private fun submitOrRefuse(request: ClientRequest): PublishFailure? {
        lock.withLock {
            if (lifecycle != NEW && lifecycle != RUNNING) return PublishFailure.Closed
            if (request is ClientRequest.Publish) {
                // Проверка и счёт — под lock: параллельные вызовы не превысят предел; уменьшает поток хоста
                if (queuedPublishes.value >= config.limits.commandQueue) return PublishFailure.QueueFull
                queuedPublishes.incrementAndGet()
            }
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
        return null
    }

    // ---- HostDriver: поток хоста ----

    override fun runOnce(nowMs: Long, events: List<TransportEvent>): HostStep = stepLock.withLock {
        // Аварийное завершение с чужого потока могло случиться до шага или во время него: часть потока хоста
        // доделывается здесь, сам шаг после завершения не исполняется
        val step = if (finalized.value) HostStep.Idle else step(nowMs, events)
        if (finalized.value) finishHostPart()
        step
    }

    private fun step(nowMs: Long, events: List<TransportEvent>): HostStep {
        now = nowMs
        wroteThisStep = false
        if (closeArmed) {
            // Шаг, выдавший последние действия закрытия, хост уже исполнил
            drained.complete(Unit)
            return HostStep.Idle
        }
        val out = ArrayList<HostAction>()
        gate.checkLevel()
        if (gate.drain()) {
            safeLog { readResumed(now - pausedSince) }
            engine.onCommand(EngineCommand.SetReadPaused(false), now)
            process(out)
        }
        while (true) handle(requests.tryReceive().getOrNull() ?: break, out)
        for (event in events) {
            if (packetLog) observeIncoming(event)
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
                queuedPublishes.decrementAndGet()
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
            is ClientRequest.Reauthenticate -> {
                val waiter = reauthWaiter
                if (waiter != null) {
                    // Один обмен за раз: второй вызов завершается отказом, не мешая текущему
                    request.result.complete(false)
                } else {
                    reauthWaiter = request.result
                    engine.onCommand(EngineCommand.Reauthenticate, now)
                    process(out)
                }
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
                cancelAuth()
                wroteThisStep = false
                if (packetLog) incomingPackets.reset()
                out += HostAction.Connect(action.host, action.port, action.timeoutMs)
            }
            is EngineAction.Write -> {
                wroteThisStep = true
                if (packetLog) {
                    outgoingPackets.reset()
                    outgoingPackets.feed(action.bytes)
                }
                out += HostAction.Write(action.bytes)
            }
            EngineAction.CloseTransport -> {
                cancelAuth()
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
            // Нет места в буфере — ворота удержат по порядку и приостановят чтение ([IncomingGate])
            is EngineAction.Emit -> gate.offer(action.incoming)
            is EngineAction.State -> onState(action.state)
            is EngineAction.Complete -> publishes.remove(action.ticket)?.complete(action.result)
            is EngineAction.Log -> log(action.event)
            is EngineAction.TopicStates -> subs.onTopicStates(action.states)
            is EngineAction.SubscriptionRefused -> subs.onRefused(action.refusal)
            is EngineAction.GroupRejected -> {
                rejectedGroup = action.group
                safeLog { groupRejected(action.filters, action.limit) }
            }
            is EngineAction.AuthChallenge -> startAuth(action)
            is EngineAction.ReauthComplete -> {
                reauthWaiter?.complete(action.success)
                reauthWaiter = null
            }
        }
    }

    /** Вызов брокера: ответ провайдера — вне потока хоста, результат — запросом в очередь. */
    private fun startAuth(challenge: EngineAction.AuthChallenge) {
        val auth = config.enhancedAuth ?: return
        val scope = authScope ?: return
        cancelAuth()
        authJob = scope.launch {
            val response = try {
                EngineCommand.AuthResponse(challenge.connection, auth.respond(challenge.method, challenge.data))
            } catch (e: Throwable) {
                // Отменён (новый вызов, конец подключения, закрытие) — ответа нет; иначе — отказ без текста ошибки (H11)
                currentCoroutineContext().ensureActive()
                EngineCommand.AuthResponse(challenge.connection, null, failed = true)
            }
            submit(ClientRequest.Command(response))
        }
    }

    private fun cancelAuth() {
        authJob?.cancel()
        authJob = null
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
            is EngineLogEvent.Resent -> resent(event.count)
        }
    }

    private fun observeIncoming(event: TransportEvent) {
        when (event) {
            TransportEvent.Connected -> incomingPackets.reset()
            is TransportEvent.Bytes -> incomingPackets.feed(event.bytes)
            else -> Unit
        }
    }

    private fun logPacket(direction: PacketDirection, type: MQTTControlPacketType, size: Int) =
        safeLog { packet(direction, type, size) }

    private inline fun safeLog(block: io.github.davidepianca98.client.MqttLogger.() -> Unit) {
        try {
            config.logger.block()
        } catch (_: Throwable) {
            // Исключение журнала не должно останавливать поток хоста
        }
    }

    /**
     * Клиент закончен: незавершённые запросы и ожидания — с отказом, `incoming` завершается (дочитываются канал и
     * удержанные сверх него, включая `SessionEnded`). Один раз: из [onStopped] (поток хоста), из [close] после
     * остановки хоста или без запуска хоста, из [submit] при отказе запуска. Поток хоста не работает либо это он сам,
     * поэтому замок шага свободен (или уже наш).
     */
    private fun finish(error: MqttError?) {
        stepLock.withLock {
            finishShared(error)
            finishHostPart()
            finishSignals()
        }
    }

    /**
     * Аварийный путь [close]: `host.close` не дождался выхода потока хоста, он может ещё исполнять шаг. Замок шага
     * свободен (ждём его не дольше [FOREIGN_LOCK_WAIT_MS]: поток мог как раз выходить через [onStopped]) — завершаем
     * целиком. Занят дольше (поток хоста в шаге, например в зависшем `MqttLogger`) — только
     * потокобезопасная часть: `close()` не ждёт поток хоста; состояние потока хоста (публикации, ожидания, удержанные
     * входящие) он доделает сам, выйдя из шага ([runOnce]). Удержанные сверх канала при этом могут не дойти до
     * сборщика: канал закрывается сразу, чтобы `incoming` завершился и при навсегда зависшем потоке.
     */
    private suspend fun finishFromForeignThread(error: MqttError?) {
        var waited = 0L
        while (!stepLock.tryLock()) {
            if (waited >= FOREIGN_LOCK_WAIT_MS) {
                finishShared(error)
                gate.closeChannel()
                finishSignals()
                return
            }
            delay(FOREIGN_LOCK_POLL_MS)
            waited += FOREIGN_LOCK_POLL_MS
        }
        try {
            finishShared(error)
            finishHostPart()
            finishSignals()
        } finally {
            stepLock.unlock()
        }
    }

    /** Потокобезопасная часть: жизненный цикл, очередь запросов. Один раз. */
    private fun finishShared(error: MqttError?) {
        if (!finalized.compareAndSet(false, true)) return
        finishError.value = error
        lock.withLock { lifecycle = CLOSED }
        authScope?.cancel()
        while (true) reject(requests.tryReceive().getOrNull() ?: break)
        requests.close()
    }

    /** Состояние потока хоста — только под [stepLock]. Один раз. */
    private fun finishHostPart() {
        if (!hostFinished.compareAndSet(false, true)) return
        for (pending in publishes.values.toList()) pending.complete(PublishResult.Failed(PublishFailure.Closed))
        publishes.clear()
        val failed = ConnectResult.Failed(finishError.value ?: closedError())
        for (waiter in connectWaiters.toList()) waiter.complete(failed)
        connectWaiters.clear()
        subs.failAll()
        reauthWaiter?.complete(false)
        reauthWaiter = null
        closeState()
        gate.close()
    }

    private fun finishSignals() {
        closeState()
        drained.complete(Unit)
        closedSignal.complete(Unit)
    }

    private fun closeState() {
        val error = finishError.value
        val current = stateFlow.value
        if (current !is ConnectionState.Closed || (error != null && current.error != error)) {
            stateFlow.value = ConnectionState.Closed(error)
        }
    }

    private fun reject(request: ClientRequest) {
        when (request) {
            is ClientRequest.Publish -> {
                queuedPublishes.decrementAndGet()
                request.result.complete(PublishResult.Failed(PublishFailure.Closed))
            }
            is ClientRequest.AwaitConnect -> request.result.complete(ConnectResult.Failed(closedError()))
            is ClientRequest.AwaitApplied -> request.result.completeExceptionally(IllegalStateException("MqttClient closed"))
            is ClientRequest.TemporaryStart -> request.refused.completeExceptionally(IllegalStateException("MqttClient closed"))
            is ClientRequest.Reauthenticate -> request.result.complete(false)
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

        /** Аварийный путь [close]: сколько ждать замок шага, прежде чем завершить без потока хоста. */
        const val FOREIGN_LOCK_WAIT_MS: Long = 200
        private const val FOREIGN_LOCK_POLL_MS: Long = 5

        /** Текст отказа при `tls != null` и хосте без TLS (без адресов и секретов). */
        const val TLS_HOST_REQUIRED: String =
            "MqttClientOptions.tls is set, but the host does not apply TLS: use the platform factory MqttClient(options)"

        /** Текст отказа при `webSocket != null` и хосте без WebSocket (без адресов и секретов). */
        const val WEBSOCKET_HOST_REQUIRED: String =
            "MqttClientOptions.webSocket is set, but the host does not apply WebSocket: use MqttClient(options) on JVM or JS"

        /** Текст отказа при `tls.checkServerCertificate = false` и `verifyHostname = true` (без адресов и секретов). */
        const val TRUST_ALL_NEEDS_NO_HOSTNAME: String =
            "MqttClientOptions: trust-all TLS (checkServerCertificate = false) needs an explicit verifyHostname = false"

        /** Номер временной группы — растёт в процессе. */
        fun nextTemporary(): Long = temporaryCounter.incrementAndGet()

        private fun closedError(): MqttError = MqttError.Io("closed", null)
    }
}
