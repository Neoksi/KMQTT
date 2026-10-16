package io.github.davidepianca98.client.engine

import io.github.davidepianca98.client.ConnectionState
import io.github.davidepianca98.client.MqttError
import io.github.davidepianca98.client.MqttIncoming
import io.github.davidepianca98.client.PublishFailure
import io.github.davidepianca98.client.PublishResult
import io.github.davidepianca98.client.PublishStage
import io.github.davidepianca98.client.ServerInfo
import io.github.davidepianca98.client.TransportEvent
import io.github.davidepianca98.client.core.ConnackInfo
import io.github.davidepianca98.client.core.CoreOutput
import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.core.MqttSessionCore
import io.github.davidepianca98.client.core.OutgoingPublish
import io.github.davidepianca98.client.core.PublishAdmission
import io.github.davidepianca98.client.core.PublishRejection
import io.github.davidepianca98.client.core.SessionSettings
import io.github.davidepianca98.client.core.SubscriptionRequest
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import kotlin.time.Duration.Companion.milliseconds

/**
 * Движок клиента (W6 §2.4, S06b-T03a): над ядром [MqttSessionCore] — цикл подключения и переподключения, стабильность
 * сессии, эпохи, оффлайн-очередь публикаций с TTL и отменой, штатное закрытие. Без ввода-вывода, потоков и часов:
 * входы — [onCommand], [onTransport], [onTick] с временем `nowMs`; выходы — [takeActions] (по порядку) и
 * [nextDeadline]. Однопоточный: все методы — с одного потока (цикла хоста). Не логирует (события — [EngineAction.Log]).
 *
 * Перенос поведения `MQTTConnection.runLoop/runSession` приложения:
 * - Подключение: [EngineAction.Connect] → `TransportEvent.Connected` → **новое ядро** на каждое подключение, CONNECT →
 *   CONNACK → эпоха из `nextEpoch()`, `State(Connected)`, `Emit(SessionStarted)`, подписка всего набора порциями
 *   [EngineOptions.subscribeBatch], затем оффлайн-очередь по порядку (`resubscribeAll` → `drainQueue`).
 * - Конец подключения (ошибка ядра, `TransportEvent.Failed`, EOF): `Emit(SessionEnded)` (только если был CONNACK),
 *   ждущие PUBACK → `Failed(ConnectionLost)` (не досылаются: `cleanStart`), `State(Waiting)` и дедлайн переподключения.
 * - Задержка — [io.github.davidepianca98.client.ReconnectPolicy] (`ReconnectBackoff` 1:1), отсчёт от конца
 *   подключения. Сброс счётчика — только после стабильной сессии: все SUBSCRIBE подключения подтверждены SUBACK без
 *   отказов **или** после CONNACK прошло ≥ `stableAfter` (`MQTTConnection.kt:341,380-381`); проверка — в конце
 *   подключения. Счётчик исчерпан (`maxAttempts`) — `State(Closed(error))`.
 * - Публикация в живой сессии отдаётся ядру сразу, иначе ждёт в [OfflineQueue] до TTL. `Sent` выдаётся, когда
 *   PUBLISH отдан на запись ([EngineAction.Write] стоит перед [EngineAction.Complete]); подтверждения записи от хоста
 *   нет. `until = Delivered` ждёт PUBACK; у QoS 0 PUBACK не бывает — исход `Sent`. FlowControlled (окно Receive
 *   Maximum брокера) — публикация и всё за ней ждут в очереди до следующего PUBACK (TTL идёт).
 * - [EngineCommand.Close] исполняется в той же FIFO (H2): всё, что поставлено раньше, уже отдано ядру, затем
 *   DISCONNECT, ожидание EOF брокера (или дедлайна `closeLingerMs` ядра), `CloseTransport`, `State(Closed(null))`.
 *   Неотправленные публикации и ждущие PUBACK → `Failed(Closed)`. Без живой сессии — закрытие сразу.
 *
 * Подписки здесь — минимальный набор для стабильности (группы, весь набор после CONNACK, разница в живой сессии,
 * повтор); состояние фильтров, отказы по фильтрам и временные группы — S06b-T03b (`SubscriptionSet`).
 */
@MqttCoreApi
public class MqttClientEngine(
    private val options: EngineOptions,
    private val nextEpoch: () -> Long,
    private val coreFactory: (SessionSettings, Long) -> MqttSessionCore = { settings, now -> MqttSessionCore(settings, now) },
) {
    private enum class Phase { Idle, Connecting, Connected, Closing, Waiting, Closed }

    private var phase = Phase.Idle
    private val actions = ArrayList<EngineAction>()
    private val queue = OfflineQueue(options.offlineCapacity)
    private val policy = options.reconnect

    /** Текущее состояние (последнее выданное [EngineAction.State]). */
    public var state: ConnectionState = ConnectionState.Idle
        private set

    // Переподключение: число задержек с последнего сброса (attempt у ReconnectBackoff) и срок следующей попытки
    private var attempt = 0
    private var reconnectAt: Long? = null
    private var closeRequested = false

    // Текущее подключение
    private var core: MqttSessionCore? = null
    private var epoch: Long? = null
    private var connectedAt: Long? = null
    private var subscribesSent = 0
    private var subacksOk = 0
    private var subscriptionsConfirmed = false
    private val awaitingAck = HashMap<Int, EngineCommand.Publish>()
    private val pendingSent = ArrayList<EngineCommand.Publish>() // приняты ядром, Complete — после Write

    // Обратное давление
    private var readPaused = false
    private var pausedSince = 0L

    // Подписки (минимум T03a; T03b заменяет на SubscriptionSet): группа → фильтр → QoS; подписанное в этой сессии
    private val groups = LinkedHashMap<String, Map<String, Qos>>()
    private var applied: Map<String, Qos> = emptyMap()

    /** Забрать накопленные действия (по порядку). */
    public fun takeActions(): List<EngineAction> {
        if (actions.isEmpty()) return emptyList()
        return ArrayList(actions).also { actions.clear() }
    }

    /** Ближайший момент, когда нужен [onTick]: TTL очереди, переподключение, сроки ядра, предел паузы; null — нет. */
    public fun nextDeadline(): Long? {
        var result = queue.nextDeadline()
        fun take(value: Long?) {
            if (value != null && (result == null || value < result!!)) result = value
        }
        if (phase == Phase.Waiting) take(reconnectAt)
        take(core?.nextDeadline())
        take(stallDeadline())
        return result
    }

    public fun onCommand(command: EngineCommand, nowMs: Long) {
        when (command) {
            EngineCommand.Start -> if (phase == Phase.Idle) connect(nowMs)
            is EngineCommand.Publish -> publish(command, nowMs)
            is EngineCommand.Cancel -> cancel(command.ticket)
            is EngineCommand.SetGroup -> setGroup(command, nowMs)
            is EngineCommand.Resubscribe -> resubscribe(command.group, nowMs)
            is EngineCommand.SetReadPaused -> setReadPaused(command.paused, nowMs)
            is EngineCommand.Close -> close(command.reason, nowMs)
        }
        flush()
    }

    public fun onTransport(event: TransportEvent, nowMs: Long) {
        if (phase != Phase.Connecting && phase != Phase.Connected && phase != Phase.Closing) return
        when (event) {
            TransportEvent.Connected -> if (core == null && phase == Phase.Connecting) {
                val created = coreFactory(options.session, nowMs)
                core = created
                if (readPaused) created.setReadPaused(true, nowMs)
                handle(created, created.start(nowMs), nowMs)
            }
            is TransportEvent.Bytes -> core?.let { handle(it, it.onBytes(event.bytes, 0, event.bytes.size, nowMs), nowMs) }
            TransportEvent.Eof -> core?.let { handle(it, it.onEof(nowMs), nowMs) }
            // Хост уже закрыл канал: ядро отбрасывается без onEof, подключение кончается этой ошибкой
            is TransportEvent.Failed -> endConnection(event.error, nowMs)
        }
        flush()
    }

    public fun onTick(nowMs: Long) {
        for (expired in queue.expire(nowMs)) complete(expired, PublishResult.Expired)
        val wake = reconnectAt
        if (phase == Phase.Waiting && wake != null && nowMs >= wake) connect(nowMs)
        core?.let { handle(it, it.onTick(nowMs), nowMs) }
        val stall = stallDeadline()
        if (stall != null && nowMs >= stall) {
            actions += EngineAction.CloseTransport
            endConnection(MqttError.ConsumerStalled, nowMs)
        }
        flush()
    }

    // ---- подключение ----

    private fun connect(nowMs: Long) {
        phase = Phase.Connecting
        reconnectAt = null
        setState(ConnectionState.Connecting(attempt + 1))
        actions += EngineAction.Connect(options.host, options.port, options.connectTimeoutMs)
        // Пауза чтения относится к текущему транспорту хоста: после нового Connect — заново
        if (readPaused) {
            actions += EngineAction.PauseRead(true)
            pausedSince = nowMs
        }
    }

    private fun handle(source: MqttSessionCore, outputs: List<CoreOutput>, nowMs: Long) {
        for (output in outputs) {
            if (core !== source) return
            when (output) {
                is CoreOutput.Connected -> onConnack(output.connack, nowMs)
                is CoreOutput.Message -> epoch?.let {
                    actions += EngineAction.Emit(MqttIncoming.Message(output.topic, output.payload, output.qos, output.retain, it))
                }
                is CoreOutput.PubAck -> onPubAck(output.packetId, output.reason, nowMs)
                is CoreOutput.SubAck -> onSubAck(output.codes)
                is CoreOutput.UnsubAck -> Unit
                is CoreOutput.Dropped -> actions += EngineAction.Log(EngineLogEvent.Dropped(output.reason))
                is CoreOutput.Closed -> {
                    // Штатное закрытие оставляет DISCONNECT в исходящих; при ошибке там может быть DISCONNECT(code) MQTT 5
                    flush(lost = output.error != null)
                    actions += EngineAction.CloseTransport
                    endConnection(output.error, nowMs)
                }
            }
        }
    }

    private fun onConnack(connack: ConnackInfo, nowMs: Long) {
        val current = nextEpoch()
        phase = Phase.Connected
        epoch = current
        connectedAt = nowMs
        setState(ConnectionState.Connected(current, connack.sessionPresent, connack.toServerInfo()))
        actions += EngineAction.Emit(MqttIncoming.SessionStarted(current))
        actions += EngineAction.Log(EngineLogEvent.Connected(current))
        // Весь набор, затем ждавшие публикации (порядок MQTTConnection: resubscribeAll → drainQueue)
        applied = desired()
        subscribe(applied, nowMs)
        drainQueue(nowMs)
    }

    /** Конец текущего подключения (транспорт уже закрыт или закрывается действием перед вызовом). */
    private fun endConnection(error: MqttError?, nowMs: Long) {
        core = null
        val ended = epoch
        val lived = connectedAt?.let { nowMs - it }
        val stable = subscriptionsConfirmed || (lived != null && lived >= policy.stableAfter.inWholeMilliseconds)
        epoch = null
        connectedAt = null
        subscribesSent = 0
        subacksOk = 0
        subscriptionsConfirmed = false
        applied = emptyMap()
        val lost = if (closeRequested) PublishFailure.Closed else PublishFailure.ConnectionLost
        for (pending in pendingSent) complete(pending, PublishResult.Failed(lost))
        pendingSent.clear()
        val unacked = awaitingAck.values.toList()
        awaitingAck.clear()
        for (pending in unacked) complete(pending, PublishResult.Failed(lost))
        if (ended != null) actions += EngineAction.Emit(MqttIncoming.SessionEnded(ended, error))
        actions += EngineAction.Log(EngineLogEvent.Disconnected(error))
        if (closeRequested) {
            finishClose()
            return
        }
        if (stable) attempt = 0
        val lastError = error ?: MqttError.Io("closed", null)
        val limit = policy.maxAttempts
        if (limit != null && attempt >= limit) {
            phase = Phase.Closed
            failQueue()
            setState(ConnectionState.Closed(lastError))
            return
        }
        val delayMs = policy.delayMs(attempt)
        attempt++
        phase = Phase.Waiting
        reconnectAt = nowMs + delayMs
        setState(ConnectionState.Waiting(attempt + 1, delayMs.milliseconds, lastError))
        actions += EngineAction.Log(EngineLogEvent.ReconnectIn(delayMs))
    }

    private fun close(reason: ReasonCode, nowMs: Long) {
        if (phase == Phase.Closed || closeRequested) return
        closeRequested = true
        when (phase) {
            Phase.Idle, Phase.Waiting -> finishClose()
            Phase.Connecting -> {
                actions += EngineAction.CloseTransport
                endConnection(null, nowMs)
            }
            Phase.Connected -> {
                phase = Phase.Closing
                val current = core ?: return
                handle(current, current.disconnect(reason, nowMs), nowMs)
            }
            Phase.Closing, Phase.Closed -> Unit
        }
    }

    private fun finishClose() {
        phase = Phase.Closed
        reconnectAt = null
        failQueue()
        setState(ConnectionState.Closed(null))
    }

    private fun failQueue() {
        for (pending in queue.drain()) complete(pending, PublishResult.Failed(PublishFailure.Closed))
    }

    // ---- публикации ----

    private fun publish(command: EngineCommand.Publish, nowMs: Long) {
        if (phase == Phase.Closed || closeRequested) {
            complete(command, PublishResult.Failed(PublishFailure.Closed))
            return
        }
        if (phase == Phase.Connected && queue.isEmpty() && offer(command, nowMs)) return
        if (!queue.add(command, nowMs)) complete(command, PublishResult.Failed(PublishFailure.QueueFull))
    }

    /** Отдать публикацию ядру; `false` — окно брокера занято (FlowControlled), публикация остаётся ждать. */
    private fun offer(command: EngineCommand.Publish, nowMs: Long): Boolean {
        val current = core ?: return false
        val admission = current.publish(OutgoingPublish(command.topic, command.payload, command.qos, command.retain), nowMs)
        when (admission) {
            is PublishAdmission.Accepted -> {
                val packetId = admission.packetId
                if (command.until == PublishStage.Delivered && packetId != null) {
                    awaitingAck[packetId] = command
                } else {
                    pendingSent += command
                }
            }
            is PublishAdmission.Rejected ->
                complete(command, PublishResult.Failed(PublishFailure.Rejected(admission.reason.toReasonCode())))
            PublishAdmission.FlowControlled -> return false
        }
        return true
    }

    private fun drainQueue(nowMs: Long) {
        for (expired in queue.expire(nowMs)) complete(expired, PublishResult.Expired)
        while (phase == Phase.Connected) {
            val head = queue.peek() ?: return
            if (!offer(head.command, nowMs)) return
            queue.removeFirst()
        }
    }

    private fun onPubAck(packetId: Int, reason: ReasonCode, nowMs: Long) {
        val command = awaitingAck.remove(packetId)
        if (reason.value >= 0x80) actions += EngineAction.Log(EngineLogEvent.PublishRefused(reason))
        if (command != null) {
            val result = if (reason.value >= 0x80) {
                PublishResult.Failed(PublishFailure.Rejected(reason))
            } else {
                PublishResult.Delivered(epoch ?: 0)
            }
            complete(command, result)
        }
        // Окно брокера освободилось
        drainQueue(nowMs)
    }

    private fun cancel(ticket: PublishTicket) {
        if (queue.remove(ticket)) return
        val packetId = awaitingAck.entries.firstOrNull { it.value.ticket == ticket }?.key ?: return
        awaitingAck.remove(packetId)
    }

    private fun complete(command: EngineCommand.Publish, result: PublishResult) {
        actions += EngineAction.Complete(command.ticket, result)
    }

    /** Исходящие ядра — в [EngineAction.Write], затем `Sent` принятых публикаций ([lost] — байты не ушли). */
    private fun flush(lost: Boolean = false) {
        core?.takeOutbound()?.let { actions += EngineAction.Write(it) }
        if (pendingSent.isEmpty()) return
        val sentEpoch = epoch ?: 0
        for (command in pendingSent) {
            complete(command, if (lost) PublishResult.Failed(PublishFailure.ConnectionLost) else PublishResult.Sent(sentEpoch))
        }
        pendingSent.clear()
    }

    // ---- подписки (минимум T03a) ----

    private fun desired(): Map<String, Qos> {
        val result = LinkedHashMap<String, Qos>()
        for (filters in groups.values) {
            for ((filter, qos) in filters) {
                val known = result[filter]
                if (known == null || qos.value > known.value) result[filter] = qos
            }
        }
        return result
    }

    private fun setGroup(command: EngineCommand.SetGroup, nowMs: Long) {
        if (command.filters.isEmpty()) {
            groups.remove(command.group)
        } else {
            groups[command.group] = command.filters.associateWith { command.qos }
        }
        if (phase != Phase.Connected) return
        val target = desired()
        val removed = applied.keys.filter { it !in target }
        val added = target.filter { (filter, qos) -> applied[filter] != qos }
        applied = target
        val current = core ?: return
        if (removed.isNotEmpty()) removed.chunked(options.subscribeBatch).forEach { current.unsubscribe(it, nowMs) }
        subscribe(added, nowMs)
    }

    private fun resubscribe(group: String?, nowMs: Long) {
        if (phase != Phase.Connected) return
        val filters = if (group == null) applied else applied.filterKeys { groups[group]?.containsKey(it) == true }
        subscribe(filters, nowMs)
    }

    private fun subscribe(filters: Map<String, Qos>, nowMs: Long) {
        val current = core ?: return
        if (filters.isEmpty()) return
        actions += EngineAction.Log(EngineLogEvent.Subscribe(filters.size))
        filters.entries.chunked(options.subscribeBatch).forEach { chunk ->
            current.subscribe(chunk.map { SubscriptionRequest(it.key, it.value) }, nowMs)
            subscribesSent++
        }
    }

    private fun onSubAck(codes: List<ReasonCode>) {
        val refused = codes.count { it.value >= 0x80 }
        if (refused == 0) subacksOk++ else actions += EngineAction.Log(EngineLogEvent.Refused(refused))
        if (subscribesSent > 0 && subacksOk >= subscribesSent) subscriptionsConfirmed = true
    }

    // ---- обратное давление ----

    private fun setReadPaused(paused: Boolean, nowMs: Long) {
        if (paused == readPaused) return
        readPaused = paused
        pausedSince = nowMs
        core?.setReadPaused(paused, nowMs)
        if (phase == Phase.Connecting || phase == Phase.Connected || phase == Phase.Closing) {
            actions += EngineAction.PauseRead(paused)
        }
    }

    private fun stallDeadline(): Long? {
        val limit = options.consumerStallLimitMs ?: return null
        return if (readPaused && phase == Phase.Connected) pausedSince + limit else null
    }

    private fun setState(value: ConnectionState) {
        state = value
        actions += EngineAction.State(value)
    }

    private fun ConnackInfo.toServerInfo(): ServerInfo =
        ServerInfo(maximumQos, retainAvailable, receiveMaximum, maximumPacketSize, serverKeepAliveSec)

    private fun PublishRejection.toReasonCode(): ReasonCode? = when (this) {
        PublishRejection.NotConnected -> null
        PublishRejection.QosNotSupported -> ReasonCode.QOS_NOT_SUPPORTED
        PublishRejection.RetainNotSupported -> ReasonCode.RETAIN_NOT_SUPPORTED
        is PublishRejection.TooLarge -> ReasonCode.PACKET_TOO_LARGE
        PublishRejection.InvalidTopic -> ReasonCode.TOPIC_NAME_INVALID
    }
}
