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
 *   ждущие PUBACK → `Failed(ConnectionLost)` (при `cleanStart = true` не досылаются), `State(Waiting)` и дедлайн
 *   переподключения.
 * - Продолжение сессии (SEVEN-251, `session.cleanStart = false`): каждая принятая ядром публикация QoS ≥ 1 хранится с
 *   packetId и дедлайном TTL (от вызова `publish`) до PUBACK/PUBCOMP, не больше [EngineOptions.maxUnacknowledged]
 *   (сверх — ждёт в оффлайн-очереди). Обрыв не завершает ждущих `Delivered` — они ждут следующего CONNACK, кроме
 *   помеченных [EngineCommand.Publish.resend] = `false` (`Failed(ConnectionLost)` сразу, никогда не досылаются).
 *   Без сессии TTL идёт: истёкшие → `Expired` (получившие `Sent` забываются). CONNACK с Session Present = 1 —
 *   досылка по порядку с DUP = 1 и прежним packetId (QoS 2 после PUBREC — PUBREL), раньше подписок и очереди; новые
 *   публикации ждут, пока не дослано всё. Session Present = 0 — ждущие → `Failed(SessionLost)`, ничего не досылается.
 *   Исчерпаны попытки (`maxAttempts`) — ждущие досылки → `Failed(ConnectionLost)`; [EngineCommand.Close] —
 *   `Failed(Closed)`; снятая ([EngineCommand.Cancel]) не досылается. Инвариант: публикация, получившая
 *   `Failed`/`Expired`, никогда больше не пишется в сеть; досылаются только ждущие исхода и получившие `Sent`.
 *   Хранилище живёт в памяти движка: новый клиент или перезапуск процесса его не наследует.
 * - Задержка — [io.github.davidepianca98.client.ReconnectPolicy] (`ReconnectBackoff` 1:1), отсчёт от конца
 *   подключения. Сброс счётчика — только после стабильной сессии: все SUBSCRIBE подключения подтверждены SUBACK без
 *   отказов **или** после CONNACK прошло ≥ `stableAfter` (`MQTTConnection.kt:341,380-381`); проверка — в конце
 *   подключения. Счётчик исчерпан (`maxAttempts`) — `State(Closed(error))`.
 * - Переподключение при паузе чтения (S06b-T05g): пока действует [EngineCommand.SetReadPaused] `(true)`, движок в
 *   `Waiting` не подключается и не выдаёт срока переподключения ([nextDeadline]) — новое подключение с приостановленным
 *   чтением не прочло бы CONNACK и кончилось бы `ConnackTimeout`, скрыв настоящую причину. Состояние остаётся одним
 *   `Waiting` с исходной `lastError` (после предела паузы — [MqttError.ConsumerStalled]). После `SetReadPaused(false)`
 *   попытка — на ближайшем [onTick], не раньше срока задержки (отсчёт по-прежнему от конца подключения). Пауза, начатая
 *   до разрыва по другой причине (EOF, ошибка), держит переподключение так же; `lastError` тогда — эта причина, предел
 *   паузы в `Waiting` не считается. Потребитель не придёт никогда — `Waiting` без сетевой активности до [EngineCommand.Close].
 * - Публикация в живой сессии отдаётся ядру сразу, иначе ждёт в [OfflineQueue] до TTL. `Sent` выдаётся, когда
 *   PUBLISH отдан на запись ([EngineAction.Write] стоит перед [EngineAction.Complete]); подтверждения записи от хоста
 *   нет. `until = Delivered` ждёт PUBACK; у QoS 0 PUBACK не бывает — исход `Sent`. FlowControlled (окно Receive
 *   Maximum брокера) — публикация и всё за ней ждут в очереди до следующего PUBACK (TTL идёт).
 * - [EngineCommand.Close] исполняется в той же FIFO (H2): всё, что поставлено раньше, уже отдано ядру, затем
 *   DISCONNECT, ожидание EOF брокера (или дедлайна `closeLingerMs` ядра), `CloseTransport`, `State(Closed(null))`.
 *   Неотправленные публикации и ждущие PUBACK → `Failed(Closed)`. Без живой сессии — закрытие сразу.
 *
 * Подписки — [SubscriptionSet] (S06b-T03b): группы и их разница в живой сессии, весь набор после CONNACK, состояние
 * фильтров ([EngineAction.TopicStates]), отказы SUBACK без разрыва ([EngineAction.SubscriptionRefused], H7),
 * [EngineCommand.Resubscribe], предел [EngineOptions.maxFilters]; стабильность сессии — его счётчик SUBACK.
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
    private val awaitingAck = HashMap<Int, EngineCommand.Publish>()
    private val pendingSent = ArrayList<EngineCommand.Publish>() // приняты ядром, Complete — после Write

    // Продолжение сессии (SEVEN-251, только cleanStart = false)
    private val persistent = !options.session.cleanStart
    private val unacked = LinkedHashMap<Int, Unacked>() // QoS ≥ 1, приняты живым ядром, ждут PUBACK/PUBCOMP
    private val resume = LinkedHashMap<Int, Unacked>() // после обрыва: ждут досылки в следующем подключении сессии
    private var carriedIncomingQos2: Set<Int> = emptySet()

    /**
     * Неподтверждённая исходящая QoS ≥ 1 продолжаемой сессии: [deadlineMs] — TTL от вызова `publish`, [waiting] —
     * вызывающий ещё ждёт исхода (`until = Delivered`), [released] — на QoS 2 пришёл PUBREC (досылается PUBREL).
     */
    private class Unacked(val command: EngineCommand.Publish, val deadlineMs: Long, val waiting: Boolean) {
        var released = false
    }

    // Обратное давление
    private var readPaused = false
    private var pausedSince = 0L

    private val subscriptions = SubscriptionSet(options.subscribeBatch, options.maxFilters, actions, persistent)

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
        // Пауза чтения держит переподключение (S06b-T05g): срока нет, хост не просыпается, пока потребитель не разберёт буфер
        if (phase == Phase.Waiting && !readPaused) take(reconnectAt)
        take(core?.nextDeadline())
        take(stallDeadline())
        for (entry in resume.values) take(entry.deadlineMs)
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
        expireResume(nowMs)
        val wake = reconnectAt
        if (phase == Phase.Waiting && !readPaused && wake != null && nowMs >= wake) connect(nowMs)
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
                is CoreOutput.SubAck -> subscriptions.onSubAck(output.packetId, output.codes)
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
        val resumed = persistent && connack.sessionPresent
        // Досылка продолжаемой сессии (SEVEN-251) — раньше всего: эти публикации вызваны раньше стоящих в очереди
        if (persistent) core?.let { resumeSession(it, resumed, nowMs) }
        // Весь набор, затем ждавшие публикации (порядок MQTTConnection: resubscribeAll → drainQueue)
        core?.let { subscriptions.sessionStarted(it, nowMs, resumed) }
        drainQueue(nowMs)
    }

    /** Конец текущего подключения (транспорт уже закрыт или закрывается действием перед вызовом). */
    private fun endConnection(error: MqttError?, nowMs: Long) {
        val old = core
        core = null
        val ended = epoch
        val lived = connectedAt?.let { nowMs - it }
        val stable = subscriptions.confirmed || (lived != null && lived >= policy.stableAfter.inWholeMilliseconds)
        epoch = null
        connectedAt = null
        subscriptions.sessionEnded()
        val lost = if (closeRequested) PublishFailure.Closed else PublishFailure.ConnectionLost
        var awaitingPubcomp: Set<Int> = emptySet()
        if (persistent) {
            if (ended != null && old != null) {
                awaitingPubcomp = old.awaitingPubcomp()
                carriedIncomingQos2 = old.incomingQos2()
            }
            // Не ушедшие в сеть получают исход ниже — после него их никогда не досылают
            forgetUnacked(pendingSent)
        }
        for (pending in pendingSent) complete(pending, PublishResult.Failed(lost))
        pendingSent.clear()
        val unconfirmed = awaitingAck.values.toList()
        awaitingAck.clear()
        for (pending in unconfirmed) complete(pending, PublishResult.Failed(lost))
        if (persistent) suspendUnacked(awaitingPubcomp, lost)
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
            failResume(PublishFailure.ConnectionLost)
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
        failResume(PublishFailure.Closed)
        for (pending in queue.drain()) complete(pending, PublishResult.Failed(PublishFailure.Closed))
    }

    // ---- публикации ----

    private fun publish(command: EngineCommand.Publish, nowMs: Long) {
        if (phase == Phase.Closed || closeRequested) {
            complete(command, PublishResult.Failed(PublishFailure.Closed))
            return
        }
        if (phase == Phase.Connected && queue.isEmpty() && resume.isEmpty() &&
            offer(command, OfflineQueue.deadlineOf(command, nowMs), nowMs)
        ) {
            return
        }
        if (!queue.add(command, nowMs)) complete(command, PublishResult.Failed(PublishFailure.QueueFull))
    }

    /**
     * Отдать публикацию ядру; `false` — окно брокера занято (FlowControlled) или, в продолжаемой сессии, хранилище
     * неподтверждённых полно ([EngineOptions.maxUnacknowledged]): публикация остаётся ждать. [deadlineMs] — TTL от
     * вызова `publish` (хранится для решения о досылке после обрыва).
     */
    private fun offer(command: EngineCommand.Publish, deadlineMs: Long, nowMs: Long): Boolean {
        val current = core ?: return false
        val tracked = persistent && command.qos != Qos.AT_MOST_ONCE
        if (tracked && unacked.size >= options.maxUnacknowledged) return false
        val admission = current.publish(OutgoingPublish(command.topic, command.payload, command.qos, command.retain), nowMs)
        when (admission) {
            is PublishAdmission.Accepted -> {
                val packetId = admission.packetId
                if (tracked && packetId != null) {
                    val waiting = command.until == PublishStage.Delivered
                    unacked[packetId] = Unacked(command, deadlineMs, waiting)
                    if (!waiting) pendingSent += command
                } else if (command.until == PublishStage.Delivered && packetId != null) {
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
        // Пока не дослано неподтверждённое прошлого подключения, новые ждут за ним (FIFO вызовов)
        while (phase == Phase.Connected && resume.isEmpty()) {
            val head = queue.peek() ?: return
            if (!offer(head.command, head.deadlineMs, nowMs)) return
            queue.removeFirst()
        }
    }

    private fun onPubAck(packetId: Int, reason: ReasonCode, nowMs: Long) {
        val command = if (persistent) {
            unacked.remove(packetId)?.takeIf { it.waiting }?.command
        } else {
            awaitingAck.remove(packetId)
        }
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
        drainResume(nowMs)
        drainQueue(nowMs)
    }

    private fun cancel(ticket: PublishTicket) {
        if (queue.remove(ticket)) return
        if (persistent) {
            // Вызывающий ушёл: исход никому не нужен, а TTL запрещает позднее исполнение — не досылать
            unacked.entries.firstOrNull { it.value.command.ticket == ticket }?.let {
                unacked.remove(it.key)
                return
            }
            resume.entries.firstOrNull { it.value.command.ticket == ticket }?.let {
                resume.remove(it.key)
                core?.releasePacketId(it.key)
                return
            }
        }
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
        // Не ушедшие в сеть получают ConnectionLost — после исхода их никогда не досылают
        if (lost && persistent) forgetUnacked(pendingSent)
        val sentEpoch = epoch ?: 0
        for (command in pendingSent) {
            complete(command, if (lost) PublishResult.Failed(PublishFailure.ConnectionLost) else PublishResult.Sent(sentEpoch))
        }
        pendingSent.clear()
    }

    // ---- продолжение сессии (SEVEN-251) ----

    /**
     * CONNACK продолжаемой сессии. Session Present = 1 — брокер помнит сессию: зарезервировать packetId ждущих досылки,
     * вернуть состояние входящих QoS 2 и дослать ([drainResume]). Session Present = 0 — сессия на брокере потеряна:
     * всё неподтверждённое прошлых подключений (могло дойти, могло нет) завершается `Failed(SessionLost)`, а уже
     * получившие `Sent` забываются; повторно не отправляется ничего.
     */
    private fun resumeSession(live: MqttSessionCore, sessionPresent: Boolean, nowMs: Long) {
        if (!sessionPresent) {
            carriedIncomingQos2 = emptySet()
            failResume(PublishFailure.SessionLost)
            return
        }
        live.restoreIncomingQos2(carriedIncomingQos2)
        live.reservePacketIds(resume.keys)
        drainResume(nowMs)
    }

    /**
     * Досылка по порядку: PUBLISH с DUP = 1 и прежним packetId (или PUBREL для QoS 2 после PUBREC), пока не кончится
     * окно брокера или предел [EngineOptions.maxUnacknowledged]. Истёкшие по TTL не досылаются (`Expired`), отклонённые
     * ядром (CONNACK нового подключения объявил другие пределы) — `Failed(Rejected)`.
     */
    private fun drainResume(nowMs: Long) {
        if (resume.isEmpty() || phase != Phase.Connected) return
        val live = core ?: return
        expireResume(nowMs)
        var resent = 0
        val iterator = resume.entries.iterator()
        while (iterator.hasNext()) {
            val (packetId, entry) = iterator.next()
            if (entry.released) {
                live.resendPubrel(packetId, nowMs)
            } else {
                if (unacked.size >= options.maxUnacknowledged) break
                val command = entry.command
                val outgoing = OutgoingPublish(command.topic, command.payload, command.qos, command.retain)
                when (val admission = live.resendPublish(outgoing, packetId, nowMs)) {
                    is PublishAdmission.Accepted -> Unit
                    PublishAdmission.FlowControlled -> break
                    is PublishAdmission.Rejected -> {
                        iterator.remove()
                        live.releasePacketId(packetId)
                        if (entry.waiting) {
                            val code = admission.reason.toReasonCode()
                            complete(command, PublishResult.Failed(PublishFailure.Rejected(code)))
                        }
                        continue
                    }
                }
            }
            iterator.remove()
            unacked[packetId] = entry
            resent++
        }
        if (resent > 0) actions += EngineAction.Log(EngineLogEvent.Resent(resent))
    }

    /** Ждущие досылки с истёкшим TTL: ждущим исхода — `Expired`, получившие `Sent` забываются. */
    private fun expireResume(nowMs: Long) {
        if (resume.isEmpty()) return
        val iterator = resume.entries.iterator()
        while (iterator.hasNext()) {
            val (packetId, entry) = iterator.next()
            if (entry.deadlineMs > nowMs) continue
            iterator.remove()
            core?.releasePacketId(packetId)
            if (entry.waiting) complete(entry.command, PublishResult.Expired)
        }
    }

    /**
     * Конец подключения продолжаемой сессии: неподтверждённые переходят в ожидание досылки ([resume]), кроме
     * помеченных «не досылать» ([EngineCommand.Publish.resend] = `false`) и случая закрытия: тем исход [lost] сразу.
     * Порядок сохраняется: пока [resume] не пуст, новые публикации ядру не отдаются, поэтому в [unacked] лежат только
     * более ранние (уже досланные).
     */
    private fun suspendUnacked(awaitingPubcomp: Set<Int>, lost: PublishFailure) {
        if (unacked.isEmpty()) return
        val next = LinkedHashMap<Int, Unacked>()
        for ((packetId, entry) in unacked) {
            if (closeRequested || !entry.command.resend) {
                if (entry.waiting) complete(entry.command, PublishResult.Failed(lost))
                continue
            }
            entry.released = packetId in awaitingPubcomp
            next[packetId] = entry
        }
        unacked.clear()
        next.putAll(resume)
        resume.clear()
        resume.putAll(next)
    }

    /** Всё, что ждёт досылки, — с исходом [reason] (ждущим исхода); получившие `Sent` забываются. */
    private fun failResume(reason: PublishFailure) {
        if (resume.isEmpty()) return
        val pending = resume.values.toList()
        resume.clear()
        for (entry in pending) if (entry.waiting) complete(entry.command, PublishResult.Failed(reason))
    }

    /** Убрать из хранилища публикации, которые получат исход, не уйдя в сеть (их никогда не досылают). */
    private fun forgetUnacked(commands: List<EngineCommand.Publish>) {
        if (commands.isEmpty() || unacked.isEmpty()) return
        unacked.values.removeAll { entry -> commands.any { it === entry.command } }
    }

    // ---- подписки ----

    private fun setGroup(command: EngineCommand.SetGroup, nowMs: Long) {
        subscriptions.set(command.group, command.filters, command.qos, command.refreshShared, liveCore(), nowMs)
    }

    private fun resubscribe(group: String?, nowMs: Long) = subscriptions.resubscribe(group, liveCore(), nowMs)

    /** Ядро живой сессии (после CONNACK, до закрытия): только ему отдаются SUBSCRIBE/UNSUBSCRIBE. */
    private fun liveCore(): MqttSessionCore? = if (phase == Phase.Connected) core else null

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
