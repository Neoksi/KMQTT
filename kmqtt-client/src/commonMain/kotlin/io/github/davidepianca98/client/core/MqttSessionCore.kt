package io.github.davidepianca98.client.core

import io.github.davidepianca98.client.DropReason
import io.github.davidepianca98.client.MqttError
import io.github.davidepianca98.generateRandomClientId
import io.github.davidepianca98.mqtt.MQTTException
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.Subscription
import io.github.davidepianca98.mqtt.packets.ConnectFlags
import io.github.davidepianca98.mqtt.packets.MQTTPacket
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTConnack
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTDisconnect
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPingresp
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPuback
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPubcomp
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPubrec
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPubrel
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTSuback
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTUnsuback
import io.github.davidepianca98.mqtt.packets.mqttv4.ConnectReturnCode
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Connack
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Connect
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Disconnect
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pingreq
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Puback
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pubcomp
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Publish
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pubrec
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pubrel
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Suback
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Subscribe
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Unsubscribe
import io.github.davidepianca98.mqtt.packets.mqttv4.SubackReturnCode
import io.github.davidepianca98.mqtt.packets.mqttv4.toReasonCode
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Auth
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Connack
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Connect
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Disconnect
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pingreq
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Properties
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Puback
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubcomp
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Publish
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubrec
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubrel
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Suback
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Subscribe
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Unsuback
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Unsubscribe
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import io.github.davidepianca98.mqtt.packets.mqttv5.SubscriptionOptions

/**
 * Протокольное ядро одного MQTT-подключения без ввода-вывода (W6 §3): байты ⇄ пакеты ⇄ события.
 *
 * Контракт:
 * - Новый экземпляр на **каждое** подключение (алиасы, packetId, неподтверждённые — поля экземпляра).
 * - Однопоточное: все методы вызываются с одного потока (цикла хоста). Замков нет.
 * - Время — только параметр `nowMs` (монотонные миллисекунды хоста); ядро не читает часы, не логирует, не делает
 *   ввода-вывода и не выделяет буферов больше лимита пакета ([MqttFramer]).
 * - Хост: после транспортного подключения — [start]; прочитанные байты — [onBytes]; EOF — [onEof]; по
 *   [nextDeadline] — [onTick]; после каждого вызова — записать [takeOutbound]. По [CoreOutput.Closed] — дописать
 *   остаток [takeOutbound] без ожидания и закрыть соединение.
 *
 * Поведение (перенос `MQTTClient.kt` по таблице W6 §3.2):
 * - CONNECT 3.1.1/5; в MQTT 5 — Maximum Packet Size = [SessionSettings.maxIncomingPacket], Receive Maximum
 *   (если ≠ 65535), Topic Alias Maximum (если > 0), для will — пустые `willProperties` (H3).
 * - CONNACK ≠ успех (любой код, в т. ч. `SERVER_MOVED`/`USE_ANOTHER_SERVER` с `serverReference`) →
 *   `Closed(Refused(code))` без перехода (H10). `serverKeepAlive` применяется. Любой пакет брокера до CONNACK,
 *   повторный CONNACK, `sessionPresent` при `cleanStart` → `Closed(Protocol(PROTOCOL_ERROR))`.
 * - keepAlive (правило S06-T08): PINGREQ, когда с последней отправки прошло 0,9·keepAlive или брокер молчит
 *   keepAlive; ответ (любой входящий байт) ждём ½·keepAlive → `Closed(KeepAliveTimeout)`. Пока чтение на паузе
 *   ([setReadPaused]) PINGREQ уходит каждые 0,9·keepAlive от последней отправки, таймаут ответа не считается;
 *   после снятия паузы отсчёт ответа и молчания начинается заново (W6 §4.5).
 * - Входящий QoS 1: PUBACK ставится в исходящие при выдаче [CoreOutput.Message]. QoS 2 — как в старом клиенте
 *   (PUBREC, повтор по packetId не выдаётся, PUBREL → PUBCOMP; неизвестный PUBREL — `PACKET_IDENTIFIER_NOT_FOUND`).
 * - Отброшенный фреймером PUBLISH (H9) подтверждается: QoS 1 — PUBACK (`SUCCESS`), QoS 2 — PUBREC (`SUCCESS`, id
 *   запоминается для PUBCOMP); без packetId (не успел прочитаться) подтвердить нечем — известный предел.
 * - Topic Alias (MQTT 5): алиас > объявленного максимума (в т. ч. при максимуме 0) → `Protocol(TOPIC_ALIAS_INVALID)`;
 *   неизвестный алиас без топика → `Protocol(PROTOCOL_ERROR)`. Если фреймер отбросил PUBLISH как
 *   `TopicTooLong` при объявленном максимуме > 0, такой PUBLISH мог задавать (или перепривязывать) алиас: таблица
 *   алиасов очищается целиком (иначе старая привязка доставила бы сообщения под чужим топиком), а дальнейшие
 *   PUBLISH с неизвестным алиасом без топика подтверждаются и выдаются как `Dropped(TopicTooLong(length = -1, ...))`
 *   (−1 — длина неизвестна), пока брокер не пришлёт алиас с топиком заново.
 * - Ошибка протокола (в т. ч. от фреймера) → в MQTT 5 в исходящие кладётся DISCONNECT с кодом ошибки (will брокер
 *   опубликует: код ≠ 0), в 3.1.1 — ничего (DISCONNECT 3.1.1 отменил бы will); затем `Closed(Protocol(code))`.
 * - DISCONNECT брокера (любой код) → `Closed(ServerDisconnect(code))`; в 3.1.1 (брокер его не шлёт) — код `SUCCESS`.
 * - Входящий AUTH → `Closed(Protocol(PROTOCOL_ERROR))`: расширенная аутентификация не переносится (нецель).
 *
 * Отличия от `MQTTClient` (сознательные):
 * - Проверки `requestProblemInformation == 0` не переносятся: ядро это свойство не отправляет (по умолчанию 1),
 *   так что у старого клиента они тоже не срабатывали.
 * - Проверка «QoS входящего PUBLISH ≤ Maximum QoS брокера» не переносится: Maximum QoS ограничивает публикации
 *   клиента, а не брокера (QoS входящих задаёт подписка).
 * - Публикация до CONNACK не копится в ядре (`Rejected(NotConnected)`): оффлайн-очередь — в движке.
 * - packetId не пересекается с ожидающими SUBSCRIBE/UNSUBSCRIBE (старый клиент их не учитывал).
 * - Досылки неподтверждённого при `cleanStart = false` и `sessionPresent` нет: ядро живёт одно подключение.
 */
@MqttCoreApi
public class MqttSessionCore(private val settings: SessionSettings, nowMs: Long) {

    /** Текущее состояние подключения. */
    public var state: CoreState = CoreState.Connecting
        private set

    private val mqtt5 = settings.version == MQTTVersion.MQTT5
    private val framer = MqttFramer(settings.version, settings.maxIncomingPacket, settings.maxTopicLength)
    private val outbound = ArrayList<ByteArray>()
    private var started = false

    // Время
    private var connackDeadline = 0L
    private var lingerDeadline = 0L
    private var keepAliveMs = settings.keepAliveSec * 1000L
    private var lastSentMs = nowMs
    private var lastReceivedMs = nowMs
    private var pingSentMs: Long? = null
    private var readPaused = false

    // Свойства сервера (CONNACK)
    private var serverReceiveMaximum = 65_535
    private var serverMaximumQos = Qos.EXACTLY_ONCE
    private var serverRetainAvailable = true
    private var serverMaximumPacketSize = Int.MAX_VALUE

    // Сессия этого подключения
    private var lastPacketId = 0
    private val pendingAck = HashMap<Int, Qos>() // исходящие QoS ≥ 1, ждут PUBACK/PUBREC
    private val pendingPubcomp = HashSet<Int>() // исходящие QoS 2, ждут PUBCOMP
    private val pendingSubscriptions = HashSet<Int>() // SUBSCRIBE/UNSUBSCRIBE, ждут SUBACK/UNSUBACK
    private val qos2Received = HashSet<Int>() // входящие QoS 2, ждут PUBREL
    private val topicAliases = HashMap<Int, String>()
    private var aliasesLost = false

    /** Отправляет CONNECT. Вызывается один раз, когда транспорт подключён. */
    public fun start(nowMs: Long): List<CoreOutput> {
        check(!started) { "start() already called" }
        started = true
        if (state != CoreState.Connecting) return emptyList()
        connackDeadline = nowMs + settings.connackTimeoutMs
        lastReceivedMs = nowMs
        enqueue(buildConnect(), nowMs)
        return emptyList()
    }

    /** Байты, прочитанные из транспорта. В `Closing` читаются и отбрасываются, после `Closed` игнорируются. */
    public fun onBytes(bytes: ByteArray, off: Int, len: Int, nowMs: Long): List<CoreOutput> {
        when (state) {
            CoreState.Closed, CoreState.Closing -> return emptyList()
            CoreState.Connecting -> check(started) { "onBytes() before start()" }
            CoreState.Connected -> Unit
        }
        if (len == 0) return emptyList()
        // Любой байт от брокера — признак живого соединения (PINGRESP в том числе)
        lastReceivedMs = nowMs
        pingSentMs = null
        val out = ArrayList<CoreOutput>(4)
        try {
            for (frame in framer.feed(bytes, off, len)) {
                if (state != CoreState.Connecting && state != CoreState.Connected) break
                when (frame) {
                    is FramerOutput.Packet -> handlePacket(frame.packet, nowMs, out)
                    is FramerOutput.Publish -> handlePublish(frame.publish, nowMs, out)
                    is FramerOutput.Dropped -> handleDropped(frame, nowMs, out)
                }
            }
        } catch (e: MQTTException) {
            if (state == CoreState.Connecting || state == CoreState.Connected) failProtocol(e.reasonCode, nowMs, out)
        }
        return out
    }

    /**
     * Транспорт закрыт брокером (EOF). В `Closing` — штатный конец (`Closed(null)`), иначе — `Closed(Io("eof"))`.
     */
    public fun onEof(nowMs: Long): List<CoreOutput> = when (state) {
        CoreState.Closed -> emptyList()
        CoreState.Closing -> closeWith(null)
        CoreState.Connecting, CoreState.Connected -> closeWith(MqttError.Io("eof", null))
    }

    /**
     * Публикация. Только в `Connected`; при `inflight ≥ Receive Maximum` брокера — [PublishAdmission.FlowControlled].
     */
    public fun publish(p: OutgoingPublish, nowMs: Long): PublishAdmission {
        if (state != CoreState.Connected) return PublishAdmission.Rejected(PublishRejection.NotConnected)
        if (!isValidTopic(p.topic)) return PublishAdmission.Rejected(PublishRejection.InvalidTopic)
        if (p.qos.value > serverMaximumQos.value) return PublishAdmission.Rejected(PublishRejection.QosNotSupported)
        if (p.retain && !serverRetainAvailable) return PublishAdmission.Rejected(PublishRejection.RetainNotSupported)
        if (p.qos != Qos.AT_MOST_ONCE && pendingAck.size + pendingPubcomp.size >= serverReceiveMaximum) {
            return PublishAdmission.FlowControlled
        }
        val packetId = if (p.qos != Qos.AT_MOST_ONCE) nextPacketId() else null
        val id = packetId?.toUInt()
        val payload = p.payload.asUByteArray()
        // timestamp передаётся явно: значение по умолчанию у kmqtt-common читает системные часы
        val packet = if (mqtt5) {
            MQTT5Publish(p.retain, p.qos, false, p.topic, id, MQTT5Properties(), payload, nowMs)
        } else {
            MQTT4Publish(p.retain, p.qos, false, p.topic, id, payload, nowMs)
        }
        val data = packet.toByteArray().asByteArray()
        if (data.size > serverMaximumPacketSize) {
            return PublishAdmission.Rejected(PublishRejection.TooLarge(data.size, serverMaximumPacketSize))
        }
        if (packetId != null) pendingAck[packetId] = p.qos
        enqueue(data, nowMs)
        return PublishAdmission.Accepted(packetId)
    }

    /**
     * SUBSCRIBE; возвращает packetId (ответ — [CoreOutput.SubAck]). Только в `Connected`, иначе
     * `IllegalStateException` (движок подписывается после [CoreOutput.Connected]). Разбивка на порции — в движке.
     */
    public fun subscribe(filters: List<SubscriptionRequest>, nowMs: Long): Int {
        check(state == CoreState.Connected) { "subscribe() in state $state" }
        require(filters.isNotEmpty()) { "no filters" }
        val packetId = nextPacketId()
        val subscriptions = filters.map { Subscription(it.filter, SubscriptionOptions(it.qos)) }
        val packet = if (mqtt5) {
            MQTT5Subscribe(packetId.toUInt(), subscriptions)
        } else {
            MQTT4Subscribe(packetId.toUInt(), subscriptions)
        }
        val data = packet.toByteArray().asByteArray()
        pendingSubscriptions += packetId
        enqueue(data, nowMs)
        return packetId
    }

    /** UNSUBSCRIBE; возвращает packetId (ответ — [CoreOutput.UnsubAck]). Только в `Connected`. */
    public fun unsubscribe(filters: List<String>, nowMs: Long): Int {
        check(state == CoreState.Connected) { "unsubscribe() in state $state" }
        require(filters.isNotEmpty()) { "no filters" }
        val packetId = nextPacketId()
        val packet = if (mqtt5) {
            MQTT5Unsubscribe(packetId.toUInt(), filters)
        } else {
            MQTT4Unsubscribe(packetId.toUInt(), filters)
        }
        val data = packet.toByteArray().asByteArray()
        pendingSubscriptions += packetId
        enqueue(data, nowMs)
        return packetId
    }

    /**
     * Штатное закрытие (H1): DISCONNECT ([reason] — только MQTT 5) в исходящие и состояние `Closing`: входящее
     * читается и отбрасывается до EOF ([onEof] → `Closed(null)`) или дедлайна `closeLingerMs` ([onTick] →
     * `Closed(null)`). При `closeLingerMs = 0` — `Closed(null)` сразу (DISCONNECT остаётся в [takeOutbound]).
     * До [start] — `Closed(null)` без DISCONNECT. В `Closing`/`Closed` — ничего.
     */
    public fun disconnect(reason: ReasonCode, nowMs: Long): List<CoreOutput> {
        when (state) {
            CoreState.Closing, CoreState.Closed -> return emptyList()
            CoreState.Connecting -> if (!started) return closeWith(null)
            CoreState.Connected -> Unit
        }
        val packet = if (mqtt5) MQTT5Disconnect(reason) else MQTT4Disconnect()
        enqueue(packet.toByteArray().asByteArray(), nowMs)
        state = CoreState.Closing
        lingerDeadline = nowMs + settings.closeLingerMs
        return if (settings.closeLingerMs == 0L) closeWith(null) else emptyList()
    }

    /** Таймауты: CONNACK, keepAlive (PINGREQ и ожидание ответа), linger закрытия. */
    public fun onTick(nowMs: Long): List<CoreOutput> {
        when (state) {
            CoreState.Closed -> return emptyList()
            CoreState.Connecting -> {
                if (started && nowMs >= connackDeadline) return closeWith(MqttError.ConnackTimeout)
                return emptyList()
            }
            CoreState.Closing -> return if (nowMs >= lingerDeadline) closeWith(null) else emptyList()
            CoreState.Connected -> Unit
        }
        if (keepAliveMs == 0L) return emptyList()
        val responseDeadline = pingResponseDeadline()
        if (responseDeadline != null && nowMs >= responseDeadline) return closeWith(MqttError.KeepAliveTimeout)
        if (nowMs >= nextPingAt()) {
            enqueue((if (mqtt5) PINGREQ_5 else PINGREQ_4).copyOf(), nowMs)
            pingSentMs = nowMs
        }
        return emptyList()
    }

    /** Ближайший момент, когда нужен [onTick]: CONNACK, keepAlive или linger; null — не нужен. */
    public fun nextDeadline(): Long? = when (state) {
        CoreState.Closed -> null
        CoreState.Connecting -> if (started) connackDeadline else null
        CoreState.Closing -> lingerDeadline
        CoreState.Connected -> if (keepAliveMs == 0L) {
            null
        } else {
            val ping = nextPingAt()
            val response = pingResponseDeadline()
            if (response != null && response < ping) response else ping
        }
    }

    /** Всё, что нужно записать в транспорт (пакеты подряд), или null. */
    public fun takeOutbound(): ByteArray? {
        if (outbound.isEmpty()) return null
        val result = if (outbound.size == 1) {
            outbound[0]
        } else {
            val total = outbound.sumOf { it.size }
            val joined = ByteArray(total)
            var pos = 0
            for (chunk in outbound) {
                chunk.copyInto(joined, pos)
                pos += chunk.size
            }
            joined
        }
        outbound.clear()
        return result
    }

    /**
     * Обратное давление (W6 §4.5): хост перестал (или снова начал) читать транспорт. На паузе PINGREQ уходит по
     * расписанию, таймаут ответа не считается; при снятии паузы отсчёт ответа и молчания брокера — заново от [nowMs].
     */
    public fun setReadPaused(paused: Boolean, nowMs: Long) {
        if (paused == readPaused) return
        readPaused = paused
        if (!paused) {
            lastReceivedMs = nowMs
            if (pingSentMs != null) pingSentMs = nowMs
        }
    }

    // ---- keepAlive ----

    private fun nextPingAt(): Long {
        val bySent = lastSentMs + keepAliveMs * 9 / 10
        return when {
            readPaused -> bySent
            pingSentMs != null -> Long.MAX_VALUE // ждём ответа на отправленный PINGREQ
            else -> minOf(bySent, lastReceivedMs + keepAliveMs)
        }
    }

    private fun pingResponseDeadline(): Long? {
        val ping = pingSentMs ?: return null
        return if (readPaused) null else ping + keepAliveMs / 2
    }

    // ---- входящие ----

    private fun handlePacket(packet: MQTTPacket, nowMs: Long, out: MutableList<CoreOutput>) {
        if (packet is MQTTConnack) {
            handleConnack(packet, out)
            return
        }
        if (state != CoreState.Connected) throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        when (packet) {
            is MQTTPuback -> {
                val id = packet.packetId.toInt()
                if (pendingAck.remove(id) != null) {
                    out += CoreOutput.PubAck(id, (packet as? MQTT5Puback)?.reasonCode ?: ReasonCode.SUCCESS)
                }
            }
            is MQTTPubrec -> {
                val id = packet.packetId.toInt()
                val code = (packet as? MQTT5Pubrec)?.reasonCode ?: ReasonCode.SUCCESS
                val wasPending = pendingAck.remove(id) != null
                if (code.value >= 0x80) {
                    // Брокер отказал в QoS 2: поток завершён, PUBREL не отправляется
                    if (wasPending) out += CoreOutput.PubAck(id, code)
                } else {
                    pendingPubcomp += id
                    val pubrel = if (mqtt5) MQTT5Pubrel(packet.packetId) else MQTT4Pubrel(packet.packetId)
                    enqueue(pubrel.toByteArray().asByteArray(), nowMs)
                }
            }
            is MQTTPubrel -> {
                val pubcomp = if (mqtt5) MQTT5Pubcomp(packet.packetId) else MQTT4Pubcomp(packet.packetId)
                enqueue(pubcomp.toByteArray().asByteArray(), nowMs)
                if (!qos2Received.remove(packet.packetId.toInt())) {
                    throw MQTTException(ReasonCode.PACKET_IDENTIFIER_NOT_FOUND)
                }
            }
            is MQTTPubcomp -> {
                val id = packet.packetId.toInt()
                if (!pendingPubcomp.remove(id)) throw MQTTException(ReasonCode.PACKET_IDENTIFIER_NOT_FOUND)
                out += CoreOutput.PubAck(id, (packet as? MQTT5Pubcomp)?.reasonCode ?: ReasonCode.SUCCESS)
            }
            is MQTTSuback -> {
                val id = packet.packetIdentifier.toInt()
                pendingSubscriptions -= id
                val codes = when (packet) {
                    is MQTT5Suback -> packet.reasonCodes
                    is MQTT4Suback -> packet.reasonCodes.map { it.toReason() }
                    else -> emptyList()
                }
                out += CoreOutput.SubAck(id, codes)
            }
            is MQTTUnsuback -> {
                val id = packet.packetIdentifier.toInt()
                pendingSubscriptions -= id
                out += CoreOutput.UnsubAck(id, (packet as? MQTT5Unsuback)?.reasonCodes ?: emptyList())
            }
            is MQTTPingresp -> Unit // отметка «брокер жив» уже сделана в onBytes
            is MQTTDisconnect -> {
                val code = (packet as? MQTT5Disconnect)?.reasonCode ?: ReasonCode.SUCCESS
                out += closeWith(MqttError.ServerDisconnect(code))
            }
            is MQTT5Auth -> throw MQTTException(ReasonCode.PROTOCOL_ERROR)
            else -> throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        }
    }

    private fun handleConnack(packet: MQTTConnack, out: MutableList<CoreOutput>) {
        if (state != CoreState.Connecting) throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        val info: ConnackInfo
        when (packet) {
            is MQTT5Connack -> {
                if (packet.connectReasonCode != ReasonCode.SUCCESS) {
                    // Любой отказ, в т. ч. SERVER_MOVED/USE_ANOTHER_SERVER: без перехода на serverReference (H10)
                    out += closeWith(MqttError.Refused(packet.connectReasonCode))
                    return
                }
                val props = packet.properties
                val receiveMaximum = props.receiveMaximum?.toLong() ?: 65_535L
                if (receiveMaximum == 0L) throw MQTTException(ReasonCode.PROTOCOL_ERROR)
                val maximumQos = props.maximumQos?.let {
                    Qos.valueOf(it.toInt()) ?: throw MQTTException(ReasonCode.PROTOCOL_ERROR)
                } ?: Qos.EXACTLY_ONCE
                info = ConnackInfo(
                    sessionPresent = packet.connectAcknowledgeFlags.sessionPresentFlag,
                    serverKeepAliveSec = props.serverKeepAlive?.toInt(),
                    receiveMaximum = receiveMaximum.coerceAtMost(65_535L).toInt(),
                    maximumQos = maximumQos,
                    retainAvailable = props.retainAvailable?.let { it != 0u } ?: true,
                    maximumPacketSize = props.maximumPacketSize?.let { it.toLong().coerceAtMost(Int.MAX_VALUE.toLong()).toInt() },
                    assignedClientId = props.assignedClientIdentifier,
                    topicAliasMaximum = props.topicAliasMaximum?.toInt() ?: 0,
                )
            }
            is MQTT4Connack -> {
                if (packet.connectReturnCode != ConnectReturnCode.CONNECTION_ACCEPTED) {
                    out += closeWith(MqttError.Refused(packet.connectReturnCode.toReasonCode()))
                    return
                }
                info = ConnackInfo(
                    sessionPresent = packet.connectAcknowledgeFlags.sessionPresentFlag,
                    serverKeepAliveSec = null,
                    receiveMaximum = 65_535,
                    maximumQos = Qos.EXACTLY_ONCE,
                    retainAvailable = true,
                    maximumPacketSize = null,
                    assignedClientId = null,
                    topicAliasMaximum = 0,
                )
            }
            else -> throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        }
        if (settings.cleanStart && info.sessionPresent) throw MQTTException(ReasonCode.PROTOCOL_ERROR)

        serverReceiveMaximum = info.receiveMaximum
        serverMaximumQos = info.maximumQos
        serverRetainAvailable = info.retainAvailable
        serverMaximumPacketSize = info.maximumPacketSize ?: Int.MAX_VALUE
        info.serverKeepAliveSec?.let { keepAliveMs = it * 1000L }
        state = CoreState.Connected
        out += CoreOutput.Connected(info)
    }

    private fun handlePublish(publish: IncomingPublish, nowMs: Long, out: MutableList<CoreOutput>) {
        if (state != CoreState.Connected) throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        var topic = publish.topic
        val alias = publish.topicAlias
        if (alias != null) {
            if (alias > settings.topicAliasMaximum) throw MQTTException(ReasonCode.TOPIC_ALIAS_INVALID)
            if (topic.isNotEmpty()) {
                topicAliases[alias] = topic
            } else {
                val known = topicAliases[alias]
                if (known == null) {
                    if (!aliasesLost) throw MQTTException(ReasonCode.PROTOCOL_ERROR)
                    // Алиас мог быть задан PUBLISH, отброшенным как TopicTooLong: топик неизвестен
                    acknowledgeDropped(publish.packetId, publish.qos, nowMs)
                    out += CoreOutput.Dropped(DropReason.TopicTooLong(-1, settings.maxTopicLength))
                    return
                }
                topic = known
            }
        }
        val packetId = publish.packetId
        when (publish.qos) {
            Qos.AT_MOST_ONCE -> out += CoreOutput.Message(topic, publish.payload, publish.qos, publish.retain)
            Qos.AT_LEAST_ONCE -> {
                val id = packetId ?: throw MQTTException(ReasonCode.MALFORMED_PACKET)
                out += CoreOutput.Message(topic, publish.payload, publish.qos, publish.retain)
                enqueue(puback(id), nowMs)
            }
            Qos.EXACTLY_ONCE -> {
                val id = packetId ?: throw MQTTException(ReasonCode.MALFORMED_PACKET)
                val fresh = id !in qos2Received
                if (fresh && qos2Received.size >= settings.receiveMaximum) {
                    throw MQTTException(ReasonCode.RECEIVE_MAXIMUM_EXCEEDED)
                }
                enqueue(pubrec(id), nowMs)
                if (fresh) {
                    qos2Received += id
                    out += CoreOutput.Message(topic, publish.payload, publish.qos, publish.retain)
                }
            }
        }
    }

    private fun handleDropped(dropped: FramerOutput.Dropped, nowMs: Long, out: MutableList<CoreOutput>) {
        if (state != CoreState.Connected) throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        if (dropped.reason is DropReason.TopicTooLong && mqtt5 && settings.topicAliasMaximum > 0) {
            // Отброшенный PUBLISH мог задать или перепривязать алиас: старые привязки больше не достоверны
            topicAliases.clear()
            aliasesLost = true
        }
        acknowledgeDropped(dropped.packetId, dropped.qos, nowMs)
        out += CoreOutput.Dropped(dropped.reason)
    }

    /** Подтверждает отброшенный PUBLISH, чтобы у брокера не закрывалось окно неподтверждённых. */
    private fun acknowledgeDropped(packetId: Int?, qos: Qos?, nowMs: Long) {
        if (packetId == null) return
        when (qos) {
            Qos.AT_LEAST_ONCE -> enqueue(puback(packetId), nowMs)
            Qos.EXACTLY_ONCE -> {
                qos2Received += packetId
                enqueue(pubrec(packetId), nowMs)
            }
            else -> Unit
        }
    }

    // ---- служебное ----

    private fun buildConnect(): ByteArray {
        val will = settings.will
        val credentials = settings.credentials
        val login = credentials?.login
        val password = credentials?.password?.asUByteArray()
        val flags = ConnectFlags(
            login != null,
            password != null,
            will?.retain ?: false,
            will?.qos ?: Qos.AT_MOST_ONCE,
            will != null,
            settings.cleanStart,
            false,
        )
        val clientId = settings.clientId ?: generateRandomClientId()
        val packet = if (mqtt5) {
            val properties = MQTT5Properties(
                maximumPacketSize = settings.maxIncomingPacket.toUInt(),
                receiveMaximum = settings.receiveMaximum.takeIf { it != 65_535 }?.toUInt(),
                topicAliasMaximum = settings.topicAliasMaximum.takeIf { it > 0 }?.toUInt(),
            )
            MQTT5Connect(
                "MQTT",
                flags,
                settings.keepAliveSec,
                clientId,
                properties,
                if (will != null) MQTT5Properties() else null,
                will?.topic,
                will?.payload?.asUByteArray(),
                login,
                password,
            )
        } else {
            MQTT4Connect(
                "MQTT",
                flags,
                settings.keepAliveSec,
                clientId,
                will?.topic,
                will?.payload?.asUByteArray(),
                login,
                password,
            )
        }
        return packet.toByteArray().asByteArray()
    }

    private fun puback(id: Int): ByteArray =
        (if (mqtt5) MQTT5Puback(id.toUInt()) else MQTT4Puback(id.toUInt())).toByteArray().asByteArray()

    private fun pubrec(id: Int): ByteArray =
        (if (mqtt5) MQTT5Pubrec(id.toUInt()) else MQTT4Pubrec(id.toUInt())).toByteArray().asByteArray()

    private fun enqueue(data: ByteArray, nowMs: Long) {
        outbound += data
        lastSentMs = nowMs
    }

    private fun nextPacketId(): Int {
        repeat(MAX_PACKET_ID) {
            lastPacketId = lastPacketId % MAX_PACKET_ID + 1
            val id = lastPacketId
            if (id !in pendingAck && id !in pendingPubcomp && id !in pendingSubscriptions) return id
        }
        throw IllegalStateException("no free packet identifier")
    }

    private fun failProtocol(code: ReasonCode, nowMs: Long, out: MutableList<CoreOutput>) {
        val closed = closeWith(MqttError.Protocol(code))
        if (mqtt5) {
            // DISCONNECT с кодом ≠ 0: брокер публикует will, как при обрыве. В 3.1.1 DISCONNECT отменил бы will.
            enqueue(MQTT5Disconnect(code).toByteArray().asByteArray(), nowMs)
        }
        out += closed
    }

    /**
     * Переход в `Closed` (один раз). Штатное закрытие сохраняет исходящие (DISCONNECT ещё может быть не записан),
     * при ошибке они отбрасываются: соединение закрывается сразу.
     */
    private fun closeWith(error: MqttError?): List<CoreOutput> {
        if (state == CoreState.Closed) return emptyList()
        state = CoreState.Closed
        if (error != null) outbound.clear()
        return listOf(CoreOutput.Closed(error))
    }

    private fun isValidTopic(topic: String): Boolean {
        if (topic.isEmpty()) return false
        if (topic.indexOf('+') >= 0 || topic.indexOf('#') >= 0 || topic.indexOf('\u0000') >= 0) return false
        return topic.length <= MAX_TOPIC_CHARS_FAST || topic.encodeToByteArray().size <= MAX_STRING_BYTES
    }

    private fun SubackReturnCode.toReason(): ReasonCode = when (this) {
        SubackReturnCode.MAXIMUM_QOS0 -> ReasonCode.SUCCESS
        SubackReturnCode.MAXIMUM_QOS1 -> ReasonCode.GRANTED_QOS1
        SubackReturnCode.MAXIMUM_QOS2 -> ReasonCode.GRANTED_QOS2
        SubackReturnCode.FAILURE -> ReasonCode.UNSPECIFIED_ERROR
    }

    private companion object {
        const val MAX_PACKET_ID = 65_535
        const val MAX_STRING_BYTES = 65_535

        /** Строка не длиннее этого числа символов заведомо ≤ 65535 байт UTF-8 (≤ 3 байта на символ UTF-16). */
        const val MAX_TOPIC_CHARS_FAST = MAX_STRING_BYTES / 3

        val PINGREQ_4: ByteArray = MQTT4Pingreq().toByteArray().asByteArray()
        val PINGREQ_5: ByteArray = MQTT5Pingreq().toByteArray().asByteArray()
    }
}
