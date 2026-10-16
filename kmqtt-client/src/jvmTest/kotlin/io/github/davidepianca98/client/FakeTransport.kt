package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.BrokerScript
import io.github.davidepianca98.mqtt.MQTTCurrentPacket
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.MQTTPacket
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTConnect
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTDisconnect
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPingreq
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPublish
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTSubscribe
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTUnsubscribe
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode

/**
 * Брокер-сценарий для [FakeTransport] (W6 §7.1, S06b-T05b): разбирает исходящее клиента пакетами (`MQTTCurrentPacket`,
 * как брокер) и отвечает по настройкам — CONNACK с кодом или тишина, SUBACK с отказами, PUBACK с кодом, EOF после
 * DISCONNECT; тест может в любой момент прислать байты ([send]), EOF ([eof]) или обрыв ([fail]).
 *
 * Однопоточный: всё — на потоке `runTest` (хост [VirtualHost] и тест делят его).
 */
internal class FakeBroker(val version: MQTTVersion = MQTTVersion.MQTT5) {
    /** Как отвечает транспорт на `connect`. */
    sealed interface ConnectBehavior {
        /** Сразу `TransportEvent.Connected`. */
        object Accept : ConnectBehavior

        /** Тишина: ни `Connected`, ни ошибки, пока не истечёт срок подключения (`ConnectTimeout`). */
        object Silent : ConnectBehavior

        /** `TransportEvent.Failed(error)`. */
        class Fail(val error: MqttError) : ConnectBehavior
    }

    val script = BrokerScript(version)

    var connect: ConnectBehavior = ConnectBehavior.Accept

    /** Код CONNACK; `null` — CONNACK не присылать (тишина после CONNECT). */
    var connackCode: ReasonCode? = ReasonCode.SUCCESS

    /** Код SUBACK по фильтру; `null` — выдать запрошенный QoS. */
    var subackCode: (String) -> ReasonCode? = { null }

    /** Отвечать SUBACK автоматически. */
    var autoSuback = true

    /** Код PUBACK на PUBLISH QoS 1; `null` — PUBACK не присылать. */
    var pubackCode: ReasonCode? = ReasonCode.SUCCESS

    /** Закрыть соединение (EOF) в ответ на DISCONNECT клиента. */
    var eofOnDisconnect = true

    /** Делить входящее клиенту на куски по [chunk] байт (частичные чтения); `null` — целиком. */
    var chunk: Int? = null

    /** Пакеты клиента по порядку (все подключения). */
    val received = ArrayList<MQTTPacket>()

    /** Журнал транспорта по порядку: `connect`, имена пакетов клиента (`CONNECT`, `PUBLISH`…), `close`. */
    val log = ArrayList<String>()

    /** Все транспорты (по одному на `HostAction.Connect`). */
    val transports = ArrayList<FakeTransport>()

    val current: FakeTransport get() = transports.last()

    inline fun <reified T : MQTTPacket> packets(): List<T> = received.filterIsInstance<T>()

    /** Прислать клиенту байты по текущему транспорту. */
    fun send(bytes: ByteArray) = current.inject(bytes)

    /** Брокер закрыл соединение. */
    fun eof() = current.injectEof()

    /** Обрыв транспорта. */
    fun fail(error: MqttError = MqttError.Io("read", null)) = current.injectFailure(error)

    internal fun onPacket(transport: FakeTransport, packet: MQTTPacket) {
        received += packet
        log += packet.name()
        when (packet) {
            is MQTTConnect -> connackCode?.let { transport.inject(connack(it)) }
            is MQTTSubscribe -> if (autoSuback) {
                val codes = packet.subscriptions.map { subackCode(it.topicFilter) ?: granted(it.options.qos) }
                transport.inject(script.suback(packet.packetIdentifier.toInt(), codes))
            }
            is MQTTUnsubscribe -> transport.inject(script.unsuback(packet.packetIdentifier.toInt(), packet.topicFilters.map { ReasonCode.SUCCESS }))
            is MQTTPublish -> if (packet.qos == Qos.AT_LEAST_ONCE) pubackCode?.let { transport.inject(script.puback(packet.packetId!!.toInt(), it)) }
            is MQTTPingreq -> transport.inject(script.pingresp())
            is MQTTDisconnect -> if (eofOnDisconnect) transport.injectEof()
            else -> Unit
        }
    }

    private fun connack(code: ReasonCode): ByteArray =
        if (version == MQTTVersion.MQTT5 || code == ReasonCode.SUCCESS) script.connack(code) else error("MQTT 3.1.1: use send(script.connack4)")

    private fun granted(qos: Qos): ReasonCode = when (qos) {
        Qos.AT_MOST_ONCE -> ReasonCode.SUCCESS
        Qos.AT_LEAST_ONCE -> ReasonCode.GRANTED_QOS1
        Qos.EXACTLY_ONCE -> ReasonCode.GRANTED_QOS2
    }

    private fun MQTTPacket.name(): String = this::class.simpleName!!.removePrefix("MQTT5").removePrefix("MQTT4").uppercase()
}

/**
 * Транспорт [VirtualHost] без сокета (W6 §7.1, S06b-T05b): запись разбирает [FakeBroker], ответы брокера ждут во
 * входящей очереди и выдаются событиями `Bytes` (кусками [FakeBroker.chunk]). Пауза чтения ([setReadInterest]) — как у
 * сокета: входящие байты и EOF **придерживаются**, пока чтение не включат; обрыв ([injectFailure]) — сразу.
 */
@OptIn(io.github.davidepianca98.client.core.MqttCoreApi::class)
internal class FakeTransport(private val broker: FakeBroker, private val wakeup: () -> Unit) : MqttTransport {
    private val parser = MQTTCurrentPacket(16u * 1024u * 1024u, broker.version)
    private val inbound = ArrayDeque<TransportEvent>()
    private val ready = ArrayList<TransportEvent>()
    private var connectDeadline: Long? = null

    var connected = false
        private set
    var closed = false
        private set
    var readEnabled = true
        private set

    /** Байтов, придержанных паузой чтения. */
    val withheldBytes: Int get() = inbound.sumOf { (it as? TransportEvent.Bytes)?.bytes?.size ?: 0 }

    override fun connect(host: String, port: Int, deadlineMs: Long) {
        broker.log += "connect"
        when (val behavior = broker.connect) {
            FakeBroker.ConnectBehavior.Accept -> {
                connected = true
                ready += TransportEvent.Connected
            }
            FakeBroker.ConnectBehavior.Silent -> connectDeadline = deadlineMs
            is FakeBroker.ConnectBehavior.Fail -> fail(behavior.error)
        }
    }

    override fun write(bytes: ByteArray): Int {
        if (closed) return 0
        for (packet in parser.addData(bytes.asUByteArray())) broker.onPacket(this, packet)
        return bytes.size
    }

    override val pendingWriteBytes: Long get() = 0

    override fun setReadInterest(enabled: Boolean) {
        readEnabled = enabled
    }

    override fun poll(nowMs: Long) {
        val deadline = connectDeadline ?: return
        if (nowMs >= deadline) fail(MqttError.ConnectTimeout)
    }

    override fun nextDeadlineMs(): Long? = connectDeadline

    override fun events(): List<TransportEvent> {
        while (readEnabled && inbound.isNotEmpty()) ready += inbound.removeFirst()
        if (ready.isEmpty()) return emptyList()
        return ArrayList(ready).also { ready.clear() }
    }

    override fun close() {
        if (closed) return
        closed = true
        connectDeadline = null
        broker.log += "close"
        inbound.clear()
        ready.clear()
    }

    fun inject(bytes: ByteArray) {
        if (closed) return
        val size = broker.chunk ?: bytes.size
        var offset = 0
        while (offset < bytes.size) {
            val end = minOf(bytes.size, offset + size)
            inbound += TransportEvent.Bytes(bytes.copyOfRange(offset, end))
            offset = end
        }
        wakeup()
    }

    fun injectEof() {
        if (closed) return
        inbound += TransportEvent.Eof
        wakeup()
    }

    fun injectFailure(error: MqttError) {
        if (closed) return
        fail(error)
        wakeup()
    }

    private fun fail(error: MqttError) {
        // Как у NioTransport: пути ошибок закрывают канал сразу, событие — последнее
        connectDeadline = null
        inbound.clear()
        ready += TransportEvent.Failed(error)
        closed = true
        broker.log += "failed"
    }
}
