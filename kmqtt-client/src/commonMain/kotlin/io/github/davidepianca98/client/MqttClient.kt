package io.github.davidepianca98.client

import io.github.davidepianca98.client.internal.MqttClientImpl
import io.github.davidepianca98.mqtt.packets.Qos
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow
import kotlin.time.Duration

/**
 * Корутинный MQTT-клиент (W6 §4.1, S06b-T05a): одно соединение с брокером с переподключением, декларативными
 * подписками ([subscriptions]), оффлайн-очередью публикаций с TTL и входящими с обратным давлением ([incoming]).
 *
 * Все методы потокобезопасны и не блокируют: команда кладётся в очередь и будит поток хоста (H4); протокол, таймеры
 * и сокет живут на потоке хоста ([MqttHost], на JVM — `kmqtt-io-<n>`). Продолжения `suspend`-методов и `Flow`
 * исполняются на диспетчере вызывающего.
 *
 * Жизненный цикл: создан ([ConnectionState.Idle]) → [start]/[connect] → … → [close] ([ConnectionState.Closed]).
 * Поток хоста запускается первой командой (любой метод), а не только [start]: так TTL публикаций, поставленных до
 * [start], истекает вовремя. Клиент одноразовый: после [close] не перезапускается. Если клиент закрылся сам
 * (исчерпан [ReconnectPolicy.maxAttempts]), поток хоста продолжает жить до [close] — вызывать [close] обязательно.
 */
public interface MqttClient {
    public val config: MqttClientOptions

    /**
     * Состояние подключения — для UI, журнала и синхронной пометки «сессия кончилась» (W6 §4.5). Сливается
     * (`StateFlow`): упорядоченным потребителям — только [incoming].
     */
    public val connection: StateFlow<ConnectionState>

    /**
     * Входящие строго по порядку получения вместе с границами сессий ([MqttIncoming.SessionStarted] →
     * [MqttIncoming.Message]… → [MqttIncoming.SessionEnded]). Один потребитель: повторный `collect` —
     * `IllegalStateException`. Буфер — [MqttLimits.incomingBuffer]; при заполнении клиент перестаёт читать сокет
     * (давление уходит в TCP и на брокер), ничего не отбрасывая; пауза дольше [MqttLimits.consumerStallLimit] рвёт
     * подключение с [MqttError.ConsumerStalled]. PUBACK входящего QoS 1 уходит при постановке в буфер. Поток
     * завершается после [close] (уже лежащие в буфере сообщения дочитываются).
     */
    public val incoming: Flow<MqttIncoming>

    public val subscriptions: MqttSubscriptions

    /** Запуск: подключение и переподключение по [MqttClientOptions.reconnect]. Идемпотентен; после [close] — ничего. */
    public fun start()

    /**
     * [start] + ожидание первого CONNACK (уже подключён — сразу). С [ReconnectPolicy.Never] — результат первой попытки;
     * с переподключением ждёт, пока не подключится или не закроется (ограничивайте `withTimeout`). После [close] —
     * `Failed`.
     */
    public suspend fun connect(): ConnectResult

    /**
     * Публикация. Ждёт до стадии [until]: [PublishStage.Sent] — PUBLISH отдан на запись в сокет живой сессии;
     * [PublishStage.Delivered] — PUBACK (QoS 1; у QoS 0 PUBACK нет — исход `Sent`). Без соединения ждёт в оффлайн-очереди
     * не дольше [ttl], затем `Expired` (брокеру не уходит никогда). Отмена вызывающей корутины до записи снимает
     * публикацию с очереди; после записи — только перестаёт ждать исхода.
     *
     * Неподтверждённая публикация после обрыва не досылается (`cleanStart`): `Failed(ConnectionLost)`. Публикации,
     * удерживаемые окном Receive Maximum брокера, при [close] получают `Failed(Closed)` (предел H2).
     */
    public suspend fun publish(
        topic: String,
        payload: ByteArray,
        qos: Qos = Qos.AT_LEAST_ONCE,
        retain: Boolean = false,
        ttl: Duration = config.offlineQueue.ttl,
        until: PublishStage = PublishStage.Sent,
    ): PublishResult

    /**
     * Штатное закрытие (H1/H2, K8): сначала уходят все команды, уже стоящие в очереди (публикации остановки —
     * стирание retained, флаг `r`), затем DISCONNECT; после него клиент читает и отбрасывает входящее, пока брокер не
     * закроет соединение или не истечёт [MqttClientOptions.closeLinger], и только потом закрывает сокет и
     * останавливает поток хоста. Ожидающие публикации, не успевшие уйти → `Failed(Closed)`. Will при штатном закрытии
     * брокер не публикует.
     *
     * Закрытие во время подключения (до CONNACK) закрывает транспорт **без** DISCONNECT (как `MQTTConnection.stop`):
     * если брокер уже принял CONNECT, он вправе опубликовать will.
     *
     * Повторный вызов ждёт того же завершения. Не виснет: общий срок — `closeLinger` + запас, затем поток хоста
     * останавливается принудительно.
     */
    public suspend fun close()
}

/**
 * Клиент на заданном хосте цикла [host] (common: js/native компилируются без JVM-API). [clock] должен совпадать с
 * часами хоста. Хост одноразовый и принадлежит клиенту: [MqttClient.close] закрывает и его. TLS-параметры
 * [MqttClientOptions.tls]/[MqttClientOptions.verifyHostname] применяет тот, кто создаёт хост (на JVM — перегрузка
 * `MqttClient(options)` с `NioActorHost`). **Безопасность:** при `tls != null` эта фабрика отказывает
 * (`IllegalArgumentException` при создании, хост не запускается): хост, переданный снаружи, TLS не применяет
 * (публичный `NioActorHost()` — только TCP), а открытым текстом клиент не подключается никогда (S06b-T05b). Для TLS на
 * JVM — `MqttClient(options)`.
 *
 * Проверка параметров — здесь же (`IllegalArgumentException`, текст без адресов и секретов).
 */
public fun MqttClient(options: MqttClientOptions, host: MqttHost, clock: MqttClock = MqttClock.Monotonic): MqttClient =
    MqttClientImpl(options, host, clock)
