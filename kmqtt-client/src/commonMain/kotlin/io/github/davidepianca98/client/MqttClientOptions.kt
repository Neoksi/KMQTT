package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import io.github.davidepianca98.socket.tls.TLSClientSettings
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

// Конфигурация нового клиента (W6 §4.2, S06b-T05a). Безопасность (H11): ни один `toString()` этого файла не печатает
// адрес брокера, логин, пароль, TLS-ключи и пароли, топик и нагрузку will.

/**
 * Параметры клиента [MqttClient].
 *
 * Сравнивать экземпляры для решения «пересоздать клиент» не следует: [ReconnectPolicy.random] и [logger] — лямбды и
 * объекты, их равенство — по ссылке (W6 §4.9 «Сравнение конфигов»).
 *
 * @param host адрес брокера (имя или IP); резолв — у хоста (на JVM — `Dispatchers.IO`).
 * @param credentials логин и пароль CONNECT; пустой логин приложение передаёт как `null`.
 * @param tls настройки TLS; `null` — обычный TCP. Применяет хост: на JVM — фабрика `MqttClient(options)`
 *   (TLS-транспорт с [verifyHostname]); общая фабрика `MqttClient(options, host, clock)` при `tls != null` отказывает
 *   (`IllegalArgumentException`) — открытым текстом клиент с `tls` не подключается никогда.
 * @param verifyHostname проверка имени хоста (и SNI) в сертификате брокера при TLS — **включена по умолчанию**, в том
 *   числе для закреплённого сертификата при подключении по IP (решение владельца 2026-10-16, W6 §8.2). Отключается
 *   только явно. Без [tls] не действует.
 * @param clientId `null` — случайный идентификатор.
 * @param keepAlive 0..65535 с; 0 — без keepAlive.
 * @param connectTimeout срок TCP (+TLS) подключения, включая резолв.
 * @param connackTimeout ожидание CONNACK после CONNECT.
 * @param closeLinger после DISCONNECT ждать закрытия соединения брокером (H1, K8): штатное закрытие без RST.
 * @param will will сессии (H3): брокер публикует его при обрыве, но не при штатном [MqttClient.close].
 */
public data class MqttClientOptions(
    val host: String,
    val port: Int,
    val version: MQTTVersion = MQTTVersion.MQTT5,
    val credentials: MqttCredentials? = null,
    val tls: TLSClientSettings? = null,
    val verifyHostname: Boolean = true,
    val clientId: String? = null,
    val keepAlive: Duration = 30.seconds,
    val connectTimeout: Duration = 30.seconds,
    val connackTimeout: Duration = 30.seconds,
    val closeLinger: Duration = 3.seconds,
    val will: MqttWill? = null,
    val reconnect: ReconnectPolicy = ReconnectPolicy(),
    val offlineQueue: OfflineQueue = OfflineQueue(),
    val limits: MqttLimits = MqttLimits(),
    val logger: MqttLogger = MqttLogger.None,
) {
    init {
        require(host.isNotBlank()) { "host is blank" }
        require(port in 1..65_535) { "port out of 1..65535" }
        require(!keepAlive.isNegative() && keepAlive.inWholeSeconds <= 65_535) { "keepAlive out of 0..65535 s" }
        require(connectTimeout.isPositive()) { "connectTimeout <= 0" }
        require(connackTimeout.isPositive()) { "connackTimeout <= 0" }
        require(!closeLinger.isNegative()) { "closeLinger < 0" }
    }

    override fun toString(): String =
        "MqttClientOptions(version=$version, credentials=$credentials, tls=${if (tls == null) "off" else "on"}, " +
            "verifyHostname=$verifyHostname, clientId=${if (clientId == null) "random" else "set"}, keepAlive=$keepAlive, " +
            "connectTimeout=$connectTimeout, connackTimeout=$connackTimeout, closeLinger=$closeLinger, will=$will, " +
            "reconnect=$reconnect, offlineQueue=$offlineQueue, limits=$limits)"
}

/** Логин и пароль CONNECT. `toString()` не печатает ни логин, ни пароль. Равенство — по содержимому пароля. */
public class MqttCredentials(public val username: String, public val password: ByteArray? = null) {
    override fun equals(other: Any?): Boolean =
        other is MqttCredentials && other.username == username && other.password.contentEquals(password)

    override fun hashCode(): Int = username.hashCode() * 31 + (password?.contentHashCode() ?: 0)

    override fun toString(): String = "MqttCredentials(username=***, password=${if (password == null) "null" else "***"})"
}

/**
 * Will сессии (H3). В MQTT 5 клиент сам добавляет пустые `willProperties` (без них брокер отвечает MALFORMED_PACKET).
 * QoS will не должен превышать QoS брокера (`willQosOf` приложения) — проверяет вызывающий. `toString()` — без топика
 * и нагрузки.
 */
public class MqttWill(
    public val topic: String,
    public val payload: ByteArray,
    public val retain: Boolean = false,
    public val qos: Qos = Qos.AT_MOST_ONCE,
) {
    override fun equals(other: Any?): Boolean =
        other is MqttWill && other.topic == topic && other.payload.contentEquals(payload) &&
            other.retain == retain && other.qos == qos

    override fun hashCode(): Int = ((topic.hashCode() * 31 + payload.contentHashCode()) * 31 + retain.hashCode()) * 31 + qos.hashCode()

    override fun toString(): String = "MqttWill(retain=$retain, qos=$qos, size=${payload.size})"
}

/**
 * Оффлайн-очередь публикаций (W6 §4.4): без живой сессии публикация ждёт не дольше [ttl] (затем `Expired`, брокеру не
 * уходит никогда); больше [capacity] ожидающих — `Failed(QueueFull)`.
 */
public data class OfflineQueue(val ttl: Duration = 10.seconds, val capacity: Int = 64) {
    init {
        require(!ttl.isNegative()) { "ttl < 0" }
        require(capacity >= 0) { "capacity < 0" }
    }
}

/**
 * Пределы памяти клиента (W6 §4.7, умолчания — §8.2 п. 10).
 *
 * @param maxIncomingPacket лимит входящего пакета: MQTT 5 — объявляется брокеру, 3.1.1 — больший пропускается
 *   (`Dropped`, сессия жива, H9).
 * @param maxTopicLength лимит топика входящего PUBLISH, байт UTF-8.
 * @param incomingBuffer сообщений в буфере [MqttClient.incoming] до паузы чтения сокета (W6 §4.5).
 * @param consumerStallLimit пауза чтения дольше — подключение закрывается с [MqttError.ConsumerStalled].
 * @param subscribeBatch фильтров в одном SUBSCRIBE/UNSUBSCRIBE.
 * @param maxFilters различных фильтров во всех группах; набор сверх предела отклоняется (группа не меняется).
 * @param commandQueue резерв: очередь команд фасада сейчас не ограничена (`Channel.UNLIMITED`, W6 §2.4), значение
 *   проверяется, но не применяется.
 */
public data class MqttLimits(
    val maxIncomingPacket: Int = 256 * 1024,
    val maxTopicLength: Int = 1024,
    val incomingBuffer: Int = 1024,
    val consumerStallLimit: Duration = 60.seconds,
    val subscribeBatch: Int = 50,
    val maxFilters: Int = 4096,
    val commandQueue: Int = 1024,
) {
    init {
        require(maxIncomingPacket >= 2) { "maxIncomingPacket < 2" }
        require(maxTopicLength >= 1) { "maxTopicLength < 1" }
        require(incomingBuffer >= 1) { "incomingBuffer < 1" }
        require(consumerStallLimit.isPositive()) { "consumerStallLimit <= 0" }
        require(subscribeBatch >= 1) { "subscribeBatch < 1" }
        require(maxFilters >= 1) { "maxFilters < 1" }
        require(commandQueue >= 1) { "commandQueue < 1" }
    }
}

/**
 * Журнал клиента (W6 §4.8): события без топиков, фильтров, нагрузок, адресов и паролей — только числа, коды и
 * [MqttError]. Вызывается на потоке хоста: реализация должна быть быстрой и не блокировать; исключения реализации
 * клиент глотает. Все методы необязательны.
 */
public interface MqttLogger {
    /** Сессия [epoch] установлена (CONNACK). */
    public fun connected(epoch: Long) {}

    /** Подключение кончилось; `error == null` — штатное закрытие. */
    public fun disconnected(error: MqttError?) {}

    /** Следующая попытка подключения — через [delayMs]. */
    public fun reconnectIn(delayMs: Long) {}

    /** Отправлен SUBSCRIBE на [count] фильтров. */
    public fun subscribe(count: Int) {}

    /** SUBACK отказал по [count] фильтрам (H7). */
    public fun refused(count: Int) {}

    /** Входящий пакет отброшен без разрыва (H9). */
    public fun dropped(reason: DropReason) {}

    /** PUBACK MQTT 5 с отказом [code]. */
    public fun publishRefused(code: ReasonCode) {}

    /** `subscriptions.set` отклонён: во всех группах было бы [filters] фильтров при пределе [limit]. */
    public fun groupRejected(filters: Int, limit: Int) {}

    public companion object {
        /** Ничего не пишет. */
        public val None: MqttLogger = object : MqttLogger {
            override fun toString(): String = "MqttLogger.None"
        }
    }
}
