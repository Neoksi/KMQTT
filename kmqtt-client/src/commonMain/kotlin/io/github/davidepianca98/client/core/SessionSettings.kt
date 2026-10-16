package io.github.davidepianca98.client.core

import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos

/**
 * Логин и пароль CONNECT. `toString()` не печатает ни логин, ни пароль (H11).
 *
 * Пароль без логина запрещён протоколом (MQTT 3.1.1 §3.1.2.9); проверка — в [SessionSettings].
 */
@MqttCoreApi
public class SessionCredentials(
    public val login: String?,
    public val password: ByteArray?,
) {
    override fun toString(): String =
        "SessionCredentials(login=${if (login == null) "null" else "***"}, password=${if (password == null) "null" else "***"})"
}

/**
 * Will сессии. Для MQTT 5 ядро само добавляет пустые `willProperties` (без них CONNECT собирается с ошибкой, а брокер
 * отвечает MALFORMED_PACKET — H3, `MqttClientFactory.kt:59-65` приложения). `toString()` не печатает топик и нагрузку.
 */
@MqttCoreApi
public class WillSpec(
    public val topic: String,
    public val payload: ByteArray,
    public val retain: Boolean = false,
    public val qos: Qos = Qos.AT_MOST_ONCE,
) {
    override fun toString(): String = "WillSpec(retain=$retain, qos=$qos, size=${payload.size})"
}

/**
 * Параметры одного подключения для [MqttSessionCore]. Проверки — при создании (`IllegalArgumentException`, текст без
 * логина, пароля и топиков), как в `MQTTClient.kt:176-191`.
 *
 * @param clientId null — случайный идентификатор (как у старого клиента).
 * @param keepAliveSec 0..65535; 0 — keepAlive выключен.
 * @param cleanStart флаг Clean Start (MQTT 5) / Clean Session (3.1.1) в CONNECT. Ядро живёт одно подключение и само
 *   ничего не досылает; при `false` продолжение сессии (досылка неподтверждённого с DUP = 1 при Session Present = 1)
 *   ведёт движок через [MqttSessionCore.resendPublish]/[MqttSessionCore.resendPubrel] (SEVEN-251). При `true` CONNACK
 *   с Session Present = 1 — ошибка протокола.
 * @param sessionExpiryIntervalSec MQTT 5 Session Expiry Interval в секундах (0..4294967295; 4294967295 — сессия не
 *   истекает); `null` — свойство не отправляется (у брокера 0: сессия кончается с соединением). В 3.1.1 не
 *   используется: срок сессии при Clean Session = 0 задаёт брокер.
 * @param connackTimeoutMs ожидание CONNACK от [MqttSessionCore.start].
 * @param closeLingerMs после DISCONNECT ждать закрытия соединения брокером (H1); 0 — `Closed(null)` сразу.
 * @param maxIncomingPacket лимит входящего пакета (с фиксированным заголовком); в MQTT 5 объявляется брокеру
 *   свойством Maximum Packet Size, в 3.1.1 пакет больше лимита пропускается (`Dropped`, H9).
 * @param maxTopicLength лимит топика входящего PUBLISH в байтах UTF-8.
 * @param receiveMaximum Receive Maximum клиента (MQTT 5: объявляется, если ≠ 65535): сколько входящих QoS 2
 *   может ждать PUBREL. На исходящие публикации влияет только Receive Maximum брокера из CONNACK.
 * @param topicAliasMaximum Topic Alias Maximum клиента (MQTT 5). По умолчанию 0 — не объявляется, и брокер не вправе
 *   присылать алиасы. Решение SEVEN-222: приложению алиасы не нужны, а PUBLISH, отброшенный фреймером как
 *   `TopicTooLong`, мог задавать алиас — сопоставление теряется (см. [MqttSessionCore]).
 */
@MqttCoreApi
public class SessionSettings(
    public val version: MQTTVersion,
    public val clientId: String?,
    public val credentials: SessionCredentials? = null,
    public val keepAliveSec: Int,
    public val cleanStart: Boolean = true,
    public val will: WillSpec? = null,
    public val connackTimeoutMs: Long = 30_000,
    public val closeLingerMs: Long = 3_000,
    public val maxIncomingPacket: Int = 256 * 1024,
    public val maxTopicLength: Int = 1024,
    public val receiveMaximum: Int = 65_535,
    public val topicAliasMaximum: Int = 0,
    public val sessionExpiryIntervalSec: Long? = null,
) {
    init {
        require(keepAliveSec in 0..65_535) { "Keep alive exceeding the maximum value" }
        require(credentials == null || credentials.login != null || credentials.password == null) {
            "Cannot set password without username"
        }
        require(will == null || will.topic.isNotEmpty()) { "Will topic empty, but other will options have been set" }
        require(will == null || !hasWildcard(will.topic)) { "Will topic must not contain wildcards" }
        require(connackTimeoutMs > 0) { "connackTimeoutMs <= 0" }
        require(closeLingerMs >= 0) { "closeLingerMs < 0" }
        require(maxIncomingPacket >= 2) { "maxIncomingPacket < 2" }
        require(maxTopicLength >= 1) { "maxTopicLength < 1" }
        require(receiveMaximum in 1..65_535) { "receiveMaximum out of 1..65535" }
        require(topicAliasMaximum in 0..65_535) { "topicAliasMaximum out of 0..65535" }
        require(sessionExpiryIntervalSec == null || sessionExpiryIntervalSec in 0..MAX_SESSION_EXPIRY) {
            "sessionExpiryIntervalSec out of 0..4294967295"
        }
    }

    override fun toString(): String =
        "SessionSettings(version=$version, clientId=${if (clientId == null) "null" else "set"}, " +
            "credentials=$credentials, keepAliveSec=$keepAliveSec, cleanStart=$cleanStart, will=$will, " +
            "connackTimeoutMs=$connackTimeoutMs, closeLingerMs=$closeLingerMs, maxIncomingPacket=$maxIncomingPacket, " +
            "maxTopicLength=$maxTopicLength, receiveMaximum=$receiveMaximum, topicAliasMaximum=$topicAliasMaximum, " +
            "sessionExpiryIntervalSec=$sessionExpiryIntervalSec)"

    public companion object {
        /** Наибольший Session Expiry Interval MQTT 5 (4294967295 с — «не истекает»). */
        public const val MAX_SESSION_EXPIRY: Long = 0xFFFF_FFFFL

        private fun hasWildcard(topic: String): Boolean =
            topic.indexOf('+') >= 0 || topic.indexOf('#') >= 0 || topic.indexOf('\u0000') >= 0
    }
}
