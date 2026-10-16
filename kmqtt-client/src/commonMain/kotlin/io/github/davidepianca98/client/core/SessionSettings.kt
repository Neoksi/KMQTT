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
 * @param cleanStart ядро живёт одно подключение и не досылает неподтверждённое после переподключения, поэтому
 *   `cleanStart = false` поддержан только на уровне флага CONNECT (сессия на стороне брокера); досылка — забота движка.
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
    }

    override fun toString(): String =
        "SessionSettings(version=$version, clientId=${if (clientId == null) "null" else "set"}, " +
            "credentials=$credentials, keepAliveSec=$keepAliveSec, cleanStart=$cleanStart, will=$will, " +
            "connackTimeoutMs=$connackTimeoutMs, closeLingerMs=$closeLingerMs, maxIncomingPacket=$maxIncomingPacket, " +
            "maxTopicLength=$maxTopicLength, receiveMaximum=$receiveMaximum, topicAliasMaximum=$topicAliasMaximum)"

    private companion object {
        fun hasWildcard(topic: String): Boolean =
            topic.indexOf('+') >= 0 || topic.indexOf('#') >= 0 || topic.indexOf('\u0000') >= 0
    }
}
