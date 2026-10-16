package io.github.davidepianca98.client

import kotlin.math.min
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Политика переподключения нового клиента (W6 §4.2, H8). Перенос `ReconnectBackoff` приложения один-в-один
 * (контракт — `MqttReconnectTest`): задержка [initial], ×2 на каждую неудачную попытку, не больше [max]
 * (1 → 2 → 4 → 8 → 10 → 10 с); джиттер **только уменьшает** задержку (до доли [jitter] от неё), поэтому верхняя граница
 * детерминирована, а клиенты одного дома не стучатся в брокер одновременно.
 *
 * Счётчик попыток живёт в движке, а не здесь: политика — только конфигурация. Сброс счётчика — **только после
 * стабильной сессии**: все SUBSCRIBE подключения подтверждены SUBACK без отказов **или** подключение прожило не меньше
 * [stableAfter] после CONNACK. SUBACK с отказом успешным не считается: брокер, принимающий CONNECT и отказывающий
 * в подписке (ACL), иначе держал бы клиента в цикле «подключение раз в секунду».
 *
 * @param maxAttempts сколько переподключений подряд разрешено после конца подключения (счётчик сбрасывает стабильная
 *   сессия вместе с задержкой); исчерпано — клиент закрывается с последней ошибкой (`ConnectionState.Closed(error)`).
 *   `null` — бесконечно; `0` — без переподключений ([Never]).
 * @param random источник случайности в `[0, 1)` (тесты подставляют свой).
 */
public data class ReconnectPolicy(
    val initial: Duration = 1.seconds,
    val max: Duration = 10.seconds,
    val jitter: Double = 0.2,
    val stableAfter: Duration = 30.seconds,
    val maxAttempts: Int? = null,
    val random: () -> Double = { Random.nextDouble() },
) {
    init {
        require(initial.isPositive()) { "initial <= 0" }
        require(max >= initial) { "max < initial" }
        require(jitter in 0.0..1.0) { "jitter out of 0..1" }
        require(!stableAfter.isNegative()) { "stableAfter < 0" }
        require(maxAttempts == null || maxAttempts >= 0) { "maxAttempts < 0" }
    }

    /**
     * Задержка перед попыткой номер [attempt] (0 — первая после сброса), мс. Формула `ReconnectBackoff.next()`:
     * `base = attempt ≥ 20 ? max : min(max, initial shl attempt)`, затем `base · (1 − jitter · random())`.
     */
    internal fun delayMs(attempt: Int): Long {
        val initialMs = initial.inWholeMilliseconds
        val maxMs = max.inWholeMilliseconds
        val base = if (attempt >= MAX_SHIFT) maxMs else min(maxMs, initialMs shl attempt)
        return (base * (1.0 - jitter * random())).toLong()
    }

    public companion object {
        private const val MAX_SHIFT = 20

        /** Без переподключений: первый конец подключения закрывает клиент (`MQTTTestConnection`, проба брокера). */
        public val Never: ReconnectPolicy = ReconnectPolicy(maxAttempts = 0)
    }
}
