package io.github.davidepianca98.client

import kotlin.math.min
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Reconnection policy of an [MqttClient]. The delay before an attempt starts at [initial], doubles after every failed
 * attempt and never exceeds [max] (with the defaults 1, 2, 4, 8, 10, 10 seconds). Jitter only ever shortens the delay
 * (by up to the fraction [jitter]), so the upper bound is deterministic while several clients do not hit the broker at
 * the same moment.
 *
 * The attempt counter lives in the client, not here: the policy is configuration only. The counter and the delay are
 * reset only after a stable session: either every SUBSCRIBE of the connection was acknowledged without refusals, or the
 * connection lived at least [stableAfter] after CONNACK. A SUBACK that refuses a filter does not count as success;
 * otherwise a broker that accepts CONNECT but denies the subscription (for example by ACL) would keep the client in a
 * loop of one connection per second.
 *
 * @param initial delay before the first retry; must be positive.
 * @param max upper bound of the delay; at least [initial].
 * @param jitter fraction 0..1 by which the delay may be shortened at random (0 disables jitter).
 * @param stableAfter lifetime after CONNACK that makes a session stable on its own.
 * @param maxAttempts how many reconnection attempts in a row are allowed after a connection ended (a stable session
 *   resets the counter together with the delay); when they are exhausted the client closes with the last error
 *   ([ConnectionState.Closed] carries it). `null` retries forever; `0` means no reconnection, see [Never].
 * @param random source of random numbers in `[0, 1)`; tests supply their own.
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
     * Delay before attempt number [attempt] (0 is the first after a reset), in milliseconds:
     * `base = attempt >= 20 ? max : min(max, initial shl attempt)`, then `base * (1 - jitter * random())`.
     */
    internal fun delayMs(attempt: Int): Long {
        val initialMs = initial.inWholeMilliseconds
        val maxMs = max.inWholeMilliseconds
        val base = if (attempt >= MAX_SHIFT) maxMs else min(maxMs, initialMs shl attempt)
        return (base * (1.0 - jitter * random())).toLong()
    }

    public companion object {
        private const val MAX_SHIFT = 20

        /**
         * No reconnection: the first end of a connection closes the client. Useful for a one-shot connection check;
         * [MqttClient.connect] then returns the result of that single attempt.
         */
        public val Never: ReconnectPolicy = ReconnectPolicy(maxAttempts = 0)
    }
}
