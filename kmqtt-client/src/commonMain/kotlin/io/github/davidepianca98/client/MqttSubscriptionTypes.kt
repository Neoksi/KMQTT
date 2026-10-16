package io.github.davidepianca98.client

import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode

// Public types of the subscriptions API. No toString() here prints filters.

/** State of a desired topic filter in the current session ([MqttSubscriptions.state]). */
public sealed interface TopicState {
    /** There is no live session, or the SUBSCRIBE for the filter was sent and the SUBACK is still awaited. */
    public data object Pending : TopicState

    /** The broker subscribed the filter with the granted [qos] (SUBACK 0, 1 or 2). */
    public data class Granted(val qos: Qos) : TopicState

    /** The broker refused the filter (SUBACK 0x80 or above, for example 0x87 not authorized). The session continues. */
    public data class Refused(val code: ReasonCode) : TopicState
}

/**
 * The broker refused to subscribe to [filter] of the group [group] ([MqttSubscriptions.refusals]). The session
 * continues and the other filters work. A filter shared by several groups yields one event per group. [toString] does
 * not print the filter.
 */
public class SubscribeRefused(public val group: String, public val filter: String, public val code: ReasonCode) {
    override fun equals(other: Any?): Boolean =
        other is SubscribeRefused && other.group == group && other.filter == filter && other.code == code

    override fun hashCode(): Int = (group.hashCode() * 31 + filter.hashCode()) * 31 + code.hashCode()

    override fun toString(): String = "SubscribeRefused(code=$code)"
}
