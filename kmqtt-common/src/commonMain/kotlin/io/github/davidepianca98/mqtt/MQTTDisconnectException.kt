package io.github.davidepianca98.mqtt

import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode

public class MQTTDisconnectException(
    public val reason: String,
    public val reasonCode: ReasonCode? = null
) : Exception(
    buildString {
        if (reasonCode != null) {
            append(reasonCode.toString())
            append("/n")
        }
        append(reason)
    })
