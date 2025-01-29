package io.github.davidepianca98.mqtt

public class MQTTProtocolException(public val reason: String) : Exception(reason)
