package io.github.davidepianca98.client.internal

import io.github.davidepianca98.client.MqttClientOptions
import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.mqtt.MQTTVersion
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** Параметры клиента → параметры движка: окно продолжения сессии (`sessionResumeWindow`). */
@OptIn(MqttCoreApi::class)
class EngineOptionsMappingTest {
    private val base = MqttClientOptions(host = "h", port = 1883, clientId = "client1", keepAlive = 15.seconds)

    @Test
    fun resumeWindowGivesACleanVariantOfTheSameSettings() {
        val options = base.copy(
            version = MQTTVersion.MQTT3_1_1,
            persistentSession = true,
            sessionResumeWindow = 2.minutes,
        ).toEngineOptions()
        assertEquals(120_000L, options.resumeWindowMs)
        assertFalse(options.session.cleanStart)
        val clean = assertNotNullOf(options.cleanSession)
        assertTrue(clean.cleanStart)
        assertEquals(options.session.clientId, clean.clientId)
        assertEquals(options.session.keepAliveSec, clean.keepAliveSec)
        assertEquals(options.session.maxIncomingPacket, clean.maxIncomingPacket)
        assertEquals(options.session.sessionExpiryIntervalSec, clean.sessionExpiryIntervalSec)
    }

    @Test
    fun withoutPersistentSessionTheWindowIsIgnored() {
        val options = base.copy(sessionResumeWindow = 2.minutes).toEngineOptions()
        assertNull(options.resumeWindowMs)
        assertNull(options.cleanSession)
        assertTrue(options.session.cleanStart)
    }

    @Test
    fun withoutAWindowThePersistentSessionAlwaysResumes() {
        val options = base.copy(persistentSession = true, sessionExpiry = 2.minutes).toEngineOptions()
        assertNull(options.resumeWindowMs)
        assertNull(options.cleanSession)
        assertEquals(120L, options.session.sessionExpiryIntervalSec)
    }

    @Test
    fun zeroWindowIsRefused() {
        assertFailsWith<IllegalArgumentException> { base.copy(sessionResumeWindow = 0.seconds) }
    }

    @Test
    fun closeDrainIsMappedAndNegativeIsRefused() {
        // досылка очереди перед DISCONNECT
        assertEquals(3_000L, base.toEngineOptions().closeDrainMs)
        assertEquals(1_500L, base.copy(closeDrain = 1.5.seconds).toEngineOptions().closeDrainMs)
        assertEquals(0L, base.copy(closeDrain = 0.seconds).toEngineOptions().closeDrainMs)
        assertFailsWith<IllegalArgumentException> { base.copy(closeDrain = (-1).seconds) }
        assertTrue(base.toString().contains("closeDrain=3s"))
    }

    private fun <T : Any> assertNotNullOf(value: T?): T = kotlin.test.assertNotNull(value)
}
