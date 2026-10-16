package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.internal.MqttClientImpl

/**
 * Клиент на собственном потоке хоста `kmqtt-io-<n>` ([NioActorHost], часы [MqttClock.Monotonic]) — основная фабрика
 * на JVM (S06b-T05b).
 *
 * TLS (решение владельца 2026-10-16): при [MqttClientOptions.tls] `!= null` хост строится с TLS-транспортом
 * `NioTransportFactory.tls(tls, verifyHostname)` — проверка имени хоста и SNI включены, пока
 * [MqttClientOptions.verifyHostname] не выключен явно; без `tls` — обычный TCP. Открытым текстом при заданном `tls`
 * клиент не подключается никогда: общая фабрика `MqttClient(options, host, clock)` с `tls != null` отказывает.
 *
 * Проверка параметров — `IllegalArgumentException` (текст без адресов и секретов).
 */
@OptIn(MqttCoreApi::class)
public fun MqttClient(options: MqttClientOptions): MqttClient {
    val clock = MqttClock.Monotonic
    val tls = options.tls ?: return MqttClientImpl(options, NioActorHost(nextHostThreadName(), clock), clock)
    val host = NioActorHost(
        nextHostThreadName(),
        clock,
        NioTransportFactory.tls(tls, options.verifyHostname),
        NioActorHost.DEFAULT_READ_BUFFER
    )
    return MqttClientImpl(options, host, clock, hostAppliesTls = true)
}
