package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.socket.tls.TLSClientSettings

/**
 * Фабрика TLS-транспорта для [PosixHost]: TLS на OpenSSL с memory-BIO поверх [PosixTransport]
 * (`NativeTlsTransport`), правила доверия и проверки имени хоста — как у `NioTransportFactory.tls` JVM. Настройки
 * ([settings]) не печатаются и не попадают в ошибки (в них пароль и ключ клиента). Контекст OpenSSL строится на каждое
 * подключение.
 */
@MqttCoreApi
public fun PosixTransportFactory.Companion.tls(
    settings: TLSClientSettings,
    verifyHostname: Boolean = true
): PosixTransportFactory {
    val sessions = openSslSessionFactory(settings, verifyHostname)
    return PosixTransportFactory { NativeTlsTransport(PosixTransport(it), sessions) }
}
