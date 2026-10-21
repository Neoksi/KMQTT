package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.socket.tls.TLSClientSettings

/**
 * Фабрика TLS-транспорта для [WinSockHost]: TLS на OpenSSL с memory-BIO поверх [WinSockTransport]
 * (`NativeTlsTransport`), правила доверия и проверки имени хоста — как у `NioTransportFactory.tls` JVM. Настройки
 * ([settings]) не печатаются и не попадают в ошибки (в них пароль и ключ клиента). Контекст OpenSSL строится на каждое
 * подключение.
 */
@MqttCoreApi
public fun WinSockTransportFactory.Companion.tls(
    settings: TLSClientSettings,
    verifyHostname: Boolean = true
): WinSockTransportFactory {
    val sessions = openSslSessionFactory(settings, verifyHostname)
    return WinSockTransportFactory { NativeTlsTransport(WinSockTransport(it), sessions) }
}
