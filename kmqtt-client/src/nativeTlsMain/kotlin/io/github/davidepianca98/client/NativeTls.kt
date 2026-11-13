package io.github.davidepianca98.client

import io.github.davidepianca98.socket.IOException
import io.github.davidepianca98.socket.tls.TLSClientSettings

/**
 * Клиентская TLS-сессия поверх памяти: OpenSSL с memory-BIO, без сокета. Шифротекст от собеседника
 * кладётся [feed], шифротекст для собеседника забирается [takeOutput]; открытый текст — [readPlain]/[writePlain].
 * Сокетом, очередями и событиями владеет [NativeTlsTransport]. Все методы — только на потоке хоста.
 *
 * Ошибки — [NativeTlsException] с видом отказа (`kind` для [MqttError.Io]) и текстом без адресов, имён из сертификата
 * и путей (H11): текст ошибок OpenSSL наружу не передаётся.
 */
internal interface NativeTlsSession {
    /** Рукопожатие закончено (после [handshake], вернувшего `true`). */
    val isHandshakeDone: Boolean

    /** Шаг рукопожатия: `true` — закончено, `false` — нужны данные собеседника. Отказ — [NativeTlsException]. */
    fun handshake(): Boolean

    /** Шифротекст от собеседника (принимается целиком, буфер памяти растёт). */
    fun feed(bytes: ByteArray)

    /**
     * Расшифровать в [into]: число байтов (> 0), `0` — сейчас нечего (нужен шифротекст), [READ_CLOSED] — собеседник
     * прислал close_notify. Ошибка TLS — [NativeTlsException].
     */
    fun readPlain(into: ByteArray): Int

    /** Зашифровать [length] байтов [bytes] с [offset]: число принятых (`0` — сейчас не принимает). */
    fun writePlain(bytes: ByteArray, offset: Int, length: Int): Int

    /** Накопленный шифротекст для собеседника или `null`. */
    fun takeOutput(): ByteArray?

    /** Поставить close_notify (по возможности, без ожидания ответа); дальше открытый текст не пишется. */
    fun shutdown()

    /** Освободить сессию. Повторный вызов безвреден. */
    fun free()

    companion object {
        /** [readPlain]: собеседник закрыл TLS (close_notify). */
        const val READ_CLOSED: Int = -1
    }
}

/** Новая сессия на каждое подключение; [host] — адрес подключения (SNI и проверка имени хоста). */
internal fun interface NativeTlsSessionFactory {
    /** Отказ настроек (сертификат, ключ, нечитаемый файл) — [NativeTlsException] с видом [KIND_TLS_CONFIG]. */
    fun open(host: String): NativeTlsSession
}

/**
 * Отказ TLS на native: [kind] — вид для [MqttError.Io] (`ssl`, `ssl-config`, `ssl-trust`, `ssl-hostname`,
 * `ssl-expired`, как у `TlsTransport` JVM), текст — общий, без адреса и без текста ошибок OpenSSL.
 */
internal class NativeTlsException(val kind: String, message: String) : IOException(message)

internal const val KIND_TLS = "ssl"
internal const val KIND_TLS_CONFIG = "ssl-config"
internal const val KIND_TLS_TRUST = "ssl-trust"
internal const val KIND_TLS_HOSTNAME = "ssl-hostname"
internal const val KIND_TLS_EXPIRED = "ssl-expired"

/**
 * Фабрика сессий OpenSSL по настройкам (реализация — `src/nativeTlsOpenssl`, отдельно в каждой цели): доверие
 * (закреплённый сертификат или системное хранилище), ключ клиента, ALPN и SNI, проверка имени хоста — как у
 * `TlsTransport` JVM. Контекст OpenSSL строится на каждое подключение (ошибка настроек не запоминается).
 */
internal expect fun openSslSessionFactory(settings: TLSClientSettings, verifyHostname: Boolean): NativeTlsSessionFactory

/**
 * Системные корневые сертификаты платформы для доверия без закрепления: файлы-связки PEM и каталоги с хэш-именами
 * (OpenSSL `CAfile`/`CApath`), которые есть на этой ОС. Несуществующие пропускаются при загрузке.
 */
internal expect fun systemCaFiles(): List<String>

/** Каталоги корневых сертификатов (`CApath`) платформы. */
internal expect fun systemCaDirs(): List<String>

/**
 * Корневые сертификаты из хранилища ОС в DER (Windows: системное хранилище `ROOT` через CryptoAPI); на остальных
 * платформах — пусто (там связки PEM, [systemCaFiles]).
 */
internal expect fun systemStoreCertificates(): List<ByteArray>

/** Адрес — IP-литерал (IPv4 с точками или IPv6, в том числе в скобках `[::1]`)? Тогда SNI не шлётся, имя — IP SAN. */
internal fun ipLiteralOrNull(host: String): String? {
    val bare = host.removePrefix("[").removeSuffix("]")
    if (bare.contains(':')) {
        return if (bare.all { it.isDigit() || it in 'a'..'f' || it in 'A'..'F' || it == ':' || it == '.' }) bare else null
    }
    val parts = bare.split('.')
    if (parts.size != 4) return null
    return if (parts.all { p -> p.isNotEmpty() && p.length <= 3 && p.all { it.isDigit() } && p.toInt() <= 255 }) bare else null
}
