package io.github.davidepianca98.client

/**
 * Корни для OpenSSL на Apple: на macOS — связка `/etc/ssl/cert.pem`; на iOS, tvOS и watchOS файловой связки нет, а
 * хранилище доверия системы OpenSSL не видит — без закреплённого сертификата (`serverCertificate`) проверка не
 * пройдёт (`ssl-trust`).
 */
internal actual fun systemCaFiles(): List<String> = listOf("/etc/ssl/cert.pem")

internal actual fun systemCaDirs(): List<String> = emptyList()

internal actual fun systemStoreCertificates(): List<ByteArray> = emptyList()
