package io.github.davidepianca98.client

/**
 * Связки корневых сертификатов дистрибутивов Linux: Debian/Ubuntu, Fedora/RHEL, openSUSE, Alpine. OpenSSL
 * в klib форка собран со своим `OPENSSLDIR`, который на целевой машине обычно не существует, поэтому пути ОС
 * перечислены явно; отсутствующие пропускаются.
 */
internal actual fun systemCaFiles(): List<String> = listOf(
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/ca-bundle.pem",
    "/etc/pki/tls/cacert.pem",
    "/etc/ssl/cert.pem"
)

internal actual fun systemCaDirs(): List<String> = listOf("/etc/ssl/certs")

internal actual fun systemStoreCertificates(): List<ByteArray> = emptyList()
