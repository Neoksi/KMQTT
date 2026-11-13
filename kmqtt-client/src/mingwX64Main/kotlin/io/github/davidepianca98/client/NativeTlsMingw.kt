package io.github.davidepianca98.client

import kotlinx.cinterop.pointed
import kotlinx.cinterop.readBytes
import platform.windows.CertCloseStore
import platform.windows.CertEnumCertificatesInStore
import platform.windows.CertOpenSystemStoreW

/** На Windows связок PEM нет: корни — из системного хранилища ([systemStoreCertificates]). */
internal actual fun systemCaFiles(): List<String> = emptyList()

internal actual fun systemCaDirs(): List<String> = emptyList()

/**
 * Корневые сертификаты системного хранилища Windows `ROOT` (текущий пользователь, включая унаследованные от машины)
 * через CryptoAPI, в DER. Сертификаты, которые Windows докачивает по сети при построении цепочки
 * (Windows Update), здесь не появятся, пока их нет в хранилище.
 */
internal actual fun systemStoreCertificates(): List<ByteArray> {
    val store = CertOpenSystemStoreW(0u, "ROOT") ?: return emptyList()
    val out = ArrayList<ByteArray>()
    try {
        var context = CertEnumCertificatesInStore(store, null)
        while (context != null) {
            val c = context.pointed
            val encoded = c.pbCertEncoded
            if (encoded != null && c.cbCertEncoded > 0u) out += encoded.readBytes(c.cbCertEncoded.toInt())
            context = CertEnumCertificatesInStore(store, context)
        }
    } finally {
        CertCloseStore(store, 0u)
    }
    return out
}
