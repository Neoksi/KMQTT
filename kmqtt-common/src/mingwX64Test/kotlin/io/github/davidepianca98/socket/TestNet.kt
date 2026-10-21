package io.github.davidepianca98.socket

import kotlinx.cinterop.*
import platform.posix.sockaddr
import platform.posix.sockaddr_storage

/** Платформенные мелочи общих тестов `src/nativeSocketTest` на mingwX64. */
internal object TestNet {

    /** Локальный порт сокета (IPv4 и IPv6: порт в `sockaddr_in`/`sockaddr_in6` лежит по смещению 2, сетевой порядок). */
    fun localPort(fd: Int): Int = memScoped {
        val address = alloc<sockaddr_storage>()
        val length = alloc<IntVar>()
        length.value = sizeOf<sockaddr_storage>().toInt()
        check(platform.posix.getsockname(fd.convert(), address.ptr.reinterpret<sockaddr>(), length.ptr) == 0) {
            "getsockname failed"
        }
        val bytes = address.ptr.reinterpret<ByteVar>()
        ((bytes[2].toInt() and 0xFF) shl 8) or (bytes[3].toInt() and 0xFF)
    }

    fun sleepMs(ms: Int) {
        platform.windows.Sleep(ms.convert())
    }
}
