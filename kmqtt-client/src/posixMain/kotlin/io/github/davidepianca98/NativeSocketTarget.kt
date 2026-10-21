package io.github.davidepianca98

import io.github.davidepianca98.socket.IOException
import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.readBytes
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.usePinned
import platform.posix.SOCK_STREAM

/**
 * Разрешённый адрес старого блокирующего клиента на native (`ClientSocket`, `TLSClientSocket`), семейство
 * (`AF_INET` или `AF_INET6`) и байты `sockaddr`. Сокет создаётся по адресу, а не всегда `AF_INET`.
 */
internal class NativeSocketTarget(val family: Int, private val address: ByteArray) {

    /** Новый TCP-сокет семейства адреса (дескриптор или -1). */
    fun openSocket(): Int = socket(family, SOCK_STREAM, 0)

    /** Блокирующий `connect` на этот адрес: 0 или -1 (код — `getErrno()`). */
    fun connectTo(fd: Int): Int = address.usePinned { pinned ->
        connect(fd, pinned.addressOf(0).reinterpret(), address.size.toUInt())
    }

    companion object {
        /** `socketsInit()` и резолв (IPv4 предпочтительно, затем IPv6; `[::1]` допустим); не разрешилось — `IOException`. */
        fun resolve(address: String, port: Int): NativeSocketTarget {
            socketsInit()
            val target = memScoped {
                getaddrinfo(address, port.toString())?.let { (pointer, length) ->
                    val bytes = pointer.reinterpret<ByteVar>().readBytes(length.toInt())
                    NativeSocketTarget(addressFamilyOf(length), bytes)
                }
            }
            if (target == null) {
                socketsCleanup()
                throw IOException("Failed resolving address")
            }
            return target
        }
    }
}
