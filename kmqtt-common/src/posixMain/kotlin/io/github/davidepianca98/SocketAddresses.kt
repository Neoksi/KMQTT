package io.github.davidepianca98

import kotlinx.cinterop.sizeOf
import platform.posix.AF_INET
import platform.posix.AF_INET6
import platform.posix.sockaddr_in

/**
 * Имя для `getaddrinfo`: литерал IPv6 в скобках (`[::1]`, как в URL и в `host:port`) — без скобок,
 * `getaddrinfo` скобок не принимает. Остальные имена не меняются.
 */
public fun hostForResolve(name: String): String =
    if (name.length > 2 && name.startsWith('[') && name.endsWith(']')) name.substring(1, name.length - 1) else name

/**
 * Семейство адреса, который вернул [getaddrinfo], по его длине: `sockaddr_in` — `AF_INET`, иначе
 * (`sockaddr_in6`) — `AF_INET6`. По длине, а не по полю `sa_family`: раскладка `sockaddr` на Apple другая
 * (`sa_len` + однобайтовое семейство), а длины у всех целей одинаковые (16 и 28 байт).
 */
public fun addressFamilyOf(length: UInt): Int =
    if (length.toLong() == sizeOf<sockaddr_in>()) AF_INET else AF_INET6
