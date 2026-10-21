package io.github.davidepianca98

import kotlinx.cinterop.*
import platform.posix.*

/**
 * Apple: у `send` нет надёжного флага против `SIGPIPE` (`MSG_NOSIGNAL` не обещан на всех версиях ОС), защита —
 * `SO_NOSIGPIPE` на самом сокете: его ставят [socket] и [accept] ([markNoSigPipe]). Запись в сброшенное
 * соединение возвращает ошибку `EPIPE`, а не убивает процесс сигналом.
 */
public actual fun send(socket: Int, buf: CValuesRef<ByteVar>?, len: Int, flags: Int): Int {
    return platform.posix.send(socket.convert(), buf, len.convert(), flags).convert()
}

public actual fun recv(socket: Int, buf: CValuesRef<ByteVar>?, len: Int, flags: Int): Int {
    return platform.posix.recv(socket, buf, len.convert(), flags).convert()
}

public actual fun shutdown(socket: Int): Int {
    return shutdown(socket, SHUT_WR)
}

public actual fun close(socket: Int): Int {
    return platform.posix.close(socket)
}

public actual fun memset(__s: CValuesRef<*>?, __c: Int, __n: ULong): CPointer<out CPointed>? {
    return platform.posix.memset(__s, __c, __n.convert())
}

public actual fun sendto(
    __fd: Int,
    __buf: CValuesRef<*>?,
    __n: ULong,
    __flags: Int,
    __addr: CValuesRef<sockaddr>?,
    __addr_len: UInt
): Long {
    return platform.posix.sendto(__fd, __buf, __n.convert(), __flags, __addr, __addr_len).convert()
}

public actual fun recvfrom(
    __fd: Int,
    __buf: CValuesRef<*>?,
    __n: ULong,
    __flags: Int,
    __addr: CValuesRef<sockaddr>?,
    __addr_len: CValuesRef<UIntVarOf<UInt>>?
): Long {
    return platform.posix.recvfrom(__fd, __buf, __n.convert(), __flags, __addr, __addr_len).convert()
}

public actual fun inet_ntop(
    __af: Int,
    __cp: CValuesRef<*>?,
    __buf: CValuesRef<ByteVarOf<Byte>>?,
    __len: UInt
): CPointer<ByteVarOf<Byte>>? {
    return platform.darwin.inet_ntop(__af, __cp, __buf, __len)
}

public actual fun inet_pton(__af: Int, __cp: String?, __buf: CValuesRef<*>?): Int {
    return platform.darwin.inet_pton(__af, __cp, __buf)
}

private fun htons(value: UShort): UShort {
    return if (BYTE_ORDER == BIG_ENDIAN) {
        value
    } else {
        platform.builtin.builtin_bswap16(value.convert()).convert()
    }
}

public actual fun MemScope.sockaddrIn(sin_family: UShort, sin_port: UShort): sockaddr_in {
    val sockaddr = alloc<sockaddr_in>()
    platform.posix.memset(sockaddr.ptr, 0, sizeOf<sockaddr_in>().convert())
    sockaddr.sin_family = sin_family.toUByte()
    sockaddr.sin_port = htons(sin_port)
    return sockaddr
}

public actual fun MemScope.getPortFromSockaddrIn(sockaddr: sockaddr_in): UShort {
    return sockaddr.sin_port
}

public actual fun setsockopt(__fd: Int, __level: Int, __optname: Int, __optval: CValuesRef<*>?, __optlen: UInt): Int {
    return platform.posix.setsockopt(__fd.convert(), __level, __optname, __optval, __optlen.convert())
}

public actual fun bind(__fd: Int, __addr: CValuesRef<sockaddr>?, __len: UInt): Int {
    return platform.posix.bind(__fd, __addr, __len)
}

public actual fun set_non_blocking(__fd: Int): Int {
    return fcntl(__fd, F_SETFL, O_NONBLOCK)
}

public actual fun socket(__domain: Int, __type: Int, __protocol: Int): Int {
    val fd = platform.posix.socket(__domain, __type, __protocol)
    if (fd >= 0) markNoSigPipe(fd)
    return fd
}

public actual fun connect(__fd: Int, __addr: CValuesRef<sockaddr>?, __len: UInt): Int {
    return platform.posix.connect(__fd, __addr, __len)
}

public actual fun accept(__fd: Int, __addr: CValuesRef<sockaddr>?, __addr_len: CValuesRef<UIntVarOf<UInt>>?): Int {
    val fd = platform.posix.accept(__fd, __addr, __addr_len)
    if (fd >= 0) markNoSigPipe(fd)
    return fd
}

/** `SO_NOSIGPIPE`: запись в сброшенное соединение даёт `EPIPE` вместо сигнала `SIGPIPE` (Apple). */
private fun markNoSigPipe(fd: Int) {
    memScoped {
        val one = alloc<IntVar>()
        one.value = 1
        platform.posix.setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, one.ptr, sizeOf<IntVar>().convert())
    }
}

public actual fun listen(__fd: Int, __n: Int): Int {
    return platform.posix.listen(__fd, __n)
}

public actual fun MemScope.select(
    __nfds: Int,
    __readfds: CValuesRef<fd_set>?,
    __writefds: CValuesRef<fd_set>?,
    __exceptfds: CValuesRef<fd_set>?,
    timeout: Long
): Int {
    val timeoutStruct = alloc<timeval>()
    // tv_usec должен быть меньше 1 000 000: целые секунды отдельно (как в set_*_socket_timeout)
    val seconds = timeout.coerceAtLeast(0) / 1000
    timeoutStruct.tv_sec = seconds.convert()
    timeoutStruct.tv_usec = ((timeout.coerceAtLeast(0) - seconds * 1000) * 1000).convert()
    return select(__nfds, __readfds, __writefds, __exceptfds, timeoutStruct.ptr)
}

public actual fun socketsInit() {}

public actual fun socketsCleanup() {}

public actual fun getErrno(): Int = errno

public actual fun getEagain(): Int = EAGAIN

public actual fun getEwouldblock(): Int = EWOULDBLOCK

public actual fun MemScope.set_send_socket_timeout(__fd: Int, timeout: Long): Int {
    val timeoutStruct = alloc<timeval>()
    val seconds = timeout / 1000
    timeoutStruct.tv_sec = seconds.convert()
    timeoutStruct.tv_usec = ((timeout - seconds * 1000) * 1000).convert()
    return setsockopt(__fd, SOL_SOCKET, SO_SNDTIMEO, timeoutStruct.ptr, sizeOf<timeval>().toUInt())
}

public actual fun MemScope.set_recv_socket_timeout(__fd: Int, timeout: Long): Int {
    val timeoutStruct = alloc<timeval>()
    val seconds = timeout / 1000
    timeoutStruct.tv_sec = seconds.convert()
    timeoutStruct.tv_usec = ((timeout - seconds * 1000) * 1000).convert()
    return setsockopt(__fd, SOL_SOCKET, SO_RCVTIMEO, timeoutStruct.ptr, sizeOf<timeval>().toUInt())
}

/**
 * Адрес имени [name] для TCP: IPv4 предпочтительно, IPv6 — если IPv4-адресов у имени нет или это литерал
 * IPv6 (в том числе в скобках, `[::1]`: скобки снимает [hostForResolve]). Семейство полученного адреса —
 * [addressFamilyOf] по длине. Адрес копируется в память [MemScope] (живёт, пока жив скоуп вызывающего), список
 * `addrinfo` сразу освобождается `freeaddrinfo`. `null` — имя не разрешилось.
 */
public actual fun MemScope.getaddrinfo(name: String, service: String?): Pair<CPointer<sockaddr>, UInt>? {
    val hints = alloc<addrinfo>()
    platform.posix.memset(hints.ptr, 0, sizeOf<addrinfo>().convert())
    hints.ai_family = platform.posix.AF_UNSPEC
    hints.ai_socktype = platform.posix.SOCK_STREAM
    hints.ai_protocol = platform.posix.IPPROTO_TCP
    val result = alloc<CPointerVar<addrinfo>>()
    result.value = null
    if (getaddrinfo(hostForResolve(name), service, hints.ptr, result.ptr) != 0) return null
    val head = result.value ?: return null
    try {
        var chosen: addrinfo? = null
        var node: addrinfo? = head.pointed
        while (node != null) {
            val family = node.ai_family
            val usable = node.ai_addr != null && node.ai_addrlen.toInt() > 0 &&
                (family == platform.posix.AF_INET || family == platform.posix.AF_INET6)
            if (usable) {
                if (family == platform.posix.AF_INET) {
                    chosen = node
                    break
                }
                if (chosen == null) chosen = node
            }
            node = node.ai_next?.pointed
        }
        val pick = chosen ?: return null
        val length = pick.ai_addrlen.toInt()
        val copy = allocArray<ByteVar>(length)
        platform.posix.memcpy(copy, pick.ai_addr, length.convert())
        return Pair(copy.reinterpret<sockaddr>(), length.toUInt())
    } finally {
        platform.posix.freeaddrinfo(head)
    }
}

public actual fun fdSet(fd: Int, fdSet: CValuesRef<fd_set>) {
    __darwin_fd_set(fd, fdSet)
}

public actual fun fdZero(fdSet: CValuesRef<fd_set>) {
    memset(fdSet, 0, sizeOf<fd_set>().convert())
}

public actual fun fdIsSet(fd: Int, fdSet: CValuesRef<fd_set>): Int {
    return __darwin_fd_isset(fd, fdSet)
}

public actual fun gettimeofday(timeval: timeval) {
    gettimeofday(timeval.ptr, null)
}

public actual fun MemScope.setIpv6Only(__fd: Int, only: Boolean): Int {
    val value = alloc<IntVar>()
    value.value = if (only) 1 else 0
    return setsockopt(__fd, platform.posix.IPPROTO_IPV6, platform.posix.IPV6_V6ONLY, value.ptr, sizeOf<IntVar>().toUInt())
}
