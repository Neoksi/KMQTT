// size_t/ssize_t/nfds_t разной ширины на 64- и 32-битных целях (watchOS): значения приводятся convert()/toInt()
@file:OptIn(kotlinx.cinterop.UnsafeNumber::class)

package io.github.davidepianca98.client

import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.locks.reentrantLock
import kotlinx.atomicfu.locks.withLock
import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.IntVar
import kotlinx.cinterop.IntVarOf
import kotlinx.cinterop.UIntVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.readBytes
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.set
import kotlinx.cinterop.sizeOf
import kotlinx.cinterop.usePinned
import kotlinx.cinterop.value
import platform.posix.AF_INET
import platform.posix.AF_UNSPEC
import platform.posix.EAGAIN
import platform.posix.EAI_AGAIN
import platform.posix.EAI_NONAME
import platform.posix.EALREADY
import platform.posix.EINPROGRESS
import platform.posix.EINTR
import platform.posix.EWOULDBLOCK
import platform.posix.FD_CLOEXEC
import platform.posix.F_GETFL
import platform.posix.F_SETFD
import platform.posix.F_SETFL
import platform.posix.IPPROTO_TCP
import platform.posix.O_NONBLOCK
import platform.posix.SHUT_WR
import platform.posix.SOCK_STREAM
import platform.posix.SOL_SOCKET
import platform.posix.SO_ERROR
import platform.posix.SO_RCVBUF
import platform.posix.TCP_NODELAY
import platform.posix.addrinfo
import platform.posix.errno
import platform.posix.fcntl
import platform.posix.freeaddrinfo
import platform.posix.getaddrinfo
import platform.posix.getsockname
import platform.posix.getsockopt
import platform.posix.memset
import platform.posix.pipe
import platform.posix.recv
import platform.posix.send
import platform.posix.setsockopt
import platform.posix.shutdown
import platform.posix.sockaddr
import platform.posix.sockaddr_in
import platform.posix.sockaddr_storage
import io.github.davidepianca98.hostForResolve
import io.github.davidepianca98.inet_pton
import platform.posix.socket

/**
 * Тонкий слой сокетов POSIX для хоста Linux и Apple: только то, что нужно [PosixHost] и [PosixTransport]. Перенос
 * `WinSock` (mingwX64): те же функции и соглашения (код ошибки берётся сразу после вызова, потерянный код —
 * «повторить позже»), обёртки `kmqtt-common` (`Posix.kt`) не используются (их `getaddrinfo` теряет список адресов и не
 * освобождает его, `send` без `poll`). Используются только вызовы POSIX (`socket`, `fcntl`, `poll`, `pipe`,
 * `getaddrinfo`); различие платформ одно — защита от `SIGPIPE`: на Linux флаг `MSG_NOSIGNAL` у `send`
 * ([posixSendFlags]), на Apple (там `send` такого флага не обещает) — `SO_NOSIGPIPE` на каждом новом сокете
 * ([suppressSigPipe]).
 */
internal object PosixSockets {
    const val INVALID: Int = -1

    /** Код ошибки потерян (см. [errorOr]); в `kind` — `errno-unknown`. */
    const val UNKNOWN_ERROR: Int = -1

    fun lastError(): Int = errno

    /** Имя кода `errno` для `MqttError.Io.kind` (без адресов). */
    fun errorName(code: Int): String = when (code) {
        platform.posix.EPERM -> "EPERM"
        platform.posix.EINTR -> "EINTR"
        platform.posix.EBADF -> "EBADF"
        platform.posix.EACCES -> "EACCES"
        platform.posix.EINVAL -> "EINVAL"
        platform.posix.EMFILE -> "EMFILE"
        platform.posix.ENFILE -> "ENFILE"
        platform.posix.EPIPE -> "EPIPE"
        platform.posix.EAGAIN -> "EAGAIN"
        platform.posix.EINPROGRESS -> "EINPROGRESS"
        platform.posix.EALREADY -> "EALREADY"
        platform.posix.ENOTSOCK -> "ENOTSOCK"
        platform.posix.EAFNOSUPPORT -> "EAFNOSUPPORT"
        platform.posix.EADDRINUSE -> "EADDRINUSE"
        platform.posix.EADDRNOTAVAIL -> "EADDRNOTAVAIL"
        platform.posix.ENETDOWN -> "ENETDOWN"
        platform.posix.ENETUNREACH -> "ENETUNREACH"
        platform.posix.ENETRESET -> "ENETRESET"
        platform.posix.ECONNABORTED -> "ECONNABORTED"
        platform.posix.ECONNRESET -> "ECONNRESET"
        platform.posix.ENOBUFS -> "ENOBUFS"
        platform.posix.ENOTCONN -> "ENOTCONN"
        platform.posix.ESHUTDOWN -> "ESHUTDOWN"
        platform.posix.ETIMEDOUT -> "ETIMEDOUT"
        platform.posix.ECONNREFUSED -> "ECONNREFUSED"
        platform.posix.EHOSTDOWN -> "EHOSTDOWN"
        platform.posix.EHOSTUNREACH -> "EHOSTUNREACH"
        platform.posix.ENOMEM -> "ENOMEM"
        UNKNOWN_ERROR -> "errno-unknown"
        else -> "errno-$code"
    }

    fun ioError(code: Int): MqttError = MqttError.Io(errorName(code), IllegalStateException("socket error $code"))

    /** Код последней ошибки сразу после неудачного вызова; потерянный код — [UNKNOWN_ERROR]. */
    fun lastErrorOrUnknown(): Int = errorOr(true, UNKNOWN_ERROR)

    /** «Повторить позже»: `EAGAIN`/`EWOULDBLOCK` (на Linux и Apple это один код) и `EINTR`. */
    fun isRetry(code: Int): Boolean = code == EAGAIN || code == EWOULDBLOCK || code == EINTR

    /**
     * Код последней ошибки сразу после вызова. `errno` потока может затереть рантайм Kotlin/Native между возвратом из C
     * и чтением кода (точка безопасности сборщика, освобождение памяти) — тогда он 0, как у `WSAGetLastError` на
     * mingwX64 и в `isTransientSocketError` `kmqtt-common`. Потерянный код — [lost]:
     * вызывающий трактует его как «повторить позже», настоящая ошибка проявится снова — в `SO_ERROR` или при следующем
     * вызове.
     */
    private fun errorOr(failed: Boolean, lost: Int): Int =
        if (failed) errno.let { if (it == 0) lost else it } else 0

    /** `O_NONBLOCK` и `FD_CLOEXEC`: 0 или код ошибки. */
    fun setNonBlocking(fd: Int): Int {
        val flags = fcntl(fd, F_GETFL, 0)
        if (flags < 0) return errorOr(true, UNKNOWN_ERROR)
        if (fcntl(fd, F_SETFL, flags or O_NONBLOCK) < 0) return errorOr(true, UNKNOWN_ERROR)
        fcntl(fd, F_SETFD, FD_CLOEXEC)
        return 0
    }

    fun setIntOption(fd: Int, level: Int, option: Int, value: Int): Int = memScoped {
        val v = alloc<IntVar>()
        v.value = value
        setsockopt(fd, level, option, v.ptr, sizeOf<IntVar>().convert())
    }

    fun noDelay(fd: Int): Int = setIntOption(fd, IPPROTO_TCP, TCP_NODELAY, 1)

    fun receiveBuffer(fd: Int, bytes: Int): Int = setIntOption(fd, SOL_SOCKET, SO_RCVBUF, bytes)

    /** `SO_ERROR` сокета (результат неблокирующего connect); ошибка самого вызова — её код. */
    fun pendingError(fd: Int): Int = memScoped {
        val v = alloc<IntVar>()
        val len = alloc<UIntVar>()
        len.value = sizeOf<IntVar>().convert()
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, v.ptr, len.ptr) != 0) errorOr(true, UNKNOWN_ERROR) else v.value
    }

    fun close(fd: Int, orderly: Boolean) {
        if (fd == INVALID) return
        if (orderly) shutdown(fd, SHUT_WR)
        platform.posix.close(fd)
    }

    /**
     * `send` без `SIGPIPE` (флаги [posixSendFlags] и [suppressSigPipe] сокета: запись в сброшенное соединение — `EPIPE`,
     * а не сигнал, который завершил бы процесс): число отданных байтов (≥ 0) или минус код ошибки. `EINTR` повторяется сразу; потерянный код — `EAGAIN`.
     */
    fun sendSome(fd: Int, bytes: ByteArray, offset: Int, length: Int): Int {
        if (length == 0) return 0
        while (true) {
            val r = bytes.usePinned { pinned ->
                val n = send(fd, pinned.addressOf(offset), length.convert(), posixSendFlags).toInt()
                if (n >= 0) n else -errorOr(true, EAGAIN)
            }
            if (r != -EINTR) return r
        }
    }

    /** `recv`: число байтов (> 0), 0 — конец потока, минус код ошибки (как [sendSome]). */
    fun recvSome(fd: Int, buffer: ByteArray): Int {
        while (true) {
            val r = buffer.usePinned { pinned ->
                val n = recv(fd, pinned.addressOf(0), buffer.size.convert(), 0).toInt()
                if (n >= 0) n else -errorOr(true, EAGAIN)
            }
            if (r != -EINTR) return r
        }
    }

    /**
     * Неблокирующий `connect` на адрес [address]: 0 — подключено сразу, иначе код ошибки (в т. ч. `EINPROGRESS`;
     * потерянный код и `EINTR` — тоже `EINPROGRESS`: подключение продолжается, итог скажут `poll` и `SO_ERROR`).
     */
    fun connectTo(fd: Int, address: Address): Int = memScoped {
        val raw = allocArray<ByteVar>(address.bytes.size)
        address.bytes.forEachIndexed { i, b -> raw[i] = b }
        val error = errorOr(platform.posix.connect(fd, raw.reinterpret(), address.bytes.size.convert()) != 0, EINPROGRESS)
        if (error == EINTR) EINPROGRESS else error
    }

    /** Коды «подключение идёт» неблокирующего `connect`. */
    fun isConnectPending(code: Int): Boolean = code == EINPROGRESS || code == EALREADY || code == EAGAIN

    /**
     * Новый TCP-сокет семейства [family] (неблокирующий, `FD_CLOEXEC`, без `SIGPIPE` — [suppressSigPipe]): сокет или
     * [INVALID] и код ошибки.
     */
    fun openStream(family: Int): Pair<Int, Int> {
        val fd = socket(family, SOCK_STREAM, IPPROTO_TCP)
        if (fd < 0) return INVALID to errorOr(true, UNKNOWN_ERROR)
        val error = suppressSigPipe(fd).takeIf { it != 0 } ?: setNonBlocking(fd)
        if (error != 0) {
            platform.posix.close(fd)
            return INVALID to error
        }
        return fd to 0
    }

    /** Адрес для `connect`: байты `sockaddr` и семейство. */
    class Address(val family: Int, val bytes: ByteArray)

    /** Результат резолва: адрес или код ошибки `getaddrinfo` (`EAI_*`). */
    sealed interface Resolved {
        class Found(val address: Address) : Resolved
        class Error(val code: Int) : Resolved
    }

    /** `EAI_NODATA` (glibc, расширение GNU): в заголовках платформы Kotlin/Native его может не быть. */
    private const val EAI_NODATA_GLIBC: Int = -5

    /** Коды `getaddrinfo`, означающие «такого имени нет» (→ [MqttError.UnknownHost]). */
    fun isUnknownHost(code: Int): Boolean = code == EAI_NONAME || code == EAI_AGAIN || code == EAI_NODATA_GLIBC

    /**
     * Блокирующий `getaddrinfo` (только вне потока хоста): первый IPv4, иначе первый адрес (как `NioTransport` и
     * `WinSock`); список освобождается `freeaddrinfo`.
     */
    fun resolve(host: String, port: Int): Resolved = memScoped {
        val hints = alloc<addrinfo>()
        memset(hints.ptr, 0, sizeOf<addrinfo>().convert())
        hints.ai_family = AF_UNSPEC
        hints.ai_socktype = SOCK_STREAM
        hints.ai_protocol = IPPROTO_TCP
        val result = alloc<CPointerVar<addrinfo>>()
        result.value = null
        // Литерал IPv6 в скобках ([::1], как в URL) getaddrinfo не принимает — скобки снимаются
        val rc = getaddrinfo(hostForResolve(host), port.toString(), hints.ptr, result.ptr)
        if (rc != 0) return@memScoped Resolved.Error(rc)
        val head: CPointer<addrinfo> = result.value ?: return@memScoped Resolved.Error(EAI_NONAME)
        try {
            var chosen: addrinfo? = null
            var first: addrinfo? = null
            var node: addrinfo? = head.pointed
            while (node != null) {
                if (node.ai_addr != null) {
                    if (first == null) first = node
                    if (node.ai_family == AF_INET) {
                        chosen = node
                        break
                    }
                }
                node = node.ai_next?.pointed
            }
            val pick = chosen ?: first ?: return@memScoped Resolved.Error(EAI_NONAME)
            val bytes = pick.ai_addr!!.reinterpret<ByteVar>().readBytes(pick.ai_addrlen.toInt())
            Resolved.Found(Address(pick.ai_family, bytes))
        } finally {
            freeaddrinfo(head)
        }
    }

    /** Заполнить `sockaddr_in` 127.0.0.1:[portNetworkOrder] (порт — в сетевом порядке байтов). */
    fun loopback(addr: sockaddr_in, portNetworkOrder: UShort) {
        memset(addr.ptr, 0, sizeOf<sockaddr_in>().convert())
        addr.sin_family = AF_INET.convert()
        addr.sin_port = portNetworkOrder
        inet_pton(AF_INET, "127.0.0.1", addr.sin_addr.ptr)
    }

    /** Локальный порт сокета IPv4 или IPv6 (порт `sockaddr_in`/`sockaddr_in6` — по смещению 2, сетевой порядок). */
    fun portOf(fd: Int): Int = memScoped {
        val addr = alloc<sockaddr_storage>()
        val len = alloc<UIntVar>()
        len.value = sizeOf<sockaddr_storage>().convert()
        getsockname(fd, addr.ptr.reinterpret<sockaddr>(), len.ptr)
        val bytes = addr.ptr.reinterpret<ByteVar>()
        ((bytes[2].toInt() and 0xFF) shl 8) or (bytes[3].toInt() and 0xFF)
    }
}

/**
 * Пробуждение цикла хоста: неблокирующий `pipe`. [signal] пишет один байт (переполненный канал — не беда: цикл и так
 * проснётся), цикл ждёт его в `poll` вместе с сокетами транспортов. [signal] после [close] ничего не делает: замок не
 * даёт записать в закрытый (и, возможно, уже переиспользованный) дескриптор, а запись в канал без читателя дала бы
 * `SIGPIPE`.
 */
internal class WakePipe {
    private val lock = reentrantLock()
    private var readFd: Int = PosixSockets.INVALID
    private var writeFd: Int = PosixSockets.INVALID
    private val pendingSignal = atomic(false)
    private val one = ByteArray(1)
    private val drainBuffer = ByteArray(64)

    /** Читающий конец для `poll`. */
    val fd: Int get() = readFd

    init {
        memScoped {
            val fds = allocArray<IntVarOf<Int>>(2)
            check(pipe(fds) == 0) { "wake pipe: ${PosixSockets.errorName(PosixSockets.lastError())}" }
            val r = fds[0]
            val w = fds[1]
            if (PosixSockets.setNonBlocking(r) != 0 || PosixSockets.setNonBlocking(w) != 0) {
                platform.posix.close(r)
                platform.posix.close(w)
                error("wake pipe O_NONBLOCK")
            }
            readFd = r
            writeFd = w
        }
    }

    fun signal() {
        if (!pendingSignal.compareAndSet(false, true)) return
        lock.withLock {
            val w = writeFd
            if (w != PosixSockets.INVALID) {
                one.usePinned { platform.posix.write(w, it.addressOf(0), 1.convert()) }
            }
        }
    }

    /** Вычерпать пробуждения; вызывать до `runOnce` (сигнал после сброса флага — новый байт и новый оборот). */
    fun drain() {
        val r = readFd
        if (r == PosixSockets.INVALID) return
        drainBuffer.usePinned { pinned ->
            // ssize_t: Long на 64-битных целях, Int на 32-битных (watchOS)
            var n: Long
            do {
                n = platform.posix.read(r, pinned.addressOf(0), drainBuffer.size.convert()).toLong()
            } while (n > 0)
        }
        pendingSignal.value = false
    }

    fun close() {
        lock.withLock {
            val r = readFd
            val w = writeFd
            readFd = PosixSockets.INVALID
            writeFd = PosixSockets.INVALID
            if (w != PosixSockets.INVALID) platform.posix.close(w)
            if (r != PosixSockets.INVALID) platform.posix.close(r)
        }
    }
}

/**
 * Флаги `send` против `SIGPIPE`: на Linux — `MSG_NOSIGNAL`, на Apple — 0 (там `SIGPIPE` подавлен на сокете,
 * [suppressSigPipe]).
 */
internal expect val posixSendFlags: Int

/**
 * Настроить новый сокет [fd] против `SIGPIPE`: на Apple — `SO_NOSIGPIPE`, на Linux ничего не делает (там флаг `send`,
 * [posixSendFlags]). 0 или код ошибки.
 */
internal expect fun suppressSigPipe(fd: Int): Int
