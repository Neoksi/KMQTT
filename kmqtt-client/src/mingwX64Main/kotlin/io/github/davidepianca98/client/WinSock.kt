package io.github.davidepianca98.client

import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.locks.reentrantLock
import kotlinx.atomicfu.locks.withLock
import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.IntVar
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
import platform.posix.FIONBIO
import platform.posix.IPPROTO_TCP
import platform.posix.IPPROTO_UDP
import platform.posix.SOCK_DGRAM
import platform.posix.SOCK_STREAM
import platform.posix.SOL_SOCKET
import platform.posix.SO_ERROR
import platform.posix.SO_RCVBUF
import platform.posix.SD_SEND
import platform.posix.TCP_NODELAY
import platform.posix.WSADATA
import platform.posix.WSAStartup
import platform.windows.addrinfo
import platform.posix.closesocket
import platform.posix.fd_set
import platform.windows.freeaddrinfo
import platform.windows.getaddrinfo
import platform.posix.getsockname
import platform.posix.getsockopt
import platform.posix.ioctlsocket
import platform.posix.recv
import platform.posix.send
import platform.posix.setsockopt
import platform.posix.shutdown
import platform.posix.sockaddr
import platform.posix.sockaddr_in
import platform.posix.socket
import platform.windows.WSAGetLastError
import platform.windows.inet_pton

/**
 * Тонкий слой WinSock для хоста mingwX64 (SEVEN-250): только то, что нужно [WinSockHost] и [WinSockTransport].
 * Обёртки `kmqtt-common` (`Posix.kt`) не используются: у них `setsockopt` передаёт `optval.toString()` вместо
 * указателя, `select` строит `timeval` только из микросекунд, `getaddrinfo` теряет список адресов (без `freeaddrinfo`)
 * и знает только IPv4 (находки SEVEN-250, отчёт).
 */
internal object WinSock {
    const val INVALID: ULong = ULong.MAX_VALUE
    const val SOCKET_ERROR: Int = -1
    const val WSAEWOULDBLOCK: Int = 10035
    const val WSAEINPROGRESS: Int = 10036
    const val WSAEALREADY: Int = 10037
    const val WSAHOST_NOT_FOUND: Int = 11001
    const val WSATRY_AGAIN: Int = 11002
    const val WSANO_DATA: Int = 11004
    const val FD_SETSIZE: Int = 64

    /** Код ошибки потерян (см. [errorOr]); в `kind` — `WSA-unknown`. */
    const val UNKNOWN_ERROR: Int = -1

    private val started = atomic(false)
    private val startLock = reentrantLock()

    /** `WSAStartup(2.2)` один раз на процесс (счётчик WinSock не уменьшается: сокеты нужны до выхода). */
    fun ensureStarted() {
        if (started.value) return
        startLock.withLock {
            if (started.value) return
            memScoped {
                val data = alloc<WSADATA>()
                val rc = WSAStartup(0x0202u, data.ptr)
                check(rc == 0) { "WSAStartup failed: $rc" }
            }
            started.value = true
        }
    }

    fun lastError(): Int = WSAGetLastError()

    /** Имя кода ошибки WinSock для `MqttError.Io.kind` (без адресов). */
    fun errorName(code: Int): String = when (code) {
        10004 -> "WSAEINTR"
        10013 -> "WSAEACCES"
        10022 -> "WSAEINVAL"
        10024 -> "WSAEMFILE"
        10035 -> "WSAEWOULDBLOCK"
        10038 -> "WSAENOTSOCK"
        10047 -> "WSAEAFNOSUPPORT"
        10048 -> "WSAEADDRINUSE"
        10049 -> "WSAEADDRNOTAVAIL"
        10050 -> "WSAENETDOWN"
        10051 -> "WSAENETUNREACH"
        10052 -> "WSAENETRESET"
        10053 -> "WSAECONNABORTED"
        10054 -> "WSAECONNRESET"
        10055 -> "WSAENOBUFS"
        10057 -> "WSAENOTCONN"
        10058 -> "WSAESHUTDOWN"
        10060 -> "WSAETIMEDOUT"
        10061 -> "WSAECONNREFUSED"
        10064 -> "WSAEHOSTDOWN"
        10065 -> "WSAEHOSTUNREACH"
        10093 -> "WSANOTINITIALISED"
        UNKNOWN_ERROR -> "WSA-unknown"
        else -> "WSA$code"
    }

    fun ioError(code: Int): MqttError = MqttError.Io(errorName(code), IllegalStateException("WinSock error $code"))

    /**
     * Код последней ошибки сразу после вызова. `WSAGetLastError` живёт до следующего вызова Win32 в этом потоке, а
     * Kotlin/Native между возвратом из C и следующим вызовом может пройти точку безопасности сборщика (там свои вызовы
     * Win32), и код теряется — `WSAGetLastError()` даёт 0 (замечено на ≈ 87-м цикле подряд, SEVEN-250). Потерянный код —
     * [lost]: вызывающий трактует его как «повторить позже» (`WSAEWOULDBLOCK`), настоящая ошибка проявится снова — с
     * кодом в `SO_ERROR` или при следующем вызове.
     */
    private inline fun errorOr(failed: Boolean, lost: Int): Int =
        if (failed) WSAGetLastError().let { if (it == 0) lost else it } else 0

    /** `FIONBIO`: 0 или код ошибки WinSock. */
    fun setNonBlocking(s: ULong): Int = memScoped {
        val on = alloc<UIntVar>()
        on.value = 1u
        errorOr(ioctlsocket(s, FIONBIO.toInt(), on.ptr.reinterpret()) != 0, UNKNOWN_ERROR)
    }

    fun setIntOption(s: ULong, level: Int, option: Int, value: Int): Int = memScoped {
        val v = alloc<IntVar>()
        v.value = value
        setsockopt(s, level, option, v.ptr.reinterpret<ByteVar>(), sizeOf<IntVar>().toInt())
    }

    fun noDelay(s: ULong): Int = setIntOption(s, IPPROTO_TCP, TCP_NODELAY, 1)

    fun receiveBuffer(s: ULong, bytes: Int): Int = setIntOption(s, SOL_SOCKET, SO_RCVBUF, bytes)

    /** `SO_ERROR` сокета (результат неблокирующего connect); ошибка самого вызова — код WinSock. */
    fun pendingError(s: ULong): Int = memScoped {
        val v = alloc<IntVar>()
        val len = alloc<IntVar>()
        len.value = sizeOf<IntVar>().toInt()
        if (getsockopt(s, SOL_SOCKET, SO_ERROR, v.ptr.reinterpret<ByteVar>(), len.ptr) != 0) lastError() else v.value
    }

    fun close(s: ULong, orderly: Boolean) {
        if (s == INVALID) return
        if (orderly) shutdown(s, SD_SEND)
        closesocket(s)
    }

    /**
     * `send`: число отданных байтов (≥ 0) или минус код ошибки WinSock. Код берётся сразу после вызова: позже его может
     * затереть рантайм Kotlin/Native (освобождение памяти, сборщик) — так `WSAEWOULDBLOCK` превращался в 0 (SEVEN-250).
     */
    fun sendSome(s: ULong, bytes: ByteArray, offset: Int, length: Int): Int {
        if (length == 0) return 0
        return bytes.usePinned { pinned ->
            val n = send(s, pinned.addressOf(offset), length, 0)
            if (n < 0) -errorOr(true, WSAEWOULDBLOCK) else n
        }
    }

    /** `recv`: число байтов (> 0), 0 — конец потока, минус код ошибки WinSock (как [sendSome]). */
    fun recvSome(s: ULong, buffer: ByteArray): Int =
        buffer.usePinned { pinned ->
            val n = recv(s, pinned.addressOf(0), buffer.size, 0)
            if (n < 0) -errorOr(true, WSAEWOULDBLOCK) else n
        }

    /**
     * Неблокирующий `connect` на адрес [address]: 0 — подключено сразу, иначе код ошибки WinSock (в т. ч. WOULDBLOCK;
     * потерянный код — тоже WOULDBLOCK: итог подключения скажут `select` и `SO_ERROR`).
     */
    fun connectTo(s: ULong, address: Address): Int = memScoped {
        val raw = allocArray<ByteVar>(address.bytes.size)
        address.bytes.forEachIndexed { i, b -> raw[i] = b }
        errorOr(platform.posix.connect(s, raw.reinterpret(), address.bytes.size) != 0, WSAEWOULDBLOCK)
    }

    /** Новый TCP-сокет семейства [family]: сокет или [INVALID] и код ошибки. */
    fun openStream(family: Int): Pair<ULong, Int> {
        val s = socket(family, SOCK_STREAM, IPPROTO_TCP)
        return if (s == INVALID) INVALID to errorOr(true, UNKNOWN_ERROR) else s to 0
    }

    /** Адрес для `connect`: байты `sockaddr` и семейство. */
    class Address(val family: Int, val bytes: ByteArray)

    /** Результат резолва: адрес или код ошибки `getaddrinfo`. */
    sealed interface Resolved {
        class Found(val address: Address) : Resolved
        class Error(val code: Int) : Resolved
    }

    /**
     * Блокирующий `getaddrinfo` (только вне потока хоста): первый IPv4, иначе первый адрес (как `NioTransport`);
     * список освобождается `freeaddrinfo`.
     */
    fun resolve(host: String, port: Int): Resolved = memScoped {
        ensureStarted()
        val hints = alloc<addrinfo>()
        hints.ai_family = AF_UNSPEC
        hints.ai_socktype = SOCK_STREAM
        hints.ai_protocol = IPPROTO_TCP
        val result = alloc<CPointerVar<addrinfo>>()
        val rc = getaddrinfo(host, port.toString(), hints.ptr, result.ptr)
        if (rc != 0) return@memScoped Resolved.Error(rc)
        val head: CPointer<addrinfo> = result.value ?: return@memScoped Resolved.Error(WSANO_DATA)
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
            val pick = chosen ?: first ?: return@memScoped Resolved.Error(WSANO_DATA)
            val bytes = pick.ai_addr!!.reinterpret<ByteVar>().readBytes(pick.ai_addrlen.toInt())
            Resolved.Found(Address(pick.ai_family, bytes))
        } finally {
            freeaddrinfo(head)
        }
    }

    /** Добавить сокет в `fd_set` WinSock (массив сокетов, не битовая маска). */
    fun fdAdd(set: fd_set, s: ULong) {
        val count = set.fd_count.toInt()
        for (i in 0 until count) if (set.fd_array[i] == s) return
        if (count >= FD_SETSIZE) return
        set.fd_array[count] = s
        set.fd_count = (count + 1).convert()
    }

    fun fdHas(set: fd_set, s: ULong): Boolean {
        val count = set.fd_count.toInt()
        for (i in 0 until count) if (set.fd_array[i] == s) return true
        return false
    }

    fun fdClear(set: fd_set) {
        set.fd_count = 0u
    }

    /** Заполнить `sockaddr_in` 127.0.0.1:[port] (порт — в сетевом порядке байтов). */
    fun loopback(addr: sockaddr_in, portNetworkOrder: UShort) {
        addr.sin_family = AF_INET.convert()
        addr.sin_port = portNetworkOrder
        inet_pton(AF_INET, "127.0.0.1", addr.sin_addr.ptr)
    }

    fun portOf(s: ULong): Int = memScoped {
        val addr = alloc<sockaddr_in>()
        val len = alloc<IntVar>()
        len.value = sizeOf<sockaddr_in>().toInt()
        getsockname(s, addr.ptr.reinterpret<sockaddr>(), len.ptr)
        val p = addr.sin_port.toInt()
        ((p and 0xFF) shl 8) or ((p shr 8) and 0xFF)
    }
}

/**
 * Пробуждение цикла хоста: UDP-сокет на 127.0.0.1, соединённый сам с собой. [signal] шлёт один байт (неблокирующе,
 * переполненный буфер — не беда: цикл и так проснётся), цикл ждёт его в `select` вместе с сокетами транспортов.
 * [signal] после [close] ничего не делает (замок не даёт отправить в уже закрытый и, возможно, переиспользованный
 * дескриптор).
 */
internal class WakeSocket {
    private val lock = reentrantLock()
    private var fd: ULong = WinSock.INVALID
    private val pendingSignal = atomic(false)
    private val one = ByteArray(1)
    private val drainBuffer = ByteArray(64)

    val socket: ULong get() = fd

    init {
        WinSock.ensureStarted()
        val s = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)
        check(s != WinSock.INVALID) { "wake socket: ${WinSock.errorName(WinSock.lastError())}" }
        try {
            memScoped {
                val addr = alloc<sockaddr_in>()
                WinSock.loopback(addr, 0u)
                check(platform.posix.bind(s, addr.ptr.reinterpret(), sizeOf<sockaddr_in>().toInt()) == 0) {
                    "wake socket bind: ${WinSock.errorName(WinSock.lastError())}"
                }
                val bound = alloc<sockaddr_in>()
                val len = alloc<IntVar>()
                len.value = sizeOf<sockaddr_in>().toInt()
                check(getsockname(s, bound.ptr.reinterpret(), len.ptr) == 0) { "wake socket getsockname" }
                check(platform.posix.connect(s, bound.ptr.reinterpret(), len.value) == 0) {
                    "wake socket connect: ${WinSock.errorName(WinSock.lastError())}"
                }
            }
            check(WinSock.setNonBlocking(s) == 0) { "wake socket FIONBIO" }
        } catch (e: Throwable) {
            closesocket(s)
            throw e
        }
        fd = s
    }

    fun signal() {
        if (!pendingSignal.compareAndSet(false, true)) return
        lock.withLock {
            val s = fd
            if (s != WinSock.INVALID) WinSock.sendSome(s, one, 0, 1)
        }
    }

    /** Вычерпать пробуждения; вызывать до `runOnce` (сигнал после сброса флага — новый байт и новый оборот). */
    fun drain() {
        val s = fd
        if (s == WinSock.INVALID) return
        while (WinSock.recvSome(s, drainBuffer) > 0) Unit
        pendingSignal.value = false
    }

    fun close() {
        lock.withLock {
            val s = fd
            fd = WinSock.INVALID
            if (s != WinSock.INVALID) closesocket(s)
        }
    }
}
