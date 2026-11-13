package io.github.davidepianca98.socket.tls

import kotlinx.cinterop.addressOf
import kotlinx.cinterop.usePinned
import io.github.davidepianca98.socket.IOException
import io.github.davidepianca98.socket.PendingSendQueue
import io.github.davidepianca98.socket.SocketClosedException
import io.github.davidepianca98.socket.streams.ByteArrayOutputStream
import io.github.davidepianca98.socket.tcp.Socket

public actual open class TLSSocket(
    socket: Int,
    protected val engine: TLSEngine,
    writeRequest: MutableList<Int>?,
    buffer: ByteArray
) : Socket(socket, writeRequest, buffer) {

    private val buf = ByteArray(16 * 1024)
    private val sendBuf = ByteArray(16 * 1024)

    /**
     * Открытые данные, которые TLS-движок ещё не принял (рукопожатие не закончено, нужно сначала прочитать).
     * Порядок сохраняется: новые данные уходят только за накопленными.
     */
    private val pendingSend = PendingSendQueue()

    /**
     * Открытые данные, расшифрованные во время отправки и ещё не отданные вызывающему.
     *
     * [send0] при `WANT_READ` движка читает сокет (TLS-движку нужны данные от собеседника: конец рукопожатия,
     * пересогласование), и в том же `recv` могут прийти прикладные записи собеседника. Раньше расшифрованное
     * отбрасывалось; теперь оно копится здесь (куски в порядке прихода) и отдаётся следующим [read] первым, перед тем,
     * что этот [read] прочитает сам. Порядок потока сохраняется: всё, что здесь лежит, пришло раньше.
     */
    private val receivedDuringSend = ArrayDeque<UByteArray>()

    /** Глубина вложенности [send0]: вложенный [read] из `WANT_READ` отдаёт только новое, накопленное не трогает. */
    private var sendDepth = 0

    /**
     * Есть открытые данные, уже полученные и расшифрованные, но не отданные [read]: [read] вернёт их, даже
     * если сокет сейчас не готов к чтению. Тот, кто ждёт готовности сокета (`select`/`poll`) перед [read], должен
     * проверять этот признак, иначе данные пролежат до следующего пакета собеседника.
     */
    public val hasBufferedData: Boolean
        get() = sendDepth == 0 && receivedDuringSend.isNotEmpty()

    private fun socketSend() {
        sendBuf.usePinned { pinnedSendBuf ->
            do {
                val number = engine.bioRead(pinnedSendBuf.addressOf(0), sendBuf.size)
                if (number > 0) {
                    super.send(pinnedSendBuf.get().toUByteArray().copyOfRange(0, number))
                } else if (engine.bioShouldRetry) {
                    close()
                    throw IOException("OpenSSL shouldn't retry to read BIO")
                }
            } while (number > 0)
        }
    }

    override fun send(data: UByteArray) {
        // Движок мог принять часть данных: в очереди остаётся только остаток, а не весь кусок целиком (без дублей)
        pendingSend.submit(data, ::send0)
    }

    /**
     * Одна попытка записи в TLS-движок: число принятых байтов открытых данных (`0` — сейчас не принимает).
     *
     * При `WANT_READ` читает сокет виртуальным [read] (подкласс может подождать готовности, как `TLSClientSocket`);
     * если пришли прикладные данные, они не теряются, а ложатся в [receivedDuringSend]. Если читать нечего
     * (`null`), попытка заканчивается: остаток ждёт в [pendingSend].
     */
    private fun send0(data: UByteArray): Int {
        sendDepth++
        try {
            return writeToEngine(data)
        } finally {
            sendDepth--
        }
    }

    private fun writeToEngine(data: UByteArray): Int {
        var length = data.size
        var index = 0
        data.toByteArray().usePinned { pinnedBuf ->
            while (length > 0) {
                val result = engine.write(pinnedBuf.addressOf(index), length)
                socketSend()
                if (result > 0) {
                    length -= result
                    index += result
                } else {
                    when (val status = engine.getError(result)) {
                        0 -> { // OK
                            break
                        }
                        2 -> { // WANT_READ
                            val received = read() ?: break
                            if (received.isNotEmpty()) receivedDuringSend.addLast(received)
                        }
                        3 -> { // WANT_WRITE
                            throw IOException("OpenSSL want write in write")
                        }
                        6 -> { // ZERO_RETURN
                            close()
                            throw SocketClosedException()
                        }
                        else -> {
                            close()
                            throw IOException("OpenSSL error $status")
                        }
                    }
                }
            }
        }
        return index
    }

    /**
     * Открытые данные: сначала накопленные во время отправки ([receivedDuringSend]), затем то, что пришло сейчас.
     * `null` — нового в сокете нет и накопленного тоже; пустой массив — пришли только служебные записи TLS.
     * Вложенный вызов из [send0] отдаёт только новое (накопленное остаётся на месте, порядок не меняется).
     */
    override fun read(): UByteArray? {
        if (engine.isInitFinished) {
            pendingSend.drain(::send0)
        }

        val fresh = receive()
        if (sendDepth > 0 || receivedDuringSend.isEmpty()) return fresh

        var total = fresh?.size ?: 0
        receivedDuringSend.forEach { total += it.size }
        val result = UByteArray(total)
        var offset = 0
        while (receivedDuringSend.isNotEmpty()) {
            val chunk = receivedDuringSend.removeFirst()
            chunk.copyInto(result, offset)
            offset += chunk.size
        }
        fresh?.copyInto(result, offset)
        return result
    }

    /** Прочитать сокет и расшифровать: открытые данные, пустой массив (только служебное TLS) или `null` (нечего). */
    private fun receive(): UByteArray? {
        val data = super.read() ?: return null

        val returnData = ByteArrayOutputStream()

        var length = data.size
        var index = 0
        data.toByteArray().usePinned { pinned ->
            while (length > 0) {
                val n = engine.bioWrite(pinned.addressOf(index), length)
                if (n <= 0) {
                    close()
                    throw IOException("Failed in BIO_write")
                }

                index += n
                length -= n

                buf.usePinned { pinnedBuf ->
                    var number: Int
                    do {
                        number = engine.read(pinnedBuf.addressOf(0), buf.size)
                        socketSend()
                        if (number > 0) {
                            returnData.write(pinnedBuf.get().toUByteArray(), 0, number)
                        } else {
                            when (val status = engine.getError(number)) {
                                0 -> {} // OK
                                2 -> {} // WANT_READ
                                3 -> { // WANT_WRITE
                                    throw IOException("OpenSSL want write in read")
                                }
                                6 -> { // ZERO_RETURN
                                    close()
                                    throw SocketClosedException()
                                }
                                else -> {
                                    close()
                                    throw IOException("OpenSSL error $status")
                                }
                            }
                        }
                    } while (number > 0)
                }
            }
        }
        return returnData.toByteArray()
    }

    override fun close() {
        engine.close()
        super.close()
    }
}