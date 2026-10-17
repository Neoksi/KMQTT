package io.github.davidepianca98.client.node

// SEVEN-250: глобальные таймеры Node, нужные хосту (без обёрток kotlin-wrappers: им достаточно dynamic)

internal external fun setImmediate(callback: () -> Unit): dynamic

internal external fun setTimeout(callback: () -> Unit, ms: Int): dynamic

internal external fun clearTimeout(handle: dynamic)

/** Массив вызывающего как `Buffer` без копирования (общая память с [bytes]). */
internal fun bufferOf(bytes: ByteArray): dynamic {
    val array: dynamic = bytes
    return js("Buffer").from(array.buffer, array.byteOffset, bytes.size)
}

/** Копия байтов `Buffer`/`Uint8Array` в новый [ByteArray] (событие [io.github.davidepianca98.client.TransportEvent.Bytes] владеет массивом). */
internal fun bytesOf(buffer: dynamic): ByteArray {
    val b = buffer
    return js("new Int8Array(b.buffer, b.byteOffset, b.length).slice()").unsafeCast<ByteArray>()
}

/** Пустой объект JS для опций. */
internal fun jsObject(): dynamic = js("({})")
