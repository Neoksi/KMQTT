package io.github.davidepianca98.socket

/**
 * Ошибка неблокирующего вызова сокета, после которой надо просто повторить позже.
 *
 * Помимо `EAGAIN`/`EWOULDBLOCK` сюда входит код `0`: на Kotlin/Native `WSAGetLastError()` (и `errno`) может вернуть `0`
 * после неудачного вызова — между возвратом из C и чтением кода рантайм успевает пройти точку безопасности сборщика
 * мусора, и код затирается. Принимать такой отказ за настоящую ошибку и закрывать соединение нельзя; настоящая
 * ошибка сохраняется у сокета и проявится на следующем вызове.
 */
internal fun isTransientSocketError(error: Int, eagain: Int, ewouldblock: Int): Boolean =
    error == 0 || error == eagain || error == ewouldblock
