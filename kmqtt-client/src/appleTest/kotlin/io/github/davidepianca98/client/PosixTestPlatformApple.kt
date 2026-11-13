package io.github.davidepianca98.client

internal actual val posixTestPlatform: String = "apple"

/** `/dev/fd` (fdesc) перечисляет открытые дескрипторы процесса; дескриптор самого перечисления есть в обоих замерах. */
internal actual fun processDescriptorCount(): Int = countDirectoryEntries("/dev/fd")

/** Каталога потоков процесса на Apple нет (`/proc` отсутствует): число потоков не проверяется. */
internal actual fun processThreadCount(): Int = -1
