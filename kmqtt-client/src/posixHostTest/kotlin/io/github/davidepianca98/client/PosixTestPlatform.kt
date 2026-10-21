package io.github.davidepianca98.client

import kotlinx.cinterop.pointed
import kotlinx.cinterop.toKString
import platform.posix.closedir
import platform.posix.opendir
import platform.posix.readdir

/** Имя платформы в сообщениях тестов хоста POSIX (`linux`, `apple`). */
internal expect val posixTestPlatform: String

/** Число открытых дескрипторов процесса или -1, если платформа его не показывает. */
internal expect fun processDescriptorCount(): Int

/** Число потоков процесса или -1, если платформа его не показывает. */
internal expect fun processThreadCount(): Int

/** Число записей каталога [path] без `.` и `..`; -1 — каталога нет. */
internal fun countDirectoryEntries(path: String): Int {
    val dir = opendir(path) ?: return -1
    var count = 0
    try {
        while (true) {
            val entry = readdir(dir) ?: break
            val name = entry.pointed.d_name.toKString()
            if (name != "." && name != "..") count++
        }
    } finally {
        closedir(dir)
    }
    return count
}
