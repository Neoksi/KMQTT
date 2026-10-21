package io.github.davidepianca98.client

internal actual val posixTestPlatform: String = "linux"

internal actual fun processDescriptorCount(): Int = countDirectoryEntries("/proc/self/fd")

internal actual fun processThreadCount(): Int = countDirectoryEntries("/proc/self/task")
