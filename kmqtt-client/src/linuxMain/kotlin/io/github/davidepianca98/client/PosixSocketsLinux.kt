package io.github.davidepianca98.client

import platform.posix.MSG_NOSIGNAL

/** Linux: `send` с `MSG_NOSIGNAL` — запись в сброшенное соединение даёт `EPIPE`, а не сигнал `SIGPIPE`. */
internal actual val posixSendFlags: Int = MSG_NOSIGNAL

/** Linux: защита от `SIGPIPE` — флаг `send` ([posixSendFlags]), настраивать сокет не нужно. */
internal actual fun suppressSigPipe(fd: Int): Int = 0
