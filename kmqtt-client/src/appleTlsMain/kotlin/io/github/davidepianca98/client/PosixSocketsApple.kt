package io.github.davidepianca98.client

import platform.posix.SOL_SOCKET
import platform.posix.SO_NOSIGPIPE

/** Apple: флагов `send` против `SIGPIPE` нет — сигнал подавлен на самом сокете ([suppressSigPipe]). */
internal actual val posixSendFlags: Int = 0

/**
 * Apple: `SO_NOSIGPIPE` на сокете — запись в сброшенное соединение даёт `EPIPE`, а не сигнал `SIGPIPE`, который завершил
 * бы процесс. Ставится на каждый новый сокет сразу после `socket()`. 0 или код ошибки.
 */
internal actual fun suppressSigPipe(fd: Int): Int =
    if (PosixSockets.setIntOption(fd, SOL_SOCKET, SO_NOSIGPIPE, 1) == 0) 0 else PosixSockets.lastErrorOrUnknown()
