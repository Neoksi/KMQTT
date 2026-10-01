package io.github.davidepianca98.socket

public interface SocketInterface {

    public fun send(data: UByteArray)

    public fun sendRemaining()

    public fun read(): UByteArray?

    public fun close()

    /**
     * Ends a read() waiting for data now (it returns null), so that the thread running the client loop can send
     * what has been queued meanwhile. Safe to call from any thread, also on a closed socket; if no read() is
     * waiting, the next one returns at once. Implementations without a blocking wait do nothing.
     */
    public fun wakeup() {}
}
