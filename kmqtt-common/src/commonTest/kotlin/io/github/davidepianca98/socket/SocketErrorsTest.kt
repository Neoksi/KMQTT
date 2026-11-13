package io.github.davidepianca98.socket

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SocketErrorsTest {

    @Test
    fun wouldBlockCodesAreTransient() {
        assertTrue(isTransientSocketError(11, eagain = 11, ewouldblock = 11))
        assertTrue(isTransientSocketError(10035, eagain = 10035, ewouldblock = 10035))
        assertTrue(isTransientSocketError(35, eagain = 11, ewouldblock = 35))
    }

    @Test
    fun lostErrorCodeIsTransientNotFatal() {
        assertTrue(isTransientSocketError(0, eagain = 10035, ewouldblock = 10035))
    }

    @Test
    fun realErrorsAreNotTransient() {
        assertFalse(isTransientSocketError(10054, eagain = 10035, ewouldblock = 10035)) // WSAECONNRESET
        assertFalse(isTransientSocketError(32, eagain = 11, ewouldblock = 11)) // EPIPE
    }
}
