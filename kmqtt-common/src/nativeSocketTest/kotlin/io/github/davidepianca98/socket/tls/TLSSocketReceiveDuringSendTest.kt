package io.github.davidepianca98.socket.tls

import io.github.davidepianca98.socket.TestNet
import io.github.davidepianca98.socket.TestSocketPair
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.TimeSource

/**
 * `TLSSocket.send0` при `WANT_READ` движка читает сокет, и прикладные данные собеседника, пришедшие в том
 * же `recv`, раньше терялись. Теперь они копятся в сокете и отдаются следующим `read()` первыми, в порядке прихода.
 *
 * Сценарий на настоящей петле TCP и OpenSSL (сервер ограничен TLS 1.2): клиент шлёт `A` до рукопожатия (оно
 * начинается, `A` ждёт в очереди); стороны обмениваются рукопожатием до `Finished` клиента; сервер, уже закончивший
 * рукопожатие, шлёт `B1`, `B2`; клиент шлёт `C` — очередь отправки досылает `A`, `SSL_write` получает `WANT_READ`
 * (клиент ждёт `Finished` сервера), чтение приносит `Finished` и зашифрованные `B1`, `B2`. Следующий `read()` клиента
 * обязан вернуть `B1B2`, затем приходит `B3`; сервер получает `A`, затем `C`.
 */
class TLSSocketReceiveDuringSendTest {

    private fun bytes(text: String): UByteArray = text.encodeToByteArray().toUByteArray()

    /** Читать [socket], пока [done] не станет истиной или не выйдет срок; прочитанное — в [sink]. */
    private fun pump(socket: TLSSocket, sink: MutableList<UByte>, timeoutMs: Long = 5000, done: () -> Boolean) {
        val started = TimeSource.Monotonic.markNow()
        while (!done()) {
            socket.read()?.let { sink.addAll(it) }
            if (done()) return
            check(started.elapsedNow().inWholeMilliseconds < timeoutMs) { "timeout" }
            TestNet.sleepMs(5)
        }
    }

    @Test
    fun applicationDataDecryptedDuringSendIsReturnedByNextReadInOrder() {
        val pair = TestSocketPair.open()
        val clientEngine = ObservedTlsEngine(TestTlsEngine(server = false))
        val serverEngine = TestTlsEngine(server = true, maxTls12 = true)
        val client = TLSSocket(pair.client, clientEngine, null, ByteArray(32 * 1024))
        val server = TLSSocket(pair.server, serverEngine, null, ByteArray(32 * 1024))
        val atClient = mutableListOf<UByte>()
        val atServer = mutableListOf<UByte>()
        try {
            val a = bytes("A-before-handshake;")
            val b1 = bytes("B1-server-data;")
            val b2 = bytes("B2-server-data;")
            val b3 = bytes("B3-after;")
            val c = bytes("C-after-handshake;")

            // A: ClientHello уходит, SSL_write просит чтения, читать пока нечего — A остаётся в очереди
            client.send(a)

            // Рукопожатие до Finished клиента (сервер закончил, клиент ещё ждёт Finished сервера)
            val started = TimeSource.Monotonic.markNow()
            while (!serverEngine.isInitFinished) {
                client.read()?.let { atClient.addAll(it) }
                server.read()?.let { atServer.addAll(it) }
                check(started.elapsedNow().inWholeMilliseconds < 5000) { "handshake timeout" }
                if (!serverEngine.isInitFinished) TestNet.sleepMs(5)
            }
            assertFalse(clientEngine.isInitFinished, "клиент не должен успеть получить Finished сервера")
            assertTrue(atClient.isEmpty() && atServer.isEmpty(), "до рукопожатия прикладных данных нет")

            // Сервер шлёт B1, B2; ждём, пока они дойдут по петле
            server.send(b1)
            server.send(b2)
            TestNet.sleepMs(200)

            // C: очередь досылает A, SSL_write → WANT_READ, чтение приносит Finished сервера и B1, B2
            val wantReadBefore = clientEngine.writeWantRead
            client.send(c)
            assertTrue(clientEngine.writeWantRead > wantReadBefore, "SSL_write должен был получить WANT_READ")
            assertTrue(
                clientEngine.appDataReadDuringWrite > 0,
                "прикладные данные должны были расшифроваться внутри записи (иначе сценарий не проверен)"
            )
            assertTrue(clientEngine.isInitFinished)
            val bufferedBeforeRead = client.hasBufferedData

            // Следующий read() отдаёт B1B2 сразу, даже если в сокете больше ничего нет
            val next = client.read()
            assertContentEquals((b1 + b2).toList(), next?.toList(), "B1B2 — следующим read() и в порядке прихода")
            assertTrue(bufferedBeforeRead, "до read() признак расшифрованного при отправке был виден")
            assertFalse(client.hasBufferedData)
            atClient.addAll(next!!)

            server.send(b3)
            pump(client, atClient) { atClient.size >= (b1 + b2 + b3).size }
            assertContentEquals((b1 + b2 + b3).toList(), atClient, "порядок потока сохранён")

            pump(server, atServer) { atServer.size >= (a + c).size }
            assertContentEquals((a + c).toList(), atServer, "сервер получил A, затем C")
        } finally {
            client.close()
            server.close()
            pair.dispose()
        }
    }
}
