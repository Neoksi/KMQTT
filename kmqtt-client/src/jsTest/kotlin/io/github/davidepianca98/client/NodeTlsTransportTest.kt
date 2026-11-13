@file:OptIn(MqttCoreApi::class)

package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.hostcontract.ContractDriver
import io.github.davidepianca98.client.hostcontract.EndOfStream
import io.github.davidepianca98.client.hostcontract.PeerEnd
import io.github.davidepianca98.client.node.jsObject
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.socket.tls.TLSClientSettings
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.test.TestResult
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.time.Duration.Companion.seconds

/**
 * TLS-транспорт Node ([NodeTransport] с `tls.TLSSocket`) против `tls.createServer` на 127.0.0.1 с тестовым
 * ключом (`jvmTest/resources/tls/server.p12`, SAN только `localhost`; сертификат — `tls/server.pem`).
 */
internal class NodeTlsTransportTest {

    // внутренний предел ниже общего предела Mocha (60 с) — зависший сценарий падает по своему пределу
    private fun tlsTest(block: suspend CoroutineScope.(NodeContractServer) -> Unit): TestResult =
        runTest(timeout = 45.seconds) {
            withContext(Dispatchers.Default) {
                val keys = jsObject()
                keys.key = SERVER_KEY
                keys.cert = SERVER_CERT
                val server = NodeContractServer.listen(keys)
                try {
                    block(server)
                } finally {
                    server.close()
                }
            }
        }

    private fun pinned(): TLSClientSettings = TLSClientSettings(serverCertificate = SERVER_CERT)

    private suspend fun connectThrough(
        server: NodeContractServer,
        host: String,
        settings: TLSClientSettings,
        verifyHostname: Boolean,
        block: suspend (ContractDriver, MqttHost) -> Unit
    ) {
        val driver = ContractDriver()
        val nodeHost = NodeHost(MqttClock.Monotonic, NodeTransportFactory.tls(settings, verifyHostname))
        try {
            nodeHost.start(driver)
            driver.awaitRun(5_000)
            driver.send(nodeHost, HostAction.Connect(host, server.port, 5_000))
            block(driver, nodeHost)
        } finally {
            nodeHost.close(5_000)
        }
    }

    @Test
    fun pinnedCertificateHandshakeDataAndCloseNotify() = tlsTest { server ->
        connectThrough(server, "localhost", pinned(), verifyHostname = true) { driver, host ->
            val peer = server.accept()
            assertEquals(TransportEvent.Connected, driver.awaitEvent())
            driver.send(host, HostAction.Write("over tls".encodeToByteArray()))
            assertContentEquals("over tls".encodeToByteArray(), peer.readExactly(8))
            peer.write("back".encodeToByteArray())
            assertContentEquals("back".encodeToByteArray(), assertIs<TransportEvent.Bytes>(driver.awaitEvent()).bytes)
            // FlushAndClose: close_notify и FIN за данными — сервер видит штатный конец, не сброс
            driver.send(host, HostAction.Write("last".encodeToByteArray()))
            driver.send(host, HostAction.FlushAndClose(5_000))
            assertContentEquals("last".encodeToByteArray(), peer.readExactly(4))
            assertEquals(EndOfStream(PeerEnd.Fin, 0), peer.awaitEnd())
        }
    }

    @Test
    fun hostnameMismatchIsSslErrorWhenVerified() = tlsTest { server ->
        connectThrough(server, "127.0.0.1", pinned(), verifyHostname = true) { driver, _ ->
            val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent())
            assertEquals("ssl", assertIs<MqttError.Io>(failed.error).kind)
        }
    }

    @Test
    fun hostnameNotCheckedWhenVerifyHostnameOff() = tlsTest { server ->
        connectThrough(server, "127.0.0.1", pinned(), verifyHostname = false) { driver, _ ->
            server.accept()
            assertEquals(TransportEvent.Connected, driver.awaitEvent())
        }
    }

    @Test
    fun untrustedChainIsSslError() = tlsTest { server ->
        connectThrough(server, "localhost", TLSClientSettings(), verifyHostname = true) { driver, _ ->
            val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent())
            assertEquals("ssl", assertIs<MqttError.Io>(failed.error).kind)
        }
    }

    @Test
    fun brokenPemIsSslConfigError() = tlsTest { server ->
        val broken = TLSClientSettings(serverCertificate = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----")
        connectThrough(server, "localhost", broken, verifyHostname = true) { driver, _ ->
            val failed = assertIs<TransportEvent.Failed>(driver.awaitEvent())
            val kind = assertIs<MqttError.Io>(failed.error).kind
            assertEquals("ssl-config", kind)
        }
    }

    @Test
    fun trustAllConnects() = tlsTest { server ->
        connectThrough(server, "127.0.0.1", TLSClientSettings(checkServerCertificate = false), verifyHostname = false) { driver, _ ->
            server.accept()
            assertEquals(TransportEvent.Connected, driver.awaitEvent())
        }
    }

    /** Фабрика JS `MqttClient(options)` с TLS: CONNECT/CONNACK/PUBLISH/DISCONNECT поверх TLS, конец — FIN. */
    @Test
    fun facadeOverTlsWithJsFactory() = tlsTest { server ->
        val client = MqttClient(
            MqttClientOptions(
                host = "localhost",
                port = server.port,
                version = MQTTVersion.MQTT3_1_1,
                clientId = "tls-contract",
                keepAlive = 0.seconds,
                reconnect = ReconnectPolicy.Never,
                tls = pinned(),
            )
        )
        val connecting = async { client.connect() }
        val peer = server.accept()
        assertEquals(0x10, peer.readExactly(1)[0].toInt() and 0xF0, "CONNECT")
        val length = peer.readExactly(1)[0].toInt()
        peer.readExactly(length)
        peer.write(byteArrayOf(0x20, 0x02, 0x00, 0x00))
        assertIs<ConnectResult.Connected>(withTimeout(5_000) { connecting.await() })
        assertIs<PublishResult.Sent>(client.publish("t", byteArrayOf(1), Qos.AT_MOST_ONCE))
        val closing = async { client.close() }
        val publish = peer.readExactly(2)
        assertEquals(0x30, publish[0].toInt() and 0xF0, "PUBLISH")
        peer.readExactly(publish[1].toInt())
        assertContentEquals(byteArrayOf(0xE0.toByte(), 0x00), peer.readExactly(2), "DISCONNECT")
        peer.shutdownOutput()
        assertEquals(EndOfStream(PeerEnd.Fin, 0), peer.awaitEnd())
        withTimeout(10_000) { closing.await() }
    }

    @Test
    fun jsFactoryRefusesTrustAllWithHostnameCheck() = runTest {
        // WebSocket на JS теперь есть (NodeWebSocketTransportTest); «доверять всем» с проверкой имени — отказ
        assertFailsWith<IllegalArgumentException> {
            MqttClient(
                MqttClientOptions(
                    host = "localhost",
                    port = 8884,
                    webSocket = MqttWebSocket(),
                    tls = TLSClientSettings(checkServerCertificate = false)
                )
            )
        }
        assertFailsWith<IllegalArgumentException> {
            MqttClient(
                MqttClientOptions(host = "localhost", port = 8883, tls = TLSClientSettings(checkServerCertificate = false))
            )
        }
    }

    internal companion object {
        /** Тестовый сертификат `CN=localhost`, SAN `DNS:localhost` (тот же, что `jvmTest/resources/tls/server.pem`). */
        const val SERVER_CERT = """-----BEGIN CERTIFICATE-----
MIIBfzCCASagAwIBAgIJAM6lIcX81yTkMAoGCCqGSM49BAMDMCkxEzARBgNVBAoT
CmttcXR0IHRlc3QxEjAQBgNVBAMTCWxvY2FsaG9zdDAeFw0yNjEwMTYwNTA0Mjla
Fw0zNjEwMjMwNTA0MjlaMCkxEzARBgNVBAoTCmttcXR0IHRlc3QxEjAQBgNVBAMT
CWxvY2FsaG9zdDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABL7wfoK/a0UsddxY
trY27+TfMPlP0B7kK/HGf7X6c/yNxFSmbkTFh4axzamvdykhMBu8zVa9LSMBdRUa
7LdojoqjNzA1MB0GA1UdDgQWBBQ2TNig0Mq3DwMxT3EdvQVatWA7YzAUBgNVHREE
DTALgglsb2NhbGhvc3QwCgYIKoZIzj0EAwMDRwAwRAIgXaR2+3r3j4QdIYh0LOsw
a+iIOU6rbumIvii2phbDnMwCICfH8cdWuxT5djzIMjfYP3syngMYjqq1k9IZzZht
zw+D
-----END CERTIFICATE-----
"""

        /** Тестовый ключ EC P-256 из `jvmTest/resources/tls/server.p12` (пароль `kmqtt-test`), только для тестов. */
        const val SERVER_KEY = """-----BEGIN PRIVATE KEY-----
MEECAQAwEwYHKoZIzj0CAQYIKoZIzj0DAQcEJzAlAgEBBCDiahGgJ5eS1TBlse0Z
LGxikGWq+phzU7KgwP8/6sc87Q==
-----END PRIVATE KEY-----
"""
    }
}
