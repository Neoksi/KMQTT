@file:OptIn(MqttCoreApi::class)

package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.hostcontract.ContractServer
import io.github.davidepianca98.client.hostcontract.MqttHostContractTest
import io.github.davidepianca98.client.hostcontract.PendingProbe
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * общий контракт хоста ([MqttHostContractTest]) на [NodeHost] с [NodeWebSocketTransport] (`ws://` над
 * `net.Socket`) против [WsContractServer].
 */
internal class NodeWebSocketHostContractTest : MqttHostContractTest() {

    override val platform: String = "js-node-ws"

    // 16 МиБ больше буферов сокета loopback (видимый остаток записи на Linux) и укладывается в 30 с сценария
    override val bigPayload: Int = 16 * 1024 * 1024

    override val scenarioTimeout: Duration = 30.seconds

    override val writeBackpressureVisible: Boolean = js("process.platform") != "win32"

    override val writableAfterEof: Boolean = false

    override fun newHost(probe: PendingProbe?): MqttHost = NodeHost(
        MqttClock.Monotonic,
        NodeTransportFactory { context ->
            NodeWebSocketTransport(NodeTransport(context), MqttWebSocket(), secure = false).also { probe?.attach(it) }
        }
    )

    override suspend fun newServer(smallReceiveBuffer: Boolean): ContractServer = WsContractServer.listen()
}

/**
 * общий контракт хоста на браузерном хосте [BrowserHost] с [BrowserWebSocketTransport] — на глобальном
 * `WebSocket` Node (WHATWG, undici; в настоящем браузере не гонялся) против [WsContractServer].
 */
internal class BrowserHostContractTest : MqttHostContractTest() {

    override val platform: String = "js-browser-ws"

    // 16 МиБ больше буферов сокета loopback (видимый остаток записи на Linux) и укладывается в 30 с сценария
    override val bigPayload: Int = 16 * 1024 * 1024

    override val scenarioTimeout: Duration = 30.seconds

    override val writeBackpressureVisible: Boolean = js("process.platform") != "win32"

    override val writableAfterEof: Boolean = false

    override val unknownHostReported: Boolean = false

    override fun newHost(probe: PendingProbe?): MqttHost = BrowserHost(MqttClock.Monotonic) { wakeup ->
        BrowserWebSocketTransport(wakeup, MqttWebSocket(), secure = false).also { probe?.attach(it) }
    }

    override suspend fun newServer(smallReceiveBuffer: Boolean): ContractServer = WsContractServer.listen()
}
