package io.github.davidepianca98.socket.tls

import io.github.davidepianca98.close
import io.github.davidepianca98.mqtt.broker.Broker
import io.github.davidepianca98.mqtt.broker.ClientConnection
import io.github.davidepianca98.socket.IOException
import io.github.davidepianca98.socket.ServerSocket
import io.github.davidepianca98.socket.SocketState
import io.github.davidepianca98.socket.tcp.WebSocket

internal actual class TLSServerSocket actual constructor(
    private val broker: Broker,
    selectCallback: (attachment: Any?, state: SocketState) -> Boolean
) : ServerSocket(broker, selectCallback) {

    private val tlsServerContext = TLSServerContext(broker.tlsSettings!!)

    /** TLS-сокеты клиентов по дескриптору: у них могут быть открытые данные, расшифрованные при отправке. */
    private val tlsSockets = mutableMapOf<Int, TLSSocket>()

    override fun socketsWithBufferedData(): List<Int> {
        // Закрытые клиенты (их дескриптора нет в clients) забываются: номер дескриптора могут выдать новому сокету
        tlsSockets.keys.retainAll(clients.keys)
        return tlsSockets.filter { it.value.hasBufferedData }.map { it.key }
    }

    override fun accept(socket: Int, type: TCPSocketType) {
        try {
            val engine = TLSServerEngine(tlsServerContext)
            val sock = TLSSocket(socket, engine, writeRequest, buffer)
            tlsSockets[socket] = sock
            clients[socket] = when (type) {
                TCPSocketType.MQTT -> ClientConnection(sock, broker)
                TCPSocketType.MQTTWS -> ClientConnection(WebSocket(sock), broker)
                TCPSocketType.CLUSTER -> {
                    TODO("Cluster not yet complete in Native")
                    //val clusterConnection = ClusterConnection(sock, broker)
                    //val remoteAddress = (channel.socket().remoteSocketAddress as InetSocketAddress).address.hostAddress
                    //broker.addClusterConnection(remoteAddress, clusterConnection)
                    //clusterConnection
                }
            }
        } catch (e: IOException) {
            close(socket)
        }
    }

    override fun close() {
        super.close()
        tlsServerContext.close()
    }
}
