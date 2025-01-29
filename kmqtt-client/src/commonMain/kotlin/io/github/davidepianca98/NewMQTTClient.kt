package io.github.davidepianca98

import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPubrel
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pubrec
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pubrel
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Unsubscribe
import io.github.davidepianca98.mqtt.packets.mqttv4.SubackReturnCode
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Puback
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Subscribe
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.locks.ReentrantLock
import kotlinx.atomicfu.locks.withLock
import io.github.davidepianca98.mqtt.MQTTCurrentPacket
import io.github.davidepianca98.mqtt.MQTTException
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.Subscription
import io.github.davidepianca98.mqtt.packets.ConnectFlags
import io.github.davidepianca98.mqtt.packets.MQTTPacket
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTConnack
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTConnect
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTDisconnect
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPingreq
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPingresp
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPuback
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPubcomp
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPublish
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPubrec
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTSuback
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTUnsuback
import io.github.davidepianca98.mqtt.packets.mqttv4.ConnectReturnCode
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Connack
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Connect
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Disconnect
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pingreq
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Puback
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pubcomp
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Publish
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Suback
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Subscribe
import io.github.davidepianca98.mqtt.packets.mqttv4.toReasonCode
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Auth
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Connack
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Connect
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Disconnect
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pingreq
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Properties
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubcomp
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Publish
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubrec
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubrel
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Suback
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Unsuback
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Unsubscribe
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import io.github.davidepianca98.socket.IOException
import io.github.davidepianca98.socket.SocketClosedException
import io.github.davidepianca98.socket.SocketInterface
import io.github.davidepianca98.socket.streams.EOFException
import io.github.davidepianca98.socket.tls.TLSClientSettings
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow

/**
 * MQTT 3.1.1 and 5 client
 *
 * @param mqttVersion sets the version of MQTT for this client MQTTVersion.MQTT3_1_1 or MQTTVersion.MQTT5
 * @param address the URL of the server without ws/wss/mqtt/mqtts
 * @param port the port of the server
 * @param tls TLS settings, null if no TLS, otherwise it must be set
 * @param keepAlive the MQTT keep alive of the connection in seconds
 * @param webSocket whether to use a WebSocket for the underlying connection, null if no WebSocket, otherwise the HTTP path, usually /mqtt
 * @param cleanStart if set, the Client and Server MUST discard any existing session and start a new session
 * @param clientId identifies the client to the server, but be unique on the server. If set to null then it will be auto generated
 * @param userName the username field of the CONNECT packet
 * @param password the password field of the CONNECT packet
 * @param properties the properties to be included in the CONNECT message (used only in MQTT5)
 * @param willProperties the properties to be included in the will PUBLISH message (used only in MQTT5)
 * @param willTopic the topic of the will PUBLISH message
 * @param willPayload the content of the will PUBLISH message
 * @param willRetain set if the will PUBLISH must be retained by the server
 * @param willQos the QoS of the will PUBLISH message
 * @param connackTimeout timeout in seconds after which the connection is closed if no CONNACK packet has been received
 * @param connectTimeout timeout in seconds after which an exception will be thrown if the socket is not able to establish a connection
 * @param enhancedAuthCallback the callback called when authenticationData is received, it should return the data necessary to continue authentication or null if completed (used only in MQTT5 if authenticationMethod has been set in the CONNECT properties)
 * @param onConnected called when the CONNACK packet has been received and the connection has been established
 * @param onDisconnected called when a DISCONNECT packet has been received or if the connection has been terminated
 * @param onSubscribed called when a SUBACK packet has been received
 * @param onUnsubscribed called when a UNSUBACK packet has been received
 * @param debugLog set to print the hex packets sent and received
 * @param publishReceived called when a PUBLISH packet has been received
 */
public class NewMQTTClient private constructor(
    private val builder: Builder,
) {

    public data class Builder(
        val mqttVersion: MQTTVersion,
        val address: String,
        val port: Int,
        val tls: TLSClientSettings?,
        val keepAlive: Int = 60,
        val webSocket: String? = null,
        val cleanStart: Boolean = true,
        val clientId: String? = null,
        val userName: String? = null,
        val password: UByteArray? = null,
        val properties: MQTT5Properties = MQTT5Properties(),
        val willProperties: MQTT5Properties? = null,
        val willTopic: String? = null,
        val willPayload: UByteArray? = null,
        val willRetain: Boolean = false,
        val willQos: Qos = Qos.AT_MOST_ONCE,
        val connackTimeout: Int = 30,
        val connectTimeout: Int = 30,
        val autoReconnect: Boolean = true,
        val enhancedAuthCallback: (authenticationData: UByteArray?) -> UByteArray? = { null },
        val debugLog: Boolean = false
    ){

        public fun mqttVersion(mqttVersion: MQTTVersion): Builder = copy(mqttVersion = mqttVersion)

        public fun address(address: String): Builder = copy(address = address)

        public fun port(port: Int): Builder = copy(port = port)

        public fun tls(tls: TLSClientSettings?): Builder = copy(tls = tls)

        public fun keepAlive(keepAlive: Int): Builder = copy(keepAlive = keepAlive)

        public fun webSocket(webSocket: String?): Builder = copy(webSocket = webSocket)

        public fun cleanStart(cleanStart: Boolean): Builder = copy(cleanStart = cleanStart)

        public fun clientId(clientId: String?): Builder = copy(clientId = clientId)

        public fun userName(userName: String?): Builder = copy(userName = userName)

        public fun password(password: UByteArray?): Builder = copy(password = password)

        public fun password(password: String?): Builder =
            copy(password = password?.encodeToByteArray()?.toUByteArray())

        public fun properties(properties: MQTT5Properties): Builder = copy(properties = properties)

        public fun willProperties(willProperties: MQTT5Properties?): Builder =
            copy(willProperties = willProperties)

        public fun willTopic(willTopic: String?): Builder = copy(willTopic = willTopic)

        public fun willPayload(willPayload: UByteArray?): Builder = copy(willPayload = willPayload)

        public fun willPayload(willPayload: String?): Builder =
            copy(willPayload = willPayload?.encodeToByteArray()?.toUByteArray())

        public fun willRetain(willRetain: Boolean): Builder = copy(willRetain = willRetain)

        public fun willQos(willQos: Qos = Qos.AT_MOST_ONCE): Builder = copy(willQos = willQos)

        public fun connackTimeout(connackTimeout: Int): Builder =
            copy(connackTimeout = connackTimeout)

        public fun connectTimeout(connectTimeout: Int): Builder =
            copy(connectTimeout = connectTimeout)

        public fun autoReconnect(autoReconnect: Boolean): Builder =
            copy(autoReconnect = autoReconnect)

        public fun enhancedAuthCallback(
            enhancedAuthCallback: (
                authenticationData: UByteArray?
            ) -> UByteArray?
        ): Builder = copy(enhancedAuthCallback = enhancedAuthCallback)

        public fun debugLog(debugLog: Boolean): Builder = copy(debugLog = debugLog)

        public fun build(): NewMQTTClient = NewMQTTClient(this)
    }

    public fun getBuilder(): Builder = builder

    private val maximumPacketSize = builder.properties.maximumPacketSize?.toInt() ?: (1024 * 1024)
    private var socket = atomic<SocketInterface?>(null)
    private val running = atomic(false)

    private val clientId = atomic(builder.clientId ?: generateRandomClientId())
    private val keepAlive = atomic(builder.keepAlive)

    private val currentReceivedPacket = MQTTCurrentPacket(maximumPacketSize.toUInt(), builder.mqttVersion)
    private val lastActiveTimestamp = atomic(currentTimeMillis())

    // Session
    private var packetIdentifier: UInt = 1u
    // QoS 1 and QoS 2 messages which have been sent to the Server, but have not been completely acknowledged
    private val pendingAcknowledgeMessages = mutableMapOf<UInt, MQTTPublish>()
    private val pendingAcknowledgePubrel = mutableMapOf<UInt, MQTTPubrel>()
    // QoS 2 messages which have been received from the Server, but have not been completely acknowledged
    private val qos2ListReceived = mutableListOf<UInt>()

    // List of messages to be sent after CONNACK has been received
    private val pendingSendMessages = atomic(mutableListOf<UByteArray>())

    private val lock = ReentrantLock()

    // Connection
    private val topicAliasesClient = mutableMapOf<UInt, String>() // TODO reset all these on reconnection
    private val maximumQos = atomic(Qos.EXACTLY_ONCE)
    private val retainedSupported = atomic(true)
    private val maximumServerPacketSize = atomic(128 * 1024 * 1024)
    private var topicAliasMaximum = 0u
    private var wildcardSubscriptionAvailable = true
    private var subscriptionIdentifiersAvailable = true
    private var sharedSubscriptionAvailable = true
    private val receiveMax = atomic(65535u)
    private val connackReceived = atomic(false)

    init {
        with(builder) {
            if (keepAlive > 65535) {
                throw IllegalArgumentException("Keep alive exceeding the maximum value")
            }

            if (willTopic == null && (willQos != Qos.AT_MOST_ONCE || willPayload != null || willRetain)) {
                throw IllegalArgumentException("Will topic null, but other will options have been set")
            }

            if (userName == null && password != null) {
                throw IllegalArgumentException("Cannot set password without username")
            }
        }
    }

//    public fun init() {
//        if (!initialized.value) {
//            initialized.getAndSet(true)
//            running.getAndSet(true)
//
//            connectSocket(250, connectTimeout * 1000)
//        }
//    }

    @Throws(Exception::class)
    private fun initSocket(readTimeoutMs: Int, connectTimeoutMs: Int) = with(builder){
        close()

        socket.value = if (tls == null) {
            ClientSocket(
                address,
                port,
                maximumPacketSize,
                readTimeoutMs,
                connectTimeoutMs,
                ::checkError
            )
        } else {
            TLSClientSocket(
                address,
                port,
                maximumPacketSize,
                readTimeoutMs,
                connectTimeoutMs,
                tls,
                ::checkError
            )
        }
        if (webSocket != null) {
            socket.value = WebSocket(socket.value!!, address, webSocket)
        }
    }

    @Throws(SocketClosedException::class)
    private fun checkError() {
        // TODO check error in connection and trow if needed
        if (socket.value == null) {
            close()
            // Needed because of JS callbacks, otherwise the exception gets swallowed and tests don't complete correctly
            throw lastException ?: SocketClosedException("")
        }
    }

    public fun isRunning(): Boolean = running.value

    public fun isConnackReceived(): Boolean = connackReceived.value

    @Throws(SocketClosedException::class, IOException::class)
    private fun send(data: UByteArray, isConnectOrAuthPacket: Boolean = false) {
        if (connackReceived.value || isConnectOrAuthPacket) {
            socket.value?.send(data) ?: throw SocketClosedException("MQTT send failed")
            if (builder.debugLog) {
                println("Sent: " + data.toHexString())
            }
            lastActiveTimestamp.value = currentTimeMillis()
        } else {
            pendingSendMessages.value += data
        }
    }

    private fun sendConnectRequest(): MqttConnectionEvent = with(builder){
        val connect = when(mqttVersion) {
            MQTTVersion.MQTT3_1_1 -> {
                MQTT4Connect(
                    "MQTT",
                    ConnectFlags(
                        userName != null,
                        password != null,
                        willRetain,
                        willQos,
                        willTopic != null,
                        cleanStart,
                        false
                    ),
                    this@NewMQTTClient.keepAlive.value,
                    this@NewMQTTClient.clientId.value,
                    willTopic,
                    willPayload,
                    userName,
                    password
                )
            }
            MQTTVersion.MQTT5 -> {
                MQTT5Connect(
                    "MQTT",
                    ConnectFlags(
                        userName != null,
                        password != null,
                        willRetain,
                        willQos,
                        willTopic != null,
                        cleanStart,
                        false
                    ),
                    this@NewMQTTClient.keepAlive.value,
                    this@NewMQTTClient.clientId.value,
                    properties,
                    willProperties,
                    willTopic,
                    willPayload,
                    userName,
                    password
                )
            }
        }
        send(connect.toByteArray(), true)
        return MqttConnectionEvent.Connect(connect)
    }

    private fun generatePacketId(): UInt {
        lock.withLock {
            do {
                packetIdentifier++
                if (packetIdentifier > 65535u)
                    packetIdentifier = 1u
            } while (isPacketIdInUse(packetIdentifier))

            return packetIdentifier
        }
    }

    private fun isPacketIdInUse(packetId: UInt): Boolean {
        lock.withLock {
            if (qos2ListReceived.contains(packetId))
                return true
            if (pendingAcknowledgeMessages[packetId] != null)
                return true
            if (pendingAcknowledgePubrel[packetId] != null)
                return true
        }
        return false
    }

    /**
     * Send a PUBLISH message
     *
     * @param retain whether the message should be retained by the server
     * @param qos the QoS value
     * @param topic the topic of the message
     * @param payload the content of the message
     * @param properties the properties to be included in the message (used only in MQTT5)
     */
    public fun publish(retain: Boolean, qos: Qos, topic: String, payload: UByteArray?, properties: MQTT5Properties = MQTT5Properties()) {
        if (!connackReceived.value && properties.authenticationData != null) {
            throw Exception("Not sending until connection complete")
        }
        if (qos > maximumQos.value) {
            throw Exception("QoS exceeding maximum server supported QoS")
        }
        if (retain && !retainedSupported.value) {
            throw Exception("Retained not supported by the server")
        }

        val packetId = if (qos != Qos.AT_MOST_ONCE) {
            generatePacketId()
        } else {
            null
        }
        val publish = if (mqttVersion == MQTTVersion.MQTT3_1_1) {
            MQTT4Publish(retain, qos, false, topic, packetId, payload)
        } else {
            // TODO support client topic aliases
            MQTT5Publish(retain, qos, false, topic, packetId, properties, payload)
        }
        if (qos != Qos.AT_MOST_ONCE) {
            lock.withLock {
                if (pendingAcknowledgeMessages.size + pendingAcknowledgePubrel.size >= receiveMax.value.toInt()) {
                    throw Exception("Sending more PUBLISH with QoS > 0 than indicated by the server in receiveMax")
                }
                pendingAcknowledgeMessages[packetId!!] = publish
            }
        }
        val data = publish.toByteArray()
        if (data.size > maximumServerPacketSize.value) {
            throw Exception("Packet size too big for the server to handle")
        }
        send(data)
    }

    /**
     * Subscribe to the specified topics
     *
     * @param subscriptions the list of topic filters and relative settings (many settings are used only in MQTT5)
     * @param properties the properties to be included in the message (used only in MQTT5)
     * @return the packet Id
     */
    public fun subscribe(subscriptions: List<Subscription>, properties: MQTT5Properties = MQTT5Properties()): UInt {
        if (!connackReceived.value && properties.authenticationData != null) {
            throw Exception("Not sending until connection complete")
        }
        val packetId = generatePacketId()
        val subscribe = if (mqttVersion == MQTTVersion.MQTT3_1_1) {
            MQTT4Subscribe(packetId, subscriptions)
        } else {
            MQTT5Subscribe(packetId, subscriptions, properties)
        }
        send(subscribe.toByteArray())
        return packetId
    }

    /**
     * Unsubscribe from the specified topics
     *
     * @param topics the list of topic filters
     * @param properties the properties to be included in the message (used only in MQTT5)
     * @return the packet Id
     */
    public fun unsubscribe(topics: List<String>, properties: MQTT5Properties = MQTT5Properties()): UInt {
        if (!connackReceived.value && properties.authenticationData != null) {
            throw Exception("Not sending until connection complete")
        }
        val packetId = generatePacketId()
        val unsubscribe = if (mqttVersion == MQTTVersion.MQTT3_1_1) {
            MQTT4Unsubscribe(packetId, topics)
        } else {
            MQTT5Unsubscribe(packetId, topics, properties)
        }
        send(unsubscribe.toByteArray())
        return packetId
    }

    /**
     * Disconnect the client
     *
     * @param reasonCode the specific reason code (only used in MQTT5)
     */
    public fun disconnect(reasonCode: ReasonCode) {
        val disconnect = if (mqttVersion == MQTTVersion.MQTT3_1_1) {
            MQTT4Disconnect()
        } else {
            MQTT5Disconnect(reasonCode)
        }
        send(disconnect.toByteArray())
        close()
    }

    private var lastException: Exception? = null

    private fun check(): Flow<MqttConnectionEvent> = flow {
        if (!running.value || socket.value == null) {
            return@flow
        }

        socket.value?.sendRemaining()
        if (connackReceived.value) {
            val pending = pendingSendMessages.getAndSet(mutableListOf())
            for (data in pending) {
                send(data)
            }
        }

        val data = try {
            socket.value?.read()
        } catch (e: Exception) {
//            close()
//            onDisconnected(null)
//            throw e
            // TODO Try reconnect
            emit(MqttConnectionEvent.Disconnected (null))
            return@flow
        }

        if (data != null) {
            try {
                if (builder.debugLog) {
                    println("Received: " + data.toHexString())
                }
                currentReceivedPacket.addData(data).forEach {
                    emit(handlePacket(it))
                }
            } catch (e: MQTTException) {
                lastException = e
                disconnect(e.reasonCode)
                onDisconnected(null)
                throw e
            } catch (e: EOFException) {
                lastException = e
                close()
                onDisconnected(null)
                throw e
            } catch (e: IOException) {
                lastException = e
                disconnect(ReasonCode.UNSPECIFIED_ERROR)
                onDisconnected(null)
                throw e
            } catch (e: Exception) {
                lastException = e
                disconnect(ReasonCode.IMPLEMENTATION_SPECIFIC_ERROR)
                onDisconnected(null)
                throw e
            }
        }

        // If connack not received in a reasonable amount of time, then disconnect
        val currentTime = currentTimeMillis()
        val lastActive = lastActiveTimestamp.value
        val isConnackReceived = connackReceived.value

        if (!isConnackReceived && currentTime > lastActive + (builder.connackTimeout * 1000)) {
            close()
            lastException = Exception("CONNACK not received in 30 seconds")
            throw lastException!!
        }

        val actualKeepAlive = keepAlive.value
        if (actualKeepAlive != 0 && isConnackReceived) {
            if (currentTime > lastActive + (actualKeepAlive * 1000)) {
                // Timeout
                close()
                lastException = MQTTException(ReasonCode.KEEP_ALIVE_TIMEOUT)
                throw lastException!!
            } else if (currentTime > lastActive + (actualKeepAlive * 1000 * 0.9)) {
                val pingreq = if (mqttVersion == MQTTVersion.MQTT3_1_1) {
                    MQTT4Pingreq()
                } else {
                    MQTT5Pingreq()
                }
                send(pingreq.toByteArray())
                // TODO if not receiving pingresp after a reasonable amount of time, close connection
                emit(MqttConnectionEvent.PingRequest(pingreq))
            }
        }
    }

    public fun connect(): Flow<MqttConnectionEvent> = flow {
        if (isRunning()) {
            //TODO Trow connection is running
        }
        initSocket(250, builder.connectTimeout * 1000)
        emit(sendConnectRequest())
        while (running.value) {
            try {
                emitAll(check())
            } finally {
                if (running.value) {
                    disconnect(ReasonCode.IMPLEMENTATION_SPECIFIC_ERROR)
                }
            }
        }
    }

//    /**
//     * Run a single iteration of the client (blocking)
//     * This function blocks the thread for a single iteration duration
//     */
//    public fun step() {
//        if (running.value) {
//            check()
//        }
//    }
//
//    /**
//     * Run the client (blocking)
//     * This function blocks the thread until the client stops
//     */
//    public fun run() {
//        while (running.value) {
//            step()
//        }
//    }
//
//    /**
//     * Run the client
//     * This function runs the thread on the specified dispatcher until the client stops
//     * @param dispatcher the dispatcher on which to run the client
//     * @param exceptionHandler the exception handler for the coroutine scope
//     */
//    public fun runSuspend(
//        dispatcher: CoroutineDispatcher = Dispatchers.Default,
//        exceptionHandler: CoroutineExceptionHandler = CoroutineExceptionHandler { _, throwable ->}
//    ) {
//        CoroutineScope(dispatcher).launch(exceptionHandler) {
//            run()
//        }
//    }
//
//    /**
//     * Init and run the client with the ability to cancel
//     * This function runs the thread on the specified dispatcher until the client stops
//     * @param dispatcher the dispatcher on which to run the client
//     * @param exceptionHandler the exception handler for the coroutine scope
//     */
//    public fun initAndRunSuspend(
//        dispatcher: CoroutineDispatcher = Dispatchers.Default,
//        exceptionHandler: CoroutineExceptionHandler = CoroutineExceptionHandler { _, throwable ->}
//    ): Job {
//        return CoroutineScope(dispatcher).launch(exceptionHandler) {
//            try {
//                init()
//                yield()
//                while (running.value) {
//                    step()
//                    yield()
//                }
//            } finally {
//                if (running.value) {
//                    disconnect(ReasonCode.IMPLEMENTATION_SPECIFIC_ERROR)
//                }
//            }
//        }
//    }

    @Throws(MQTTException::class)
    private fun handlePacket(packet: MQTTPacket): MqttConnectionEvent = when (packet) {
        is MQTTConnack -> handleConnack(packet)
        is MQTTPublish -> handlePublish(packet)
        is MQTTPuback -> handlePuback(packet)
        is MQTTPubrec -> handlePubrec(packet)
        is MQTTPubrel -> handlePubrel(packet)
        is MQTTPubcomp -> handlePubcomp(packet)
        is MQTTSuback -> handleSuback(packet)
        is MQTTUnsuback -> handleUnsuback(packet)
        is MQTTPingresp -> handlePingresp(packet)
        is MQTTDisconnect -> handleDisconnect(packet)
        is MQTT5Auth -> handleAuth(packet)
        else -> throw MQTTException(ReasonCode.PROTOCOL_ERROR)
    }

    @Throws(MQTTException::class)
    private fun handleConnack(packet: MQTTConnack): MqttConnectionEvent {
        if (packet is MQTT5Connack) {
            if (packet.connectReasonCode != ReasonCode.SUCCESS) {
                if ((packet.connectReasonCode == ReasonCode.USE_ANOTHER_SERVER || packet.connectReasonCode == ReasonCode.SERVER_MOVED) && packet.properties.serverReference != null) {
                    // TODO if reason code 0x9C try to connect to the given server (4.11 format)
                } else {
                    throw MQTTException(packet.connectReasonCode)
                }
            }

            val receiveMax = packet.properties.receiveMaximum ?: 65535u
            this.receiveMax.getAndSet(receiveMax)
            val maximumQos = packet.properties.maximumQos?.let { Qos.valueOf(it.toInt()) } ?: Qos.EXACTLY_ONCE
            this.maximumQos.getAndSet(maximumQos)
            val retainAvailable = packet.properties.retainAvailable != 0u
            this.retainedSupported.getAndSet(retainAvailable)
            val maximumServerPacketSize = packet.properties.maximumPacketSize?.toInt() ?: maximumServerPacketSize.value
            this.maximumServerPacketSize.getAndSet(maximumServerPacketSize)
            clientId.value = packet.properties.assignedClientIdentifier ?: clientId.value
            topicAliasMaximum = packet.properties.topicAliasMaximum ?: topicAliasMaximum
            wildcardSubscriptionAvailable = packet.properties.wildcardSubscriptionAvailable != 0u
            subscriptionIdentifiersAvailable = packet.properties.subscriptionIdentifierAvailable != 0u
            sharedSubscriptionAvailable = packet.properties.sharedSubscriptionAvailable != 0u

            val keepAlive = packet.properties.serverKeepAlive?.toInt() ?: keepAlive.value
            this.keepAlive.getAndSet(keepAlive)

            builder.enhancedAuthCallback(packet.properties.authenticationData)
        } else if (packet is MQTT4Connack) {
            if (packet.connectReturnCode != ConnectReturnCode.CONNECTION_ACCEPTED) {
                throw MQTTException(packet.connectReturnCode.toReasonCode())
//                throw IOException("Connection failed with code: ${packet.connectReturnCode}")
            }
        }

        connackReceived.getAndSet(true)
        if (builder.cleanStart && packet.connectAcknowledgeFlags.sessionPresentFlag) {
            throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        } else if (!builder.cleanStart && !packet.connectAcknowledgeFlags.sessionPresentFlag) {
            // Session expired on the server, so clean the local session
            packetIdentifier = 1u
            lock.withLock {
                pendingAcknowledgeMessages.clear()
                pendingAcknowledgePubrel.clear()
                qos2ListReceived.clear()
            }
        } else if (!builder.cleanStart && packet.connectAcknowledgeFlags.sessionPresentFlag) {
            // Resend pending publish and pubrel messages (with dup=1)
            lock.withLock {
                pendingAcknowledgeMessages.forEach {
                    send(it.value.setDuplicate().toByteArray())
                }
                pendingAcknowledgePubrel.forEach {
                    send(it.value.toByteArray())
                }
            }
        }
        return MqttConnectionEvent.Connected(packet)
    }

    @Throws(MQTTException::class)
    private fun handlePublish(packet: MQTTPublish) : MqttConnectionEvent {
        var correctPacket = packet

        if (correctPacket.qos > maximumQos.value) {
            throw MQTTException(ReasonCode.QOS_NOT_SUPPORTED)
        }

        if (correctPacket.qos > Qos.AT_LEAST_ONCE) {
            if (qos2ListReceived.size > (builder.properties.receiveMaximum?.toInt() ?: 65535)) {
                // Received too many messages
                throw MQTTException(ReasonCode.RECEIVE_MAXIMUM_EXCEEDED)
            }
        }

        if (correctPacket is MQTT5Publish) {
            if (correctPacket.properties.topicAlias != null) {
                if (correctPacket.properties.topicAlias == 0u || correctPacket.properties.topicAlias!! > (builder.properties.topicAliasMaximum
                        ?: 65535u)
                ) {
                    throw MQTTException(ReasonCode.TOPIC_ALIAS_INVALID)
                }
                if (correctPacket.topicName.isNotEmpty()) {
                    // Map alias
                    topicAliasesClient[correctPacket.properties.topicAlias!!] = correctPacket.topicName
                } else if (correctPacket.topicName.isEmpty()) {
                    // Use alias
                    val topicName = topicAliasesClient[correctPacket.properties.topicAlias!!] ?: throw MQTTException(
                        ReasonCode.PROTOCOL_ERROR)
                    correctPacket = correctPacket.setTopicFromAlias(topicName)
                }
            }
        }

        return when (correctPacket.qos) {
            Qos.AT_MOST_ONCE -> {
                MqttConnectionEvent.IncomingPublication(correctPacket)
            }
            Qos.AT_LEAST_ONCE -> {
                val puback = if (correctPacket is MQTT4Publish) {
                    MQTT4Puback(correctPacket.packetId!!)
                } else {
                    MQTT5Puback(correctPacket.packetId!!)
                }
                send(puback.toByteArray())
                MqttConnectionEvent.IncomingPublication(correctPacket)
            }
            Qos.EXACTLY_ONCE -> {
                val pubrec = if (correctPacket is MQTT4Publish) {
                    MQTT4Pubrec(correctPacket.packetId!!)
                } else {
                    MQTT5Pubrec(correctPacket.packetId!!)
                }
                send(pubrec.toByteArray())
                if (!qos2ListReceived.contains(correctPacket.packetId!!)) {
                    qos2ListReceived.add(correctPacket.packetId!!)
                    MqttConnectionEvent.IncomingPublication(correctPacket)
                } else {
                    MqttConnectionEvent.IncomingPublicationDuplicate(correctPacket)
                }
            }
        }
    }

    @Throws(MQTTException::class)
    private fun handlePuback(packet: MQTTPuback): MqttConnectionEvent {
        if (packet is MQTT5Puback && builder.properties.requestProblemInformation == 0u && (packet.properties.reasonString != null || packet.properties.userProperty.isNotEmpty())) {
            throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        }
        lock.withLock {
            pendingAcknowledgeMessages.remove(packet.packetId)
        }
        return MqttConnectionEvent.PublicationConfirmed(packet)
    }

    @Throws(MQTTException::class)
    private fun handlePubrec(packet: MQTTPubrec): MqttConnectionEvent {
        if (packet is MQTT5Pubrec && builder.properties.requestProblemInformation == 0u && (packet.properties.reasonString != null || packet.properties.userProperty.isNotEmpty())) {
            throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        }
        return MqttConnectionEvent.PublicationReceived(packet, lock.withLock {
            pendingAcknowledgeMessages.remove(packet.packetId)
            val pubrel = if (packet is MQTT4Pubrec) {
                MQTT4Pubrel(packet.packetId)
            } else {
                MQTT5Pubrel(packet.packetId)
            }
            pendingAcknowledgePubrel[packet.packetId] = pubrel
            send(pubrel.toByteArray())
            return@withLock pubrel
        })
    }

    @Throws(MQTTException::class)
    private fun handlePubrel(packet: MQTTPubrel): MqttConnectionEvent {
        if (packet is MQTT5Pubrel && builder.properties.requestProblemInformation == 0u && (packet.properties.reasonString != null || packet.properties.userProperty.isNotEmpty())) {
            throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        }
        val pubcomp = if (packet is MQTT4Pubrel) {
            MQTT4Pubcomp(packet.packetId)
        } else {
            MQTT5Pubcomp(packet.packetId)
        }
        send(pubcomp.toByteArray())
        if (!qos2ListReceived.remove(packet.packetId)) {
            throw MQTTException(ReasonCode.PACKET_IDENTIFIER_NOT_FOUND)
        }
        return MqttConnectionEvent.PublicationReleased(packet, pubcomp)
    }

    @Throws(MQTTException::class)
    private fun handlePubcomp(packet: MQTTPubcomp) : MqttConnectionEvent {
        if (packet is MQTT5Pubcomp && builder.properties.requestProblemInformation == 0u && (packet.properties.reasonString != null || packet.properties.userProperty.isNotEmpty())) {
            throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        }
        lock.withLock {
            if (pendingAcknowledgePubrel.remove(packet.packetId) == null) {
                throw MQTTException(ReasonCode.PACKET_IDENTIFIER_NOT_FOUND)
            }
        }
        return MqttConnectionEvent.PublicationCompleted(packet)
    }

    @Throws(MQTTException::class)
    private fun handleSuback(packet: MQTTSuback) : MqttConnectionEvent {
        if (packet is MQTT4Suback) {
            for (reasonCode in packet.reasonCodes) {
                if (reasonCode == SubackReturnCode.FAILURE) {
                    throw MQTTException(ReasonCode.UNSPECIFIED_ERROR)
                }
            }
        } else if (packet is MQTT5Suback) {
            if (builder.properties.requestProblemInformation == 0u && (packet.properties.reasonString != null || packet.properties.userProperty.isNotEmpty())) {
                throw MQTTException(ReasonCode.PROTOCOL_ERROR)
            }
            for (reasonCode in packet.reasonCodes) {
                if (reasonCode != ReasonCode.SUCCESS && reasonCode != ReasonCode.GRANTED_QOS1 && reasonCode != ReasonCode.GRANTED_QOS2) {
                    throw MQTTException(reasonCode)
                }
            }
        }
        return MqttConnectionEvent.Subscribed(packet)
    }

    @Throws(MQTTException::class)
    private fun handleUnsuback(packet: MQTTUnsuback) : MqttConnectionEvent {
        if (packet is MQTT5Unsuback && builder.properties.requestProblemInformation == 0u && (packet.properties.reasonString != null || packet.properties.userProperty.isNotEmpty())) {
            throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        }
        return MqttConnectionEvent.Unsubscribed(packet)
    }

    private fun handlePingresp(packet: MQTTPingresp): MqttConnectionEvent {
        lastActiveTimestamp.getAndSet(currentTimeMillis())
        return MqttConnectionEvent.PingResponse(packet)
    }


    private fun handleAuth(packet: MQTT5Auth): MqttConnectionEvent {
        if (packet.authenticateReasonCode == ReasonCode.CONTINUE_AUTHENTICATION) {
            val data = builder.enhancedAuthCallback(packet.properties.authenticationData)
            val auth = MQTT5Auth(
                ReasonCode.CONTINUE_AUTHENTICATION,
                MQTT5Properties(
                    authenticationMethod = packet.properties.authenticationMethod,
                    authenticationData = data
                )
            )
            send(auth.toByteArray(), true)
            return MqttConnectionEvent.ContinueAuthentication(packet, auth)
        }
        return MqttConnectionEvent.AuthenticationError(packet)
    }

    /**
     * Start the re-authentication process, to be used only when authenticationMethod has been set in the CONNECT packet
     *
     * @param data the authenticationData if necessary
     */
    public fun reAuthenticate(data: UByteArray?) {
        val auth = MQTT5Auth(
            ReasonCode.RE_AUTHENTICATE,
            MQTT5Properties(authenticationMethod = builder.properties.authenticationMethod, authenticationData = data)
        )
        send(auth.toByteArray(), true)
    }

    @Throws(MQTTException::class)
    private fun handleDisconnect(disconnect: MQTTDisconnect) : MqttConnectionEvent {
        if (disconnect is MQTT5Disconnect) {
            close()
            if ((disconnect.reasonCode == ReasonCode.USE_ANOTHER_SERVER || disconnect.reasonCode == ReasonCode.SERVER_MOVED) && disconnect.properties.serverReference != null) {
                // TODO connect to the new server
            } else {
                throw MQTTException(disconnect.reasonCode)
            }
        }
        return MqttConnectionEvent.Disconnected(disconnect)
    }

    private fun close() {
        running.value = false
        socket.getAndSet(null)?.close()
        connackReceived.value = false
    }
}

public sealed interface MqttConnectionEvent {
    public data class Connect(val connect: MQTTConnect) : MqttConnectionEvent
    public data class Connected(val connack: MQTTConnack) : MqttConnectionEvent
    public data class Disconnected(val disconnect: MQTTDisconnect?) : MqttConnectionEvent
    public data class IncomingPublication(val publish: MQTTPublish) : MqttConnectionEvent
    public data class IncomingPublicationDuplicate(val publish: MQTTPublish) : MqttConnectionEvent
    public data class PublicationConfirmed(val puback: MQTTPuback) : MqttConnectionEvent
    public data class PublicationReceived(val pubrec: MQTTPubrec, val pubrel: MQTTPubrel) : MqttConnectionEvent
    public data class PublicationReleased(val pubrel: MQTTPubrel, val pubcomp: MQTTPubcomp) : MqttConnectionEvent
    public data class PublicationCompleted(val pubcomp: MQTTPubcomp) : MqttConnectionEvent
    public data class Subscribed(val suback: MQTTSuback) : MqttConnectionEvent
    public data class Unsubscribed(val unsuback: MQTTUnsuback) : MqttConnectionEvent
    public data class AuthenticationError(val auth: MQTT5Auth) : MqttConnectionEvent
    public data class ContinueAuthentication(val input: MQTT5Auth, val output: MQTT5Auth) : MqttConnectionEvent

    public data class PingRequest(val pingreq: MQTTPingreq) : MqttConnectionEvent
    public data class PingResponse(val pingresp: MQTTPingresp) : MqttConnectionEvent
}
