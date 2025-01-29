package io.github.davidepianca98

import io.github.davidepianca98.mqtt.MQTTCurrentPacket
import io.github.davidepianca98.mqtt.MQTTDisconnectException
import io.github.davidepianca98.mqtt.MQTTException
import io.github.davidepianca98.mqtt.MQTTProtocolException
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
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPubrel
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTSuback
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTSubscribe
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTUnsuback
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTUnsubscribe
import io.github.davidepianca98.mqtt.packets.mqttv4.ConnectReturnCode
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Connack
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Connect
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Disconnect
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pingreq
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Puback
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pubcomp
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Publish
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pubrec
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Pubrel
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Suback
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Subscribe
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Unsubscribe
import io.github.davidepianca98.mqtt.packets.mqttv4.SubackReturnCode
import io.github.davidepianca98.mqtt.packets.mqttv4.toReasonCode
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Auth
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Connack
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Connect
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Disconnect
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pingreq
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Properties
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Puback
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubcomp
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Publish
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubrec
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Pubrel
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Suback
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Subscribe
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Unsuback
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Unsubscribe
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import io.github.davidepianca98.socket.IOException
import io.github.davidepianca98.socket.SocketClosedException
import io.github.davidepianca98.socket.SocketInterface
import io.github.davidepianca98.socket.streams.EOFException
import io.github.davidepianca98.socket.tls.TLSClientSettings
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.locks.ReentrantLock
import kotlinx.atomicfu.locks.withLock
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.shareIn
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import kotlin.coroutines.cancellation.CancellationException
import kotlin.math.ceil
import kotlin.math.min

/**
 * MQTT 3.1.1 and 5 client
 */
public class NewMQTTClient private constructor(
    private val builder: Builder,
    dispatcher: CoroutineDispatcher = Dispatchers.Default
) {

    /**
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
    * @param debugLog set to print the hex packets sent and received
    */
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
        val connackTimeoutMs: Long = 30_000L,
        val connectTimeoutMs: Long = 30_000L,
        val autoInit: Boolean = true,
        val maxReconnectAttempts: Int = -1, // -1 Бесконечное количество попыток.
        val initialReconnectDelayMs: Long = 250L,
        val maxReconnectDelayMs: Long = 3_000L,
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

        public fun connackTimeoutMs(connackTimeoutMs: Long): Builder =
            copy(connackTimeoutMs = connackTimeoutMs)

        public fun connectTimeoutMs(connectTimeoutMs: Long): Builder =
            copy(connectTimeoutMs = connectTimeoutMs)

        public fun autoInit(autoInit: Boolean): Builder =
            copy(autoInit = autoInit)

        public fun enhancedAuthCallback(
            enhancedAuthCallback: (
                authenticationData: UByteArray?
            ) -> UByteArray?
        ): Builder = copy(enhancedAuthCallback = enhancedAuthCallback)

        public fun debugLog(debugLog: Boolean): Builder = copy(debugLog = debugLog)

        public fun build(): NewMQTTClient = NewMQTTClient(this)
    }

    public fun getBuilder(): Builder = builder

    private val scope = CoroutineScope(SupervisorJob() + dispatcher)

    // Reactive states
    private val _connectionState = MutableStateFlow<ConnectionState>(ConnectionState.Disconnected)
    private val _incomingMessages = MutableSharedFlow<MqttConnectionEvent>()
    private val _outgoingMessages = MutableSharedFlow<MqttConnectionEvent>()
    private val _errors = MutableSharedFlow<Throwable>()

    public val incomingMessages: SharedFlow<MqttConnectionEvent> = _incomingMessages.asSharedFlow()
    public val outgoingMessages: SharedFlow<MqttConnectionEvent> = _outgoingMessages.asSharedFlow()
    public val connectionState: StateFlow<ConnectionState> = _connectionState.asStateFlow()
    public val errors: SharedFlow<Throwable> = _errors.asSharedFlow()

    public val incomingPublications: SharedFlow<MqttConnectionEvent.Publish> = incomingMessages.filter {
        it is MqttConnectionEvent.Publish
    }.map {
        it as MqttConnectionEvent.Publish
    }.shareIn(
        scope = scope,
        started = SharingStarted.WhileSubscribed(),
        replay = 0
    )

    public val subscriptionConfirmed: SharedFlow<UInt> = incomingMessages.filter {
        it is MqttConnectionEvent.SubscribeAcknowledgment
    }.map {
        (it as MqttConnectionEvent.SubscribeAcknowledgment).suback.packetIdentifier
    }.shareIn(
        scope = scope,
        started = SharingStarted.WhileSubscribed(),
        replay = 0
    )

    public val unsubscribeConfirmed: SharedFlow<UInt> = incomingMessages.filter {
        it is MqttConnectionEvent.UnsubscribeAcknowledgment
    }.map {
        (it as MqttConnectionEvent.UnsubscribeAcknowledgment).unsuback.packetIdentifier
    }.shareIn(
        scope = scope,
        started = SharingStarted.WhileSubscribed(),
        replay = 0
    )

    // Automatic reconnection
    private val reconnectAttempt = atomic(0)
    private val reconnectJob = MutableStateFlow<Job?>(null)

    public sealed class ConnectionState {
        public data object Disconnected : ConnectionState()
        public data class Connecting(val attempt: Int) : ConnectionState()
        public data class Connected(val sessionPresent: Boolean) : ConnectionState()
    }

    private val socket = atomic<SocketInterface?>(null)

    /**
     * Статус установки соединения на уровне протокола
     */
    private val connackReceived = atomic(false)
    /**
     * Список сообщений которые нужно отправить после получения подтверждения CONNACK
     */
    private val pendingSendMessages = atomic(mutableListOf<UByteArray>())
    /**
     * Штамп времени последнего активного действия
     */
    private val lastActiveTimestamp = atomic(currentTimeMillis())
    /**
     * Максимальный размер пакета
     */
    private val maximumPacketSize = builder.properties.maximumPacketSize?.toInt() ?: (1024 * 1024)
    /**
     * Парсер входящих данных с разбивкой на пакеты
     */
    private val currentReceivedPacket = MQTTCurrentPacket(maximumPacketSize.toUInt(), builder.mqttVersion)


    // Параметры соединения, которые изменяются при получении пакета CONNACK
    /**
     * Время жизни соединения в секундах, если нет передачи пакетов
     */
    private val keepAlive = atomic(builder.keepAlive)
    /**
     * Id клиента установленный или сгенерированный для соединения, для восстановления соединений.
     */
    private val clientId = atomic(builder.clientId ?: generateRandomClientId())
    /**
     * Определяет максимальное количество неподтвержденных сообщений, которые сервер готов принять от клиента одновременно.
     */
    private val receiveMax = atomic(65535u)
    /**
     * Указывает максимальный уровень качества обслуживания (QoS), который сервер готов поддерживать для данного клиента.
     */
    private val maximumQos = atomic(Qos.EXACTLY_ONCE)
    /**
     * Указывает, поддерживает ли сервер сохраненные сообщения (retained messages) для данного клиента.
     */
    private val retainedSupported = atomic(true)
    /**
     * Указывает максимальный допустимый размер пакета, который сервер готов принять от клиента.
     */
    private val maximumServerPacketSize = atomic(128 * 1024 * 1024)
    /**
     * Указывает максимальное количество алиасов (псевдонимов) топиков, которые сервер готов поддерживать для данного клиента.
     */
    private var topicAliasMaximum = 0u
    private val topicAliasesClient = mutableMapOf<UInt, String>() // TODO reset all these on reconnection
    /**
     * Указывает, поддерживает ли сервер подписки с использованием wildcard-топиков (топиков с символами подстановки) для данного клиента.
     */
    private var wildcardSubscriptionAvailable = true
    /**
     * Указывает, поддерживает ли сервер идентификаторы подписок (Subscription Identifiers) для данного клиента.
     */
    private var subscriptionIdentifiersAvailable = true
    /**
     * Указывает, поддерживает ли сервер разделяемые подписки (shared subscriptions) для данного клиента.
     */
    private var sharedSubscriptionAvailable = true


    // Параметры сессии соединения
    private var packetIdentifier: UInt = 1u
    // QoS 1 and QoS 2 messages which have been sent to the Server, but have not been completely acknowledged
    private val pendingAcknowledgeMessages = mutableMapOf<UInt, MQTTPublish>()
    private val pendingAcknowledgePubrel = mutableMapOf<UInt, MQTTPubrel>()
    // QoS 2 messages which have been received from the Server, but have not been completely acknowledged
    private val qos2ListReceived = mutableListOf<UInt>()
    private val lock = ReentrantLock()

    init {
        // Checking critical parameters for connection
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

            if(autoInit) connect()
        }
    }

    public fun connect() {
        reconnectJob.update { job ->
            // Отменяем предыдущее соединение
            job?.cancel()
            // Инициируем новое соединение
            scope.launch {
                // Сбрасываем кол-во попыток переподключения
                resetReconnectAttempt()
                connectWithRetry()
            }
        }
    }

    /**
     * Disconnect the client
     *
     * @param reasonCode the specific reason code (only used in MQTT5)
     */
    public fun disconnect(reasonCode: ReasonCode = ReasonCode.SUCCESS) {
        reconnectJob.update { job ->
            if (job == null) {
                scope.launch {
                    _errors.emit(Exception("Disconnection error. MQTT client is not running."))
                }
                null
            } else {
                scope.launch {
                    val disconnect = if (builder.mqttVersion == MQTTVersion.MQTT3_1_1) {
                        MQTT4Disconnect()
                    } else {
                        MQTT5Disconnect(reasonCode)
                    }
                    _outgoingMessages.emit(MqttConnectionEvent.Disconnect(disconnect))
                    send(disconnect.toByteArray())
                    job.cancel()
                }
                null
            }
        }
    }

    public fun isRunning(): Boolean = reconnectJob.value != null

    private suspend fun connectWithRetry() {
        while (true) {
            // Устанавливаем статус соединения с указанием попытки
            _connectionState.value = ConnectionState.Connecting(reconnectAttempt.value)
            // Делаем задержку между попытками подключения
            delay(calculateReconnectDelay())
            try {
                // Пробуем установить связь
                connectSocket()
                // Отправляем пакет для соединения
                sendConnectRequest()
                // При удачном соединении сбрасываем счетчик попыток
                resetReconnectAttempt()
                _connectionState.value = ConnectionState.Connected(false)
                // Считываем данные из соединения рекурсивно
                processIncomingPackets()
            } catch (e: MQTTException) {
                // Ошибка на уровне протокола MQTT
                _errors.emit(e)
            } catch (e: MQTTProtocolException) {
                // Ошибка на уровне протокола MQTT
                _errors.emit(e)
            } catch (e: MQTTDisconnectException) {
                // Ошибка разрыва соединения с сервером в рамках протокола MQTT
                _errors.emit(e)
            } catch (e: SocketClosedException) {
                // Непредвиденное закрытие сокета
                _errors.emit(e)
            } catch (e: EOFException) {
                // Ошибка парсинга пакета MQTT
                _errors.emit(e)
            } catch (e: IOException) {
                // Ошибка на уровне чтения/записи в сокет
                _errors.emit(e)
            } catch (e: Exception) {
                // Сообщаем об ошибках
                _errors.emit(e)
            } finally {
                incrementReconnectAttempt()
                // Пробуем закрыть соединение корректно
                closeSocket()
            }
            yield()
        }
    }

    private fun resetReconnectAttempt() {
        reconnectAttempt.value = 0
    }

    private fun incrementReconnectAttempt() {
        if (reconnectAttempt.incrementAndGet() == Int.MAX_VALUE) {
            reconnectAttempt.value = 1
        }
    }

    private fun calculateReconnectDelay(): Long {
        val multiplier = min(
            reconnectAttempt.value,
            ceil(
                (builder.maxReconnectDelayMs / builder.initialReconnectDelayMs.toDouble())
            ).toInt()
        )
        return min(builder.initialReconnectDelayMs * multiplier, builder.maxReconnectDelayMs)
    }

    @Throws(Exception::class)
    private suspend fun connectSocket() {
        yield()
        with(builder) {
            val initSocket = if (tls == null) {
                ClientSocket(
                    address,
                    port,
                    maximumPacketSize,
                    250,
                    connectTimeoutMs.toInt(),
                    ::checkIncomingPackets
                )
            } else {
                TLSClientSocket(
                    address,
                    port,
                    maximumPacketSize,
                    250,
                    connectTimeoutMs.toInt(),
                    tls,
                    ::checkIncomingPackets
                )
            }
            socket.value = if (webSocket != null) {
                WebSocket(initSocket, address, webSocket)
            } else {
                initSocket
            }
        }
    }

    private fun checkIncomingPackets() {
        //TODO обратить внимание, возможно требуются дополнительные проверки перед зарпуском
//        Использовался код
//        if (socket == null) {
//            close()
//             Needed because of JS callbacks, otherwise the exception gets swallowed and tests don't complete correctly
//            throw lastException ?: SocketClosedException("")
//        }
        scope.launch {
            readIncomingPackets()
        }
    }

    private suspend fun processIncomingPackets() {
        while (true) {
            readIncomingPackets()
            yield()
        }
    }

    @Throws(MQTTException::class,
        MQTTProtocolException::class,
        SocketClosedException::class,
        EOFException::class,
        IOException::class,
        CancellationException::class)
    private suspend fun readIncomingPackets() {
        // Отправка оставщегося в буфере
        socket.value?.sendRemaining()

        // Прервыание, возможно корутина завершена
        yield()

        // Если соединение установленно, то отправляем отложенные сообщения
        if (connackReceived.value) {
            val pending = pendingSendMessages.getAndSet(mutableListOf())
            for (data in pending) {
                send(data)
            }
        }

        val data = socket.value.run {
            when {
                this != null -> read()
                else -> throw SocketClosedException("MQTT read failed")
            }
        }

        if (data != null) {
            if (builder.debugLog) {
                println("Received: " + data.toHexString())
            }
            currentReceivedPacket.addData(data).forEach {
                // Обработка входящих пакетов
                handlePacket(it)
            }
        }

        // Прервыание, возможно корутина завершена
        yield()

        // Если connack не получен в течение разумного периода времени, то отключаемся
        checkConnackTimeout()

        // Проверка состояния соединения
        checkKeepAliveTimeout()
    }

    private fun checkConnackTimeout() {
        if(connackReceived.value) return
        val currentTime = currentTimeMillis()
        val lastActive = lastActiveTimestamp.value
        if(!connackReceived.value && currentTime > lastActive + builder.connackTimeoutMs) {
            throw MQTTProtocolException(
                "MQTT CONNACK not received in ${builder.connackTimeoutMs / 1000L} seconds"
            )
        }
    }

    private suspend fun checkKeepAliveTimeout() {
        if (!connackReceived.value) return
        val actualKeepAlive = keepAlive.value * 1000L
        if (actualKeepAlive > 0) {
            val currentTime = currentTimeMillis()
            val lastActive = lastActiveTimestamp.value
            if (currentTime > lastActive + actualKeepAlive) {
                throw MQTTException(ReasonCode.KEEP_ALIVE_TIMEOUT)
            } else if (currentTime > lastActive + (actualKeepAlive * 0.9)) {
                val pingreq = if (builder.mqttVersion == MQTTVersion.MQTT3_1_1) {
                    MQTT4Pingreq()
                } else {
                    MQTT5Pingreq()
                }
                _outgoingMessages.emit(MqttConnectionEvent.PingRequest(pingreq))
                send(pingreq.toByteArray())
                // TODO if not receiving pingresp after a reasonable amount of time, close connection
            }
        }
    }

    @Throws(MQTTException::class, CancellationException::class)
    private suspend fun handlePacket(packet: MQTTPacket) {
        when (packet) {
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
    }

    @Throws(MQTTException::class, CancellationException::class)
    private suspend fun handleConnack(packet: MQTTConnack) {
        _incomingMessages.emit(MqttConnectionEvent.ConnectionAcknowledgment(packet))
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
            }
        }

        connackReceived.value = true
        _connectionState.value = ConnectionState.Connected(true)

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
    }

    @Throws(MQTTException::class, CancellationException::class)
    private suspend fun handlePublish(packet: MQTTPublish) {
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


        when (correctPacket.qos) {
            Qos.AT_MOST_ONCE -> {
                _incomingMessages.emit(MqttConnectionEvent.Publish(correctPacket))
            }
            Qos.AT_LEAST_ONCE -> {
                _incomingMessages.emit(MqttConnectionEvent.Publish(correctPacket))
                val puback = if (correctPacket is MQTT4Publish) {
                    MQTT4Puback(correctPacket.packetId!!)
                } else {
                    MQTT5Puback(correctPacket.packetId!!)
                }
                _outgoingMessages.emit(MqttConnectionEvent.PublishAcknowledgment(puback))
                send(puback.toByteArray())
            }
            Qos.EXACTLY_ONCE -> {
                _incomingMessages.emit(
                    if (!qos2ListReceived.contains(correctPacket.packetId!!)) {
                        qos2ListReceived.add(correctPacket.packetId!!)
                        MqttConnectionEvent.Publish(correctPacket)
                    } else {
                        MqttConnectionEvent.PublishDuplicate(correctPacket)
                    }
                )
                val pubrec = if (correctPacket is MQTT4Publish) {
                    MQTT4Pubrec(correctPacket.packetId!!)
                } else {
                    MQTT5Pubrec(correctPacket.packetId!!)
                }
                _outgoingMessages.emit(MqttConnectionEvent.PublishReceived(pubrec))
                send(pubrec.toByteArray())
            }
        }
    }

    @Throws(MQTTException::class, CancellationException::class)
    private suspend fun handlePuback(packet: MQTTPuback) {
        if (packet is MQTT5Puback && builder.properties.requestProblemInformation == 0u && (packet.properties.reasonString != null || packet.properties.userProperty.isNotEmpty())) {
            throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        }
        lock.withLock {
            pendingAcknowledgeMessages.remove(packet.packetId)
        }
        _incomingMessages.emit(MqttConnectionEvent.PublishAcknowledgment(packet))
    }

    @Throws(MQTTException::class, CancellationException::class)
    private suspend fun handlePubrec(packet: MQTTPubrec) {
        if (packet is MQTT5Pubrec && builder.properties.requestProblemInformation == 0u && (packet.properties.reasonString != null || packet.properties.userProperty.isNotEmpty())) {
            throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        }
        _incomingMessages.emit(MqttConnectionEvent.PublishReceived(packet))
        lock.withLock {
            pendingAcknowledgeMessages.remove(packet.packetId)
            val pubrel = if (packet is MQTT4Pubrec) {
                MQTT4Pubrel(packet.packetId)
            } else {
                MQTT5Pubrel(packet.packetId)
            }
            pendingAcknowledgePubrel[packet.packetId] = pubrel
            _outgoingMessages.emit(MqttConnectionEvent.PublishRelease(pubrel))
            send(pubrel.toByteArray())
        }
    }

    @Throws(MQTTException::class, CancellationException::class)
    private suspend fun handlePubrel(packet: MQTTPubrel) {
        if (packet is MQTT5Pubrel && builder.properties.requestProblemInformation == 0u && (packet.properties.reasonString != null || packet.properties.userProperty.isNotEmpty())) {
            throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        }
        _incomingMessages.emit(MqttConnectionEvent.PublishRelease(packet))
        val pubcomp = if (packet is MQTT4Pubrel) {
            MQTT4Pubcomp(packet.packetId)
        } else {
            MQTT5Pubcomp(packet.packetId)
        }
        _outgoingMessages.emit(MqttConnectionEvent.PublishComplete(pubcomp))
        send(pubcomp.toByteArray())
        if (!qos2ListReceived.remove(packet.packetId)) {
            throw MQTTException(ReasonCode.PACKET_IDENTIFIER_NOT_FOUND)
        }
    }

    @Throws(MQTTException::class, CancellationException::class)
    private suspend fun handlePubcomp(packet: MQTTPubcomp) {
        if (packet is MQTT5Pubcomp && builder.properties.requestProblemInformation == 0u && (packet.properties.reasonString != null || packet.properties.userProperty.isNotEmpty())) {
            throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        }
        _incomingMessages.emit(MqttConnectionEvent.PublishComplete(packet))
        lock.withLock {
            if (pendingAcknowledgePubrel.remove(packet.packetId) == null) {
                throw MQTTException(ReasonCode.PACKET_IDENTIFIER_NOT_FOUND)
            }
        }
    }

    @Throws(MQTTException::class, CancellationException::class)
    private suspend fun handleSuback(packet: MQTTSuback) {
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
        _incomingMessages.emit(MqttConnectionEvent.SubscribeAcknowledgment(packet))
    }

    @Throws(MQTTException::class, CancellationException::class)
    private suspend fun handleUnsuback(packet: MQTTUnsuback) {
        if (packet is MQTT5Unsuback && builder.properties.requestProblemInformation == 0u && (packet.properties.reasonString != null || packet.properties.userProperty.isNotEmpty())) {
            throw MQTTException(ReasonCode.PROTOCOL_ERROR)
        }
        _incomingMessages.emit(MqttConnectionEvent.UnsubscribeAcknowledgment(packet))
    }

    @Throws(CancellationException::class)
    private suspend fun handlePingresp(packet: MQTTPingresp) {
        lastActiveTimestamp.getAndSet(currentTimeMillis())
        _incomingMessages.emit(MqttConnectionEvent.PingResponse(packet))
    }

    @Throws(MQTTDisconnectException::class, CancellationException::class)
    private suspend fun handleDisconnect(disconnect: MQTTDisconnect) {
        _incomingMessages.emit(MqttConnectionEvent.Disconnect(disconnect))
        if (disconnect is MQTT5Disconnect) {
            if ((disconnect.reasonCode == ReasonCode.USE_ANOTHER_SERVER || disconnect.reasonCode == ReasonCode.SERVER_MOVED) && disconnect.properties.serverReference != null) {
                // TODO connect to the new server
                throw MQTTDisconnectException("The server has requested to switch to another server.", disconnect.reasonCode)
            } else {
                throw MQTTDisconnectException("The server has closed the connection.", disconnect.reasonCode)
            }
        } else {
            throw MQTTDisconnectException("The server has closed the connection.")
        }
    }

    @Throws(MQTTException::class, CancellationException::class)
    private suspend fun handleAuth(packet: MQTT5Auth) {
        _incomingMessages.emit(MqttConnectionEvent.Authentication(packet))
        if (packet.authenticateReasonCode == ReasonCode.CONTINUE_AUTHENTICATION) {
            val data = builder.enhancedAuthCallback(packet.properties.authenticationData)
            val auth = MQTT5Auth(
                ReasonCode.CONTINUE_AUTHENTICATION,
                MQTT5Properties(
                    authenticationMethod = packet.properties.authenticationMethod,
                    authenticationData = data
                )
            )
            _outgoingMessages.emit(MqttConnectionEvent.ContinueAuthentication(auth))
            send(auth.toByteArray(), true)
        } else {
            _outgoingMessages.emit(MqttConnectionEvent.AuthenticationError(packet))
        }

    }

    public fun isConnackReceived(): Boolean = connackReceived.value

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
        scope.launch {
            _outgoingMessages.emit(MqttConnectionEvent.ReAuthentication(auth))
            send(auth.toByteArray(), true)
        }
    }

    /**
     * Send a PUBLISH message
     *
     * @param retain whether the message should be retained by the server
     * @param qos the QoS value
     * @param topic the topic of the message
     * @param payload the content of the message is String
     * @param properties the properties to be included in the message (used only in MQTT5)
     */
    @Throws(Exception::class)
    public fun publish(retain: Boolean, qos: Qos, topic: String, payload: String?, properties: MQTT5Properties = MQTT5Properties()) {
        publish(
            retain = retain,
            qos = qos,
            topic = topic,
            payload = payload?.encodeToByteArray()?.toUByteArray(),
            properties = properties
        )
    }

    /**
     * Send a PUBLISH message
     *
     * @param retain whether the message should be retained by the server
     * @param qos the QoS value
     * @param topic the topic of the message
     * @param payload the content of the message is UByteArray
     * @param properties the properties to be included in the message (used only in MQTT5)
     * @return the packet Id if QOS = AT_MOST_ONCE
     */
    @Throws(Exception::class)
    public fun publish(retain: Boolean, qos: Qos, topic: String, payload: UByteArray?, properties: MQTT5Properties = MQTT5Properties()): UInt? {
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
        val publish = if (builder.mqttVersion == MQTTVersion.MQTT3_1_1) {
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
        scope.launch {
            _outgoingMessages.emit(MqttConnectionEvent.Publish(publish))
            send(data)
        }
        return packetId
    }

    /**
     * Subscribe to the specified topics
     *
     * @param subscriptions the list of topic filters and relative settings (many settings are used only in MQTT5)
     * @param properties the properties to be included in the message (used only in MQTT5)
     * @return the packet Id
     */
    @Throws(Exception::class)
    public fun subscribe(subscriptions: List<Subscription>, properties: MQTT5Properties = MQTT5Properties()): UInt {
        if (!connackReceived.value && properties.authenticationData != null) {
            throw Exception("Not sending until connection complete")
        }
        val packetId = generatePacketId()
        val subscribe = if (builder.mqttVersion == MQTTVersion.MQTT3_1_1) {
            MQTT4Subscribe(packetId, subscriptions)
        } else {
            MQTT5Subscribe(packetId, subscriptions, properties)
        }
        scope.launch {
            _outgoingMessages.emit(MqttConnectionEvent.Subscribe(subscribe))
            send(subscribe.toByteArray())
        }
        return packetId
    }

    /**
     * Unsubscribe from the specified topics
     *
     * @param topics the list of topic filters
     * @param properties the properties to be included in the message (used only in MQTT5)
     * @return the packet Id
     */
    @Throws(Exception::class)
    public fun unsubscribe(topics: List<String>, properties: MQTT5Properties = MQTT5Properties()): UInt {
        if (!connackReceived.value && properties.authenticationData != null) {
            throw Exception("Not sending until connection complete")
        }
        val packetId = generatePacketId()
        val unsubscribe = if (builder.mqttVersion == MQTTVersion.MQTT3_1_1) {
            MQTT4Unsubscribe(packetId, topics)
        } else {
            MQTT5Unsubscribe(packetId, topics, properties)
        }
        scope.launch {
            _outgoingMessages.emit(MqttConnectionEvent.Unsubscribe(unsubscribe))
            send(unsubscribe.toByteArray())
        }
        return packetId
    }

    private suspend fun sendConnectRequest() = with(builder){
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
        _outgoingMessages.emit(MqttConnectionEvent.Connect(connect))
        send(connect.toByteArray(), true)
    }

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

    private fun closeSocket() {
        socket.getAndSet(null)?.close()
        topicAliasesClient.clear()
        connackReceived.value = false
        _connectionState.value = ConnectionState.Disconnected
    }
}

public sealed interface MqttConnectionEvent {
    public data class Connect(val connect: MQTTConnect) : MqttConnectionEvent
    public data class ConnectionAcknowledgment(val connack: MQTTConnack) : MqttConnectionEvent
    public data class Disconnect(val disconnect: MQTTDisconnect) : MqttConnectionEvent
    public data class Publish(val publish: MQTTPublish) : MqttConnectionEvent
    public data class PublishDuplicate(val publish: MQTTPublish) : MqttConnectionEvent
    public data class PublishAcknowledgment(val puback: MQTTPuback) : MqttConnectionEvent
    public data class PublishReceived(val pubrec: MQTTPubrec) : MqttConnectionEvent
    public data class PublishRelease(val pubrel: MQTTPubrel) : MqttConnectionEvent
    public data class PublishComplete(val pubcomp: MQTTPubcomp) : MqttConnectionEvent
    public data class Subscribe(val subscribe: MQTTSubscribe) : MqttConnectionEvent
    public data class SubscribeAcknowledgment(val suback: MQTTSuback) : MqttConnectionEvent
    public data class Unsubscribe(val unsubscribe: MQTTUnsubscribe) : MqttConnectionEvent
    public data class UnsubscribeAcknowledgment(val unsuback: MQTTUnsuback) : MqttConnectionEvent
    public data class Authentication(val auth: MQTT5Auth) : MqttConnectionEvent
    public data class ReAuthentication(val auth: MQTT5Auth) : MqttConnectionEvent
    public data class AuthenticationError(val auth: MQTT5Auth) : MqttConnectionEvent
    public data class ContinueAuthentication(val auth: MQTT5Auth) : MqttConnectionEvent

    public data class PingRequest(val pingreq: MQTTPingreq) : MqttConnectionEvent
    public data class PingResponse(val pingresp: MQTTPingresp) : MqttConnectionEvent
}
