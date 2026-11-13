# KMQTT

[English](Readme.md) | Русский

**KMQTT** — клиент и брокер MQTT 3.1.1/5.0 на Kotlin Multiplatform; цель проекта — как можно больше платформ.

Этот репозиторий — форк `Neoksi/KMQTT` (ветка `new_mqtt_client`) проекта `davidepianca98/KMQTT`. В нём два клиента MQTT и
один брокер:

| Что | Где | Для чего |
|---|---|---|
| **Корутинный клиент** `io.github.davidepianca98.client.MqttClient` | [раздел ниже](#корутинный-клиент-iogithubdavidepianca98clientmqttclient) (только в форке) | Новый код: JVM, Android, Node.js, браузеры (только WebSocket), Windows (mingwX64), Linux (linuxX64, linuxArm64), Apple (macOS, iOS, tvOS, watchOS). `suspend` API, переподключение, декларативные подписки, обратное давление. |
| **Блокирующий клиент** `MQTTClient` (старый, legacy) | [Старый блокирующий клиент](#старый-блокирующий-клиент-mqttclient) | Старый код. Остаётся как есть, не развивается. |
| **Брокер** `Broker` | [Начало работы с брокером](#начало-работы-с-брокером) | Встроенный брокер для приложений и тестов, а также отдельная программа. |

## Возможности клиента (старый блокирующий `MQTTClient`)

У корутинного клиента своя таблица платформ, см. [«Платформы»](#корутинный-клиент-iogithubdavidepianca98clientmqttclient) ниже.

:x: = нужно сделать  
:white_check_mark: = поддерживается  
:heavy_plus_sign: = в работе

|        Платформа        |     MQTT 3.1.1     |      MQTT 5.0      |        TCP         |        TLS         |     Websocket      |
|:-----------------------:|:------------------:|:------------------:|:------------------:|:------------------:|:------------------:|
|           JVM           | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|       Windows X64       | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|        Linux X64        | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|       Linux ARM64       | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|         Node.js         | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|         iOS X64         | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|        iOS ARM64        | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|   iOS Simulator ARM64   | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|        macOS X64        | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|       macOS ARM64       | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|        tvOS X64         | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|       tvOS ARM64        | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|  tvOS Simulator ARM64   | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|       watchOS X64       | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|      watchOS ARM32      | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
|      watchOS ARM64      | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| watchOS Simulator ARM64 | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |

## Возможности брокера
:x: = нужно сделать  
:white_check_mark: = поддерживается  
:heavy_plus_sign: = в работе

|  Платформа  |     MQTT 3.1.1     |      MQTT 5.0       |         TCP         |        TLS         |     Websocket      |    Кластеризация  |
|:-----------:|:------------------:|:-------------------:|:-------------------:|:------------------:|:------------------:|:-----------------:|
|     JVM     | :white_check_mark: | :white_check_mark:  | :white_check_mark:  | :white_check_mark: | :white_check_mark: | :heavy_plus_sign: |
| Windows X64 | :white_check_mark: | :white_check_mark:  | :white_check_mark:  | :white_check_mark: | :white_check_mark: |        :x:        |
|  Linux X64  | :white_check_mark: | :white_check_mark:  | :white_check_mark:  | :white_check_mark: | :white_check_mark: |        :x:        |
| Linux ARM64 | :white_check_mark: | :white_check_mark:  | :white_check_mark:  | :white_check_mark: | :white_check_mark: |        :x:        |
|   Node.js   | :white_check_mark: | :white_check_mark:  | :white_check_mark:  | :white_check_mark: | :white_check_mark: |        :x:        |
|  macOS X64  | :white_check_mark: | :white_check_mark:  | :white_check_mark:  | :white_check_mark: | :white_check_mark: |        :x:        |
| macOS ARM64 | :white_check_mark: | :white_check_mark:  | :white_check_mark:  | :white_check_mark: | :white_check_mark: |        :x:        |




## Корутинный клиент (`io.github.davidepianca98.client.MqttClient`)

Клиент MQTT 3.1.1/5.0 для корутин; стоит рядом со [старым блокирующим `MQTTClient`](#старый-блокирующий-клиент-mqttclient).
Сам занимается переподключением, декларативными подписками, результатами публикаций и обратным давлением, поэтому
прикладному коду не нужен собственный слой управления соединением.

**Где он есть.** Этот клиент есть только в этом форке (`Neoksi/KMQTT`, ветка `new_mqtt_client`): в upstream и в
Maven-артефактах `1.0.0`, о которых сказано в разделе про старый клиент, его нет. Форк собирается из исходников, см.
[«Начало работы»](#начало-работы-с-корутинным-клиентом). Публичный API использует типы `kotlinx.coroutines` (`Flow`,
`StateFlow`, `Deferred`, `Job`) и типы `kmqtt-common` (`Qos`, `ReasonCode`, `MQTTVersion`, `TLSClientSettings`);
`kmqtt-client` отдаёт оба модуля как зависимости `api`, поэтому они подключаются сами.

**Платформы.** API и протокольный движок лежат в `commonMain` и компилируются для всех платформ. Хост ввода-вывода
(`MqttHost`) есть для:

| Платформа | Хост (фабрика `MqttClient(options)`) | TCP | TLS | WebSocket |
|---|---|---|---|---|
| JVM (в том числе Android) | `NioActorHost`: селектор `java.nio` в своём потоке `kmqtt-io-<n>` | да | да | да |
| JS, Node.js | `NodeHost`: обороты цикла событий Node (`net`, `tls`), своего потока нет | да | да | да |
| JS, браузер (любая среда без `process` Node) | браузерный хост: обороты цикла событий (`MessageChannel`), WebSocket API среды; `BrowserMqttClient(options)` | нет | только `wss://` | да |
| mingwX64 (Windows) | `WinSockHost`: `select` WinSock в своём воркере Kotlin/Native `kmqtt-io-<n>` | да | да (OpenSSL) | нет |
| linuxX64, linuxArm64 | `PosixHost`: `poll` POSIX в своём воркере Kotlin/Native `kmqtt-io-<n>` (пробуждение через `pipe`) | да | да (OpenSSL) | нет |
| Apple: macosX64, macosArm64, iosX64, iosArm64, iosSimulatorArm64, tvosX64, tvosArm64, tvosSimulatorArm64, watchosX64, watchosArm32, watchosArm64, watchosSimulatorArm64 | тот же `PosixHost` поверх BSD-сокетов (`SO_NOSIGPIPE` на каждом сокете вместо `MSG_NOSIGNAL`) | да | да (OpenSSL; на iOS/tvOS/watchOS — закреплённый сертификат) | нет |

Хост Apple — тот же код, что на Linux (один source set `posixHostMain`); различается только защита от `SIGPIPE`.
Цели Apple компилируются на любой ОС (klib), но работает хост только на технике Apple: его тесты запускаются как
`macosArm64Test` и `macosX64Test` на Mac и, с Xcode, на симуляторах iOS/tvOS/watchOS (`iosSimulatorArm64Test` и подобные);
цели устройств (iosArm64, tvosArm64, watchosArm32, watchosArm64) только компилируются (см. [«Ограничения»](#ограничения)).
Все хосты проходят один и тот же набор контрактных тестов
(`kmqtt-client/src/commonTest/.../hostcontract`; на JS у хостов Node и браузера — ещё и поверх WebSocket). Параметры,
которые фабрика платформы выполнить не может (WebSocket на mingwX64, Linux и Apple, в браузере — всё, кроме WebSocket и TLS
по умолчанию, см. [«WebSocket на JS»](#websocket-на-js-nodejs-и-браузеры)), отклоняются с `IllegalArgumentException`;
клиент никогда не переходит на
открытый текст молча. У Android нет своего хоста: он использует JVM-артефакт (цель `jvm` собирается под JVM 1.8) и тот же
`NioActorHost`.

Что он даёт:

- одно соединение с автоматическим переподключением (`ReconnectPolicy`: экспоненциальная задержка, джиттер, необязательный
  предел попыток);
- подписки, объявленные именованными группами; они восстанавливаются после каждого переподключения, отказы не ломают
  сессию; фильтр, который не является допустимым фильтром MQTT, отклоняется на месте и никуда не отправляется;
- `publish` возвращает результат (`Sent`, `Delivered`, `Expired`, `Failed`) и имеет офлайн-очередь со сроком жизни, так что
  устаревшая команда не уйдёт с опозданием;
- необязательное продолжение сессии: неподтверждённые публикации QoS 1/2 досылаются после переподключения, кроме
  помеченных `resend = false`;
- один упорядоченный `Flow` входящих сообщений с границами сессий и настоящим обратным давлением (ни одно сообщение не
  теряется);
- штатное закрытие, которое не вызывает сброс TCP;
- TLS с проверкой имени хоста по умолчанию (JVM, Node.js, mingwX64 и Linux — на OpenSSL; в браузере `wss://` с
  проверками самого браузера);
- MQTT поверх WebSocket, `ws://` и `wss://` (JVM, Node.js, браузеры);
- расширенная аутентификация MQTT 5 (обмен AUTH, например SCRAM), одинаково на любом транспорте;
- ни одного блокирующего вызова в ваших потоках: весь ввод-вывод идёт в одном daemon-потоке `kmqtt-io-<n>` на клиента (на
  Node.js — в цикле событий, без блокирующих вызовов).

### Начало работы с корутинным клиентом

Форк не опубликован в Maven. Подключите его к своему проекту как git-подмодуль и собирайте из исходников вместе с
вашим кодом: Gradle `includeBuild` подменяет Maven-координаты проектами форка:

```bash
git submodule add -b new_mqtt_client https://github.com/Neoksi/KMQTT libs/kmqtt
git submodule update --init
```

```kotlin
// settings.gradle.kts вашего проекта
includeBuild("libs/kmqtt") {
    dependencySubstitution {
        substitute(module("io.github.davidepianca98:kmqtt-common")).using(project(":kmqtt-common"))
        substitute(module("io.github.davidepianca98:kmqtt-client")).using(project(":kmqtt-client"))
        substitute(module("io.github.davidepianca98:kmqtt-broker")).using(project(":kmqtt-broker")) // только для тестов со встроенным брокером
    }
}
```

```kotlin
// build.gradle.kts модуля Kotlin Multiplatform: зависимость в commonMain, как в «Gradle» старого клиента
kotlin {
    jvm()
    // Android-цель (androidLibrary или androidTarget) берёт JVM-вариант того же артефакта
    sourceSets {
        commonMain {
            dependencies {
                // версия для разрешения не используется: побеждает подмена выше
                implementation("io.github.davidepianca98:kmqtt-common:1.0.0")
                implementation("io.github.davidepianca98:kmqtt-client:1.0.0")
            }
        }
    }
}
```

Версия Kotlin Gradle plugin в `gradle/libs.versions.toml` форка должна совпадать с версией в вашем проекте (более старый
плагин ломает Gradle Sync в IDE на Gradle 9, консольная сборка при этом проходит). Публичный KMQTT `1.0.0` из Maven
Central имеет другой API (нет `autoInit`, `onUnsubscribed`, `init`) и не содержит корутинного клиента.

### Быстрые примеры

Всем примерам нужны такие импорты (оставьте то, что используете). Вызовы проверяет
`kmqtt-client/src/jvmTest/kotlin/io/github/davidepianca98/client/ReadmeSnippetsTest.kt`: он выполняет тот же код против
встроенного брокера форка.

```kotlin
import io.github.davidepianca98.client.*
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.socket.tls.TLSClientSettings
import kotlinx.coroutines.*
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
```

#### Минимальный: подключиться, подписаться, принять, опубликовать, закрыть

```kotlin
fun main() = runBlocking {
    val client = MqttClient(MqttClientOptions(host = "broker.example.org", port = 1883)) // MQTT 5, обычный TCP
    val received = CompletableDeferred<String>()

    // у incoming ОДИН сборщик на всю жизнь клиента: запускайте его до connect()
    val reader = launch {
        client.incoming.collect { item ->
            if (item is MqttIncoming.Message) received.complete("${item.topic}: ${item.payload.decodeToString()}")
        }
    }
    client.subscriptions.set("demo", setOf("demo/#"), Qos.AT_LEAST_ONCE) // объявляем один раз, переживает переподключения

    when (val connected = withTimeout(15.seconds) { client.connect() }) { // ждёт первый CONNACK
        is ConnectResult.Connected -> println("connected, epoch ${connected.epoch}")
        is ConnectResult.Failed -> println("not connected: ${connected.error}")
    }
    client.subscriptions.awaitApplied("demo") // SUBACK по каждому фильтру: Granted или Refused

    client.publish("demo/hello", "hi".encodeToByteArray(), Qos.AT_LEAST_ONCE, until = PublishStage.Delivered)
    println(withTimeout(5.seconds) { received.await() }) // собственное сообщение вернулось

    client.close() // вызывайте всегда, в том числе после ошибок: он останавливает поток ввода-вывода
    reader.join() // incoming завершается после close
}
```

#### Всё сразу: группы, результаты, срок жизни

```kotlin
fun main() = runBlocking {
    val client = MqttClient( // фабрика JVM: MqttClient(options)
        MqttClientOptions(
            host = "broker.example.org",
            port = 1883,
            version = MQTTVersion.MQTT5,
            credentials = MqttCredentials("user", "secret".encodeToByteArray()),
        )
    )

    // incoming: один сборщик на всю жизнь клиента
    val reader = launch {
        client.incoming.collect { item ->
            when (item) {
                is MqttIncoming.SessionStarted -> println("session ${item.epoch} started")
                is MqttIncoming.Message -> println("${item.topic}: ${item.payload.decodeToString()}")
                is MqttIncoming.SessionEnded -> println("session ${item.epoch} ended: ${item.error}")
            }
        }
    }

    // Декларативные подписки: вступают в силу после каждого CONNACK
    client.subscriptions.set("sensors", setOf("sensors/#"), Qos.AT_LEAST_ONCE)

    // connect() запускает клиент и ждёт первый CONNACK
    when (val connected = withTimeout(15.seconds) { client.connect() }) {
        is ConnectResult.Connected -> println("connected, epoch ${connected.epoch}")
        is ConnectResult.Failed -> println("not connected: ${connected.error}")
    }
    println(client.subscriptions.awaitApplied("sensors")) // фильтр -> Granted/Refused

    // Публикуем и ждём подтверждения брокера
    val result = client.publish(
        topic = "lights/kitchen/set",
        payload = "on".encodeToByteArray(),
        qos = Qos.AT_LEAST_ONCE,
        ttl = 5.seconds, // если 5 с нет соединения, сообщение протухает и не отправляется никогда
        until = PublishStage.Delivered,
    )
    when (result) {
        is PublishResult.Sent -> println("handed to the socket in session ${result.epoch}")
        is PublishResult.Delivered -> println("acknowledged by the broker")
        PublishResult.Expired -> println("no connection in time, nothing was sent")
        is PublishResult.Failed -> println("failed: ${result.reason}")
    }

    client.close() // вызывайте всегда, даже после ошибок
    reader.join() // incoming завершается после close
}
```

Значения `MqttClientOptions` по умолчанию: MQTT 5, keep-alive 30 с, таймауты подключения и CONNACK по 30 с,
`closeLinger` 3 с, `closeDrain` 3 с, офлайн-очередь на 64 публикации со сроком жизни 10 с, проверка имени хоста включена, журнала нет.
`toString()` опций скрывает адрес, учётные данные, настройки TLS и will.

#### TLS с закреплённым сертификатом

```kotlin
val client = MqttClient(
    MqttClientOptions(
        host = "broker.example.org",
        port = 8883,
        tls = TLSClientSettings(
            serverCertificate = "broker.pem", // закрепляем один сертификат: путь к файлу PEM или сам текст PEM
        ),
    )
)
```

Строка, в которой есть `-----BEGIN `, — всегда текст PEM и никогда не открывается как файл; любая другая строка —
путь. Имя хоста проверяется по умолчанию, и для закреплённого сертификата тоже: в сертификате среди альтернативных имён
(subject alternative names) должен быть хост (или IP-адрес) из `host`. Без `serverCertificate` используется системное
хранилище доверия. Подробнее — [«TLS (JVM)»](#tls-jvm) и [«TLS на mingwX64, Linux и Apple»](#tls-на-mingwx64-linux-и-apple-openssl).

#### Автоматическое переподключение и наблюдение за состоянием

```kotlin
fun CoroutineScope.watch(client: MqttClient) {
    // для интерфейса: StateFlow, поэтому он склеивает значения; не рассчитывайте увидеть каждое состояние
    launch {
        client.connection.collect { state ->
            when (state) {
                ConnectionState.Idle -> println("not started")
                is ConnectionState.Connecting -> println("connecting, attempt ${state.attempt}")
                is ConnectionState.Connected -> println("online, epoch ${state.epoch}, session present ${state.sessionPresent}")
                is ConnectionState.Waiting -> println("offline (${state.lastError}), next attempt in ${state.delay}")
                is ConnectionState.Closed -> println("closed: ${state.error}")
            }
        }
    }
    // для данных: строго по порядку, с границами каждого соединения
    launch {
        client.incoming.collect { item ->
            when (item) {
                is MqttIncoming.SessionStarted -> println("new connection ${item.epoch}: values of older epochs are stale")
                is MqttIncoming.Message -> println("[${item.epoch}] ${item.topic}")
                is MqttIncoming.SessionEnded -> println("connection ${item.epoch} lost: ${item.error}")
            }
        }
    }
}

fun CoroutineScope.startClient(): MqttClient {
    val client = MqttClient(
        MqttClientOptions(
            host = "broker.example.org",
            port = 1883,
            reconnect = ReconnectPolicy(initial = 1.seconds, max = 30.seconds, jitter = 0.2), // maxAttempts = null: без предела
            logger = object : MqttLogger { // только числа и коды: ни тем, ни адресов
                override fun reconnectIn(delayMs: Long) = println("retry in $delayMs ms")
            },
        )
    )
    watch(client)
    client.subscriptions.set("devices", setOf("devices/+/state"), Qos.AT_LEAST_ONCE) // восстанавливается после каждого переподключения
    client.start() // не ждёт; connect() дождался бы первого CONNACK
    return client // когда закончите, вызовите client.close()
}
```

Подробнее: [«Переподключение»](#переподключение), [«Входящие сообщения и сессии»](#входящие-сообщения-и-сессии).

#### Продолжение сессии и `resend`

```kotlin
val client = MqttClient(
    MqttClientOptions(
        host = "broker.example.org",
        port = 1883,
        clientId = "kitchen-panel-1", // обязателен: брокер находит сессию по нему
        persistentSession = true, // Clean Start = 0, неподтверждённые QoS 1/2 переживают переподключение
        sessionExpiry = 10.minutes, // MQTT 5 Session Expiry Interval
    )
)
client.start()

// Обычное значение: если соединение оборвётся до PUBACK, после переподключения оно будет отправлено заново (DUP = 1),
// но только пока не истёк ttl (он считается от этого вызова)
val state = client.publish(
    "home/kitchen/panel", "online".encodeToByteArray(), Qos.AT_LEAST_ONCE,
    retain = true, ttl = 1.minutes, until = PublishStage.Delivered,
)

// Команда с побочным эффектом: никогда не пишется дважды. Если соединение оборвётся до PUBACK, сразу придёт
// Failed(ConnectionLost), и вы решаете сами (нужна ли она ещё? спросить пользователя снова?)
val command = client.publish(
    "home/boiler/set", "on".encodeToByteArray(), Qos.AT_LEAST_ONCE,
    ttl = 5.seconds, until = PublishStage.Delivered, resend = false,
)
```

`Failed(SessionLost)` значит, что у брокера уже нет сессии (она истекла или брокер перезапущен): ничего не
отправляется заново. Состояние хранится только в памяти экземпляра клиента. Подробнее — [«Продолжение сессии»](#продолжение-сессии).

#### WebSocket

```kotlin
val client = MqttClient(
    MqttClientOptions(
        host = "broker.example.org",
        port = 8083, // порт WebSocket у брокера
        webSocket = MqttWebSocket(path = "/mqtt"), // ws://. Добавьте tls = TLSClientSettings(...) для wss://
    )
)
```

Всё остальное (подписки, публикации, переподключение) работает так же, как по TCP. Подробнее — [«WebSocket (JVM)»](#websocket-jvm) и
[«WebSocket на JS»](#websocket-на-js-nodejs-и-браузеры).

#### Android и Kotlin Multiplatform

На Android клиент — тот же, что на JVM: зависимость кладётся в `commonMain` (или в Android-модуль) точно как в
[«Начале работы»](#начало-работы-с-корутинным-клиентом), приложению нужно разрешение `android.permission.INTERNET`. TLS и
WebSocket умеет только фабрика платформы `MqttClient(options)`, и она **не** `expect`/`actual`: это отдельные функции в
`jvmMain`, `jsMain`, `mingwX64Main`, `linuxMain` и `appleTlsMain` (все цели Apple), поэтому из общего кода её вызвать нельзя. Общая функция `MqttClient(options, host)`
требует хост и отклоняет `tls` и `webSocket`. Оберните фабрику платформы сами:

```kotlin
// commonMain
expect fun createMqttClient(options: MqttClientOptions): MqttClient

// jvmMain и androidMain
actual fun createMqttClient(options: MqttClientOptions): MqttClient = MqttClient(options)

// mingwX64Main, linuxMain и source set'ы Apple (TCP и TLS) и jsMain (Node.js; в браузере — только WebSocket) выглядят так же
```

Заводите по одному клиенту на брокер на всю жизнь приложения (область приложения или сервис), собирайте `incoming` один
раз в долгоживущей области и вызывайте `close()`, когда область заканчивается. TLS-код клиента не использует
`java.util.Base64`, которого на Android нет до API 26 (поддерживается `minSdk` 24).

### Публикация

`publish(topic, payload, qos = AT_LEAST_ONCE, retain = false, ttl, until = Sent, resend = true)` приостанавливается до
нужной стадии и возвращает ровно один результат на вызов:

| Результат | Что значит |
|---|---|
| `Sent(epoch)` | PUBLISH передан транспорту живой сессии. Поток ввода-вывода не подтверждает, что операционная система приняла все байты, и брокер пока ничего не подтвердил. |
| `Delivered(epoch)` | Брокер подтвердил: PUBACK для QoS 1, PUBCOMP для QoS 2. Только при `until = PublishStage.Delivered`. У QoS 0 подтверждения нет, он заканчивается как `Sent`. |
| `Expired` | За время `ttl` живой сессии не было; сообщение не отправлялось. |
| `Failed(reason)` | `ConnectionLost` (записано, но соединение оборвалось до подтверждения), `SessionLost` (только при продолжении сессии: записано в прежнем соединении, а у брокера сессии уже нет), `Closed`, `Rejected(code)` (отклонено клиентом по тому, что брокер объявил в CONNACK, или брокером кодом PUBACK MQTT 5 от 0x80), `QueueFull`. |

Что стоит знать:

- Публикации и изменения подписок попадают в сокет в порядке вызовов.
- Без соединения публикация ждёт в офлайн-очереди (`MqttClientOptions.offlineQueue`) не дольше `ttl`. Протухшее никогда
  не отправляется позже — в этом и смысл: не включайте свет с опозданием на десять минут.
- `QueueFull` приходит от офлайн-очереди (`OfflineQueue.capacity`) или сразу, без блокировки, когда `MqttLimits.commandQueue`
  публикаций (1024) уже ждут, пока их заберёт поток ввода-вывода.
- По умолчанию каждая сессия начинается с чистого листа. Публикация, записанная, но не подтверждённая к моменту обрыва, после
  переподключения не отправляется заново; при `until = Delivered` вы получите `Failed(ConnectionLost)` и решите сами,
  повторять ли. При `persistentSession = true` она отправляется заново, см. [«Продолжение сессии»](#продолжение-сессии).
- В MQTT 3.1.1 в PUBACK нет кода отказа, поэтому `Delivered` не доказывает, что контроль доступа брокера пропустил
  публикацию.
- Отмена вызывающей корутины до записи пакета убирает публикацию из очереди; после записи она лишь перестаёт ждать.
- Массив с данными не копируется; не меняйте его после вызова.

### Продолжение сессии

По умолчанию выключено. `MqttClientOptions(persistentSession = true, clientId = "...", sessionExpiry = 10.minutes)`
подключается с Clean Start = 0 (Clean Session = 0 в MQTT 3.1.1) и после переподключения продолжает сессию брокера:

- Клиент хранит каждую неподтверждённую публикацию QoS 1 и QoS 2 (идентификатор пакета, данные, срок жизни) между
  переподключениями, не больше `MqttLimits.maxUnacknowledged` (64); сверх этого новые публикации QoS 1/2 ждут в офлайн-очереди.
- CONNACK с **Session Present = 1**: они отправляются заново с флагом DUP и прежними идентификаторами пакетов (QoS 2 после
  PUBREC — PUBREL), в прежнем порядке и раньше всего, что опубликовано позже. Ожидающий вызов с `until = Delivered`
  завершится как обычно `Delivered`; тот, что уже получил `Sent`, досылается молча.
- Срок жизни по-прежнему главный: он считается от вызова `publish`, и публикация, у которой `ttl` истёк до досылки, не
  досылается (`Expired`, пока соединения нет). Команда не выполнится с опозданием только потому, что сессия уцелела.
- CONNACK с **Session Present = 0** (сессия истекла, брокер перезапущен): они завершаются `Failed(SessionLost)`, ничего не
  досылается.
- `publish(..., resend = false)` помечает публикацию, которую нельзя записывать дважды, например опасную команду,
  требовавшую подтверждения: если соединение оборвётся до подтверждения, она сразу завершится `ConnectionLost`, как при
  чистой сессии.
- Правило: публикация, получившая `Failed` или `Expired`, больше не записывается; отмена вызывающей корутины тоже её убирает.
- `clientId` обязателен (брокер находит сессию по нему). `sessionExpiry` (MQTT 5 Session Expiry Interval, не меньше 1 с,
  `Duration.INFINITE` = никогда) — сколько брокер хранит сессию; в MQTT 3.1.1 такого поля нет, срок решает брокер.
- `sessionResumeWindow` (необязательно, для MQTT 3.1.1): продолжать, только если прошлое подключение этого клиента
  кончилось не дольше этого срока назад; иначе, и для первого подключения экземпляра клиента, CONNECT идёт с чистым
  стартом и брокер стирает накопленное (Mosquitto по умолчанию хранит сессии 3.1.1 бессрочно). Неудачные попытки без
  CONNACK отсчёт не сдвигают.
- Подписки по-прежнему оформляются заново после каждого CONNACK; фильтры, снятые в офлайне, отписываются при продолжении.
  Пока клиента нет, брокер может копить сообщения QoS 1/2 по подпискам сессии и доставить их после переподключения.
- Состояние живёт в памяти экземпляра клиента: новый `MqttClient` или перезапущенный процесс начинает без него (брокер
  может всё равно ответить Session Present = 1, тогда просто нечего досылать).
- QoS 1 — «хотя бы раз»: досланная публикация может дойти до подписчиков дважды, если первая копия дошла.
- Входящие сообщения QoS 2 доставляются ровно один раз и при продолжении сессии: клиент помнит идентификаторы пакетов,
  на которые ответил PUBREC, и применяет их при разборе CONNACK, поэтому PUBLISH (DUP = 1) или PUBREL, которые брокер
  досылает сразу за CONNACK (Mosquitto так делает, часто тем же чтением), распознаются.
- Как проверить: `PersistentSessionBrokerTest` (встроенный брокер, прокси рвёт соединение), `EnginePersistentSessionTest` и
  `MqttSessionCoreResumeTest` (виртуальное время) в `kmqtt-client/src/jvmTest`.

### Подписки

Подписки объявляются именованными **группами**; желаемый набор — объединение всех групп, а QoS фильтра — максимум по его
группам.

```kotlin
val subs = client.subscriptions

// заменить набор группы (пустой набор удаляет группу)
subs.set("sensors", setOf("sensors/+/temperature"), Qos.AT_LEAST_ONCE) // qos по умолчанию AT_MOST_ONCE
val applied: Map<String, TopicState> = subs.awaitApplied("sensors") // ждёт SUBACK

// следовать за потоком наборов фильтров; группа удаляется, когда задача кончилась или клиент закрыт
val job = subs.bind("devices", filterSets, Qos.AT_MOST_ONCE, scope) // filterSets: Flow<Set<String>>

// краткая подписка, например на время поиска устройств
val verdict = subs.withTemporary(setOf("homeassistant/+/+/config")) { group ->
    val refusal = withTimeoutOrNull(3.seconds) { group.refused.await() }
    if (refusal != null) "not allowed: ${refusal.code}" else "window is over"
}

subs.state.value // фильтр -> Pending | Granted(qos) | Refused(code)
subs.refusals.collect { println("refused ${it.code}") } // события, по одному на группу с отклонённым фильтром
subs.resubscribe("sensors") // подписаться заново, чтобы брокер заново прислал retained-сообщения
subs.set("sensors", setOf("sensors/+/temperature"), Qos.AT_LEAST_ONCE, refreshShared = true) // то же для фильтров, которые уже держит другая группа
```

- Изменение в живой сессии отправляет только разницу: UNSUBSCRIBE снятых фильтров, затем SUBSCRIBE добавленных. После
  каждого CONNACK весь набор подписывается заново пачками по `MqttLimits.subscribeBatch` (50).
- Отклонённый фильтр (SUBACK 0x80 и выше) сессию не завершает. Он получает `TopicState.Refused(code)`, и в `refusals`
  выдаётся событие. Отказ запоминается на время сессии, поэтому группа, добавившая тот же фильтр, получает ответ сразу.
- `set`, из-за которого общее число различных фильтров превысило бы `MqttLimits.maxFilters` (4096), отклоняется: группа
  остаётся прежней, журналу сообщается, а `awaitApplied` этой группы бросает `IllegalStateException`, пока очередной `set`
  группы не пройдёт.
- `awaitApplied` бросает `IllegalStateException`, если клиент закрыт (вами или сам).
- **Фильтры проверяются до отправки**. Фильтр, который не является допустимым фильтром темы MQTT, брокеру не
  отправляется никогда: пустой, содержащий U+0000, длиннее 65 535 байт в UTF-8, `#` не в виде целого последнего уровня,
  `+` не в виде целого уровня, неправильная общая подписка `$share/<имя>/<фильтр>` (имя не должно быть пустым или
  содержать `/`, `+`, `#`, а после него должен идти допустимый фильтр). Такой фильтр сразу получает
  `TopicState.Refused(ReasonCode.TOPIC_FILTER_INVALID)` и одно событие в `refusals` для своей группы, а остальные фильтры
  группы подписываются как обычно. Он не учитывается в `MqttLimits.maxFilters`, и проверка не зависит от версии
  протокола. Проверяют `EngineSubscriptionsTest` и `TopicFilterTest`.
- `set(..., refreshShared = true)` подписывает фильтры группы ещё раз, даже если их уже держит другая группа, чтобы
  брокер заново прислал их retained-сообщения. Это то же самое, что `resubscribe`, но на уровне группы.

### Входящие сообщения и сессии

`client.incoming` — это `Flow<MqttIncoming>`, который выдаёт сообщения строго в порядке получения, с границами:
`SessionStarted(epoch)`, затем `Message` этой сессии, затем `SessionEnded(epoch, error)`.

- Эпохи растут на весь процесс: у более нового соединения эпоха всегда больше. По ним отличают «значение пришло по
  текущему соединению» от «это остаток умершего».
- По умолчанию эпохи выдаёт общий счётчик процесса. `MqttClientOptions(epochSource = { ... })` берёт их из вашего
  счётчика (вызов один раз на CONNACK, на потоке ввода-вывода клиента; возвращайте растущие значения больше нуля и не
  бросайте исключений) — тогда эпоха может быть номером сессии вашего приложения.
- У `incoming` **один сборщик на всю жизнь клиента**. Второй `collect`, в том числе после отмены первого сборщика, бросает
  `IllegalStateException`. Соберите поток один раз в долгоживущей области и раздавайте дальше сами.
- `connection: StateFlow<ConnectionState>` (`Idle`, `Connecting`, `Connected`, `Waiting`, `Closed`) нужен для интерфейса и
  диагностики. Он склеивает значения, поэтому сборщик может пропустить состояния; если нужен порядок относительно
  сообщений, используйте маркеры сессий в `incoming`.
- Сообщение QoS 1 подтверждается брокеру, когда клиент кладёт его в буфер, а не когда ваш код его обработал.
- Поток завершается после `close()`; всё уже полученное выдаётся раньше, по порядку: буфер, сообщения, удержанные сверх
  него, пока чтение стояло на паузе, и итоговый `SessionEnded`.

### Обратное давление и `ConsumerStalled`

`incoming` буферизуется (`MqttLimits.incomingBuffer`, 1024 сообщения). Когда буфер полон, клиент **перестаёт читать
сокет**, давление через TCP доходит до брокера, и ничего не теряется. Keep-alive пинги продолжаются, пока чтение стоит на
паузе, поэтому брокер не отключает здорового, но медленного клиента.

Память входящего пути ограничена буфером плюс одним залпом чтения сокета: сообщения, разобранные на шаге, который
обнаружил полный буфер, удерживаются по порядку сверх него, и клиент ничего больше не читает, пока сборщик не освободит
место. На хосте JVM залп — не больше 4 × 16 КиБ = 64 КиБ байт с провода (при TLS — открытый текст от них, плюс не более
одной TLS-записи); пакет, начатый при прежних чтениях (до `maxIncomingPacket`), может дособраться в том же шаге.

Если сборщик ничего не забирает дольше, чем `MqttLimits.consumerStallLimit` (60 с), клиент считает потребителя мёртвым и
закрывает соединение с `MqttError.ConsumerStalled`. После этого клиент остаётся в `Waiting(lastError = ConsumerStalled)`
без сетевой активности, пока потребитель не освободит буфер, и только потом переподключается (но не раньше, чем пройдёт
задержка). Потребитель, который так и не вернулся, оставляет клиента в `Waiting` до `close()`, который закрывает его
сразу. Учтите, что очередь самого брокера может отбрасывать сообщения QoS 0, если клиент долго не читает (например,
`max_queued_messages` у Mosquitto), поэтому делайте потребителя быстрым или буферизуйте у себя.

### Переподключение

`ReconnectPolicy(initial = 1 s, max = 10 s, jitter = 0.2, stableAfter = 30 s, maxAttempts = null)`:

- Задержка удваивается после каждой неудачной попытки вплоть до `max` (1, 2, 4, 8, 10, 10... с). Джиттер только укорачивает
  задержку.
- Счётчик попыток и задержка сбрасываются только после **устойчивой сессии**: все SUBSCRIBE соединения подтверждены без
  отказов, либо соединение прожило не меньше `stableAfter`. Поэтому брокер, который принимает CONNECT, но запрещает
  подписки, не удержит клиента в цикле «одно соединение в секунду».
- `maxAttempts = null` — повторять без предела; число ограничивает подряд идущие попытки переподключения, после чего клиент
  переходит в `Closed(lastError)`. `ReconnectPolicy.Never` (`maxAttempts = 0`) делает первый конец соединения
  окончательным, это удобно для разовой проверки подключения: `connect()` вернёт результат единственной попытки
  (например, `Failed(Refused(code))`, по коду которого можно выбрать другую версию протокола).
- Клиент, закрывшийся сам, всё ещё держит свой поток ввода-вывода, пока вы не вызовете `close()`.
- Перенаправление брокера (`SERVER_MOVED`, `USE_ANOTHER_SERVER`) не выполняется никогда.
- Все события также попадают в `MqttLogger`, см. ниже.

#### `MqttLogger`

`MqttClientOptions.logger` (по умолчанию `MqttLogger.None`) получает диагностические события. В событиях только числа,
коды и значения `MqttError`: никогда не темы, фильтры, данные, адреса или пароли. Методы вызываются в потоке
ввода-вывода клиента, поэтому реализация должна быть быстрой и не блокировать; исключение из неё проглатывается. Все
методы необязательны.

| Метод | Когда |
|---|---|
| `connected(epoch)` | Получен CONNACK, началась сессия. |
| `disconnected(error)` | Соединение закончилось (`error == null`: штатное закрытие). |
| `reconnectIn(delayMs)` | Назначена следующая попытка. |
| `subscribe(count)` | Отправлен SUBSCRIBE на `count` фильтров. |
| `refused(count)` | Брокер отклонил `count` фильтров в SUBACK (фильтр, отклонённый на месте как недопустимый, здесь не считается; он виден в `refusals` и `TopicState`). |
| `dropped(reason)` | Входящий пакет отброшен без завершения сессии: `DropReason.TooLarge(size, limit)` или `TopicTooLong(length, limit)`. |
| `publishRefused(code)` | PUBACK MQTT 5 пришёл с кодом отказа. |
| `resent(count)` | Продолженная сессия заново отправила `count` неподтверждённых публикаций. |
| `groupRejected(filters, limit)` | `subscriptions.set` отклонён из-за `maxFilters`. |
| `packet(direction, type, size)` | Каждый пакет MQTT: направление, тип управляющего пакета, размер в байтах, больше ничего. Исходящий сообщается, когда клиент передаёт его хосту ввода-вывода на запись, входящий — когда прочитан его последний байт (в том числе пакет, который затем отброшен как слишком большой). |
| `incomingBuffer(depth, capacity)` | Обратное давление: число сообщений, ждущих сборщика `incoming`, достигло 50, 75 или 100 % от `MqttLimits.incomingBuffer` (по разу за подъём), и ещё раз, когда упало до 25 % или ниже. Пока чтение стоит на паузе, `depth` может быть больше `capacity`. Спад сообщается на следующем шаге ввода-вывода, а не в момент, когда сборщик забрал сообщение. |
| `readPaused(depth, capacity)` | Чтение от брокера поставлено на паузу, потому что буфер полон. За ним всегда следует `readResumed`, если соединение не кончилось во время паузы. |
| `readResumed(pausedMs)` | Чтение продолжено после паузы в `pausedMs` миллисекунд. |

События обратного давления показывают медленного сборщика до того, как он станет `ConsumerStalled`: растущий уровень
`incomingBuffer` и долгие паузы значат, что потребитель должен быть быстрее. Проверяют `MqttClientFacadeTest` (события
обратного давления) и `PacketLogTest` (`packet`).

### TLS (JVM)

```kotlin
import io.github.davidepianca98.socket.tls.TLSClientSettings

val client = MqttClient(
    MqttClientOptions(
        host = "broker.example.org",
        port = 8883,
        tls = TLSClientSettings(
            serverCertificate = "broker.pem", // закрепить один сертификат (путь или строка PEM); null = системное хранилище
            alpnProtocols = listOf("mqtt"), // необязательный ALPN
            serverNameIndications = null, // необязательное явное имя SNI; по умолчанию — хост подключения
        ),
    )
)
```

- TLS доступен **только** через фабрику платформы `MqttClient(options)`: на JVM (этот раздел), на Node.js, mingwX64 и
  Linux (см. ниже). Общая фабрика `MqttClient(options, host, clock)` бросает `IllegalArgumentException` при
  `tls != null`: хост, пришедший снаружи, не обязан применять TLS, а клиент никогда не переходит на открытый текст.
- **Текст PEM или путь**. `serverCertificate`, `clientCertificate` и `clientCertificateKey` принимают либо
  текст PEM, либо путь к файлу PEM. Строка, содержащая `-----BEGIN `, — **всегда** текст PEM и никогда не открывается как
  файл, даже если похожа на путь; любая другая строка — путь. Отступы, CRLF и перенос строк любой ширины (64, 76, ...)
  допустимы. Строка, не являющаяся корректным PEM (текст до `BEGIN` или после `END`, нет парного `END`, второй блок,
  символы вне base64, пустое тело), отклоняется без обращения к файловой системе. Ключ клиента должен быть в формате PKCS#8
  (`BEGIN PRIVATE KEY`, RSA или EC). Проверяют `PemInputTest` и `TlsTransportTest`.
- **Один `SSLContext` на клиента**. Сертификаты и ключи разбираются, а системное хранилище доверия
  загружается один раз, при первом подключении, и каждое переподключение использует результат; это важно для слабых
  устройств, которые переподключаются в цикле. Ошибка настроек не запоминается: следующая попытка пробует снова
  (например, когда появился файл сертификата). Проверяет `TlsTransportTest`.
- **Проверка имени хоста включена по умолчанию** (`verifyHostname = true`), и для закреплённого сертификата, и при
  подключении по IP-адресу (тогда в сертификате среди альтернативных имён должен быть этот IP). Отключайте её только явно:
  `verifyHostname = false`.
- Сочетания `TLSClientSettings.checkServerCertificate` и `verifyHostname`:

  | `checkServerCertificate` | `verifyHostname` | Поведение |
  |---|---|---|
  | `true` | `true` | По умолчанию: проверяются цепочка сертификатов и имя хоста. |
  | `true` | `false` | Цепочка проверяется, имя хоста — нет. Безопасно с закреплённым `serverCertificate`: сам закреплённый сертификат определяет, кто брокер (используйте для брокера, к которому ходят по IP-адресу, которого нет в SAN сертификата). Слабо с системным хранилищем: принимается любой действительный сертификат любого сайта, подписанный доверенным центром, поэтому закрепляйте сертификат. |
  | `false` | `true` | **Отказ при создании**: фабрики бросают `IllegalArgumentException` («trust-all TLS ... needs an explicit verifyHostname = false»). Без доверенной цепочки имя проверять не с чем, а клиент никогда не отключает проверку молча. |
  | `false` | `false` | Полное «доверять всем»: трафик шифруется, но брокер не аутентифицирован. Только для отладки и доверенной сети. |
- Используются только TLS 1.2 и 1.3. Сертификат клиента задаётся через `clientCertificate`, `clientCertificateKey` и
  `clientCertificatePassword`.
- Сбои рукопожатия приходят как `MqttError.Io(kind = "ssl")` с общим текстом: адрес и имена из сертификата не раскрываются.
  **На JVM** отказ из-за сертификата сервера различается только по `kind` (никогда по тексту): `"ssl-hostname"`
  (имя хоста не совпало с сертификатом), `"ssl-trust"` (нет доверенного пути до закреплённого сертификата или системного
  хранилища), `"ssl-expired"` (сертификат просрочен или ещё не действует; закреплённый сертификат — якорь доверия, срок его
  действия не проверяется). Если подходит несколько, порядок такой: срок, имя хоста, доверие. Прочие сбои рукопожатия
  остаются `"ssl"`. Неверные настройки TLS (файл не читается, PEM не разбирается, ключ не читается) — `"ssl-config"`, сокет
  при этом не открывается. **На Node.js** есть только `"ssl"` и `"ssl-config"`; на mingwX64 и Linux — все виды, как на JVM
  (см. ниже).
  Проверяют `TlsTransportTest` (виды отказа, просроченная цепочка `expired-chain.p12` / `expired-ca.pem` в
  `src/jvmTest/resources/tls`) и `TlsBrokerTest`.
- `TLSClientSettings.toString()` печатает ключ клиента и пароль. Никогда не пишите его в журнал; `MqttClientOptions.toString()`
  безопасен.
- **TLS на Node.js.** Фабрика цели `js` строит транспорт на модуле Node `tls` с теми же `TLSClientSettings`: цепочка
  проверяется по системным корневым сертификатам или, при `serverCertificate`, только по этому сертификату; имя хоста
  проверяется, если не `verifyHostname = false`; TLS 1.2 и выше; работают SNI (имя хоста, но не IP-адрес, либо
  `serverNameIndications`), ALPN и сертификат клиента. Прочие ошибки подключения несут код ошибки Node в `Io.kind`
  (`ECONNREFUSED`, `ECONNRESET`, ...). Проверяет `NodeTlsTransportTest` (`./gradlew :kmqtt-client:jsNodeTest`).

### TLS на mingwX64, Linux и Apple (OpenSSL)

Фабрики `MqttClient(options)` целей mingwX64, linuxX64, linuxArm64 и целей Apple при `tls != null` строят хост с TLS-транспортом
(`WinSockTransportFactory.tls`, `PosixTransportFactory.tls`): TLS-сессия OpenSSL работает поверх памяти
(memory BIO), а шифротекст идёт через тот же неблокирующий сокет хоста. Те же `TLSClientSettings` и те же правила, что на JVM:

- только TLS 1.2 и 1.3; SNI — имя хоста (для IP-адреса не шлётся) или `serverNameIndications`; ALPN — `alpnProtocols`;
- `serverCertificate` (текст PEM или путь, как выше) — **единственный** якорь доверия, системное хранилище тогда не
  используется. Закрепить можно самоподписанный сертификат, УЦ или лист под УЦ (как якорь PKIX на JVM; в OpenSSL — флаг
  `X509_V_FLAG_PARTIAL_CHAIN`). Срок действия якоря не проверяется (закреплённый просроченный сертификат принимается),
  срок листа под закреплённым УЦ — проверяется;
- без закрепления — системные корни: на Windows — хранилище `ROOT` (CryptoAPI), на Linux — связка дистрибутива
  (`/etc/ssl/certs/ca-certificates.crt`, `/etc/pki/tls/certs/ca-bundle.crt` и др., каталог `/etc/ssl/certs`), на macOS —
  связка `/etc/ssl/cert.pem`, плюс переменные `SSL_CERT_FILE`/`SSL_CERT_DIR`. Системное хранилище доверия Apple (Keychain)
  OpenSSL не видит, а на iOS, tvOS и watchOS файловой связки нет — там закрепляйте `serverCertificate` (иначе подключение
  не пройдёт, `"ssl-trust"`);
- имя хоста проверяется при `checkServerCertificate && verifyHostname` (по умолчанию): IP-литерал — по IP в SAN, имя — по DNS
  в SAN (без частичных шаблонов вида `a*.example`); если задан `serverNameIndications`, сверяется это имя. Сочетание
  «доверять всем» с `verifyHostname = true` фабрика отклоняет, как на JVM;
- сертификат и ключ клиента — текст PEM или путь; ключ может быть зашифрован паролем `clientCertificatePassword`
  (пароль в памяти C затирается после разбора). OpenSSL принимает и другие форматы ключа PEM, не только PKCS#8;
- виды отказа `MqttError.Io.kind` — `"ssl-hostname"`, `"ssl-trust"`, `"ssl-expired"`, `"ssl"`, `"ssl-config"` (по коду
  проверки OpenSSL, текст ошибок OpenSSL наружу не идёт). Отличие от JVM: OpenSSL проверяет имя раньше сроков, поэтому
  у сертификата, который и просрочен, и выдан на другое имя, вид — `"ssl-hostname"`;
- контекст OpenSSL (`SSL_CTX`, разбор сертификатов и системных корней) строится на каждое подключение, а не один раз на
  клиента, как `SSLContext` на JVM; ошибка настроек не запоминается;
- OpenSSL — статическая сборка **3.5 LTS** (3.5.9, sha256 тарбола
  `603f5602e2eef00d77fbd429d34dcd5822bb301757a1bc9cdb24c670f1eb859a` закреплён в скрипте сборки) из klib
  `kmqtt-common/src/nativeInterop` (её же использует старый клиент). Пересборка klib — `native-build/`:
  linuxX64/linuxArm64/mingwX64 — Docker (`build-linux-mingw.sh`), Apple-цели — `build-apple.sh` на Mac с полным Xcode.
  Все klib в репо — 3.5.9; Apple-klib собраны 2026-10-21 (Apple clang 17, Xcode 26.3, Kotlin/Native 2.4.20), дата сборки,
  домашний каталог, логин и uid/gid архива из них убраны. Описание cinterop — `kmqtt-common/src/nativeInterop/openssl.def` (пути ведут в дерево сборки,
  машинно-специфичных абсолютных путей нет).

Проверяют `NativeTlsSessionTest` и `NativeTlsTransportTest` (сервер OpenSSL в памяти, без сети: доверие, имя хоста, сроки,
SNI, ключ клиента, TLS ниже 1.2, close_notify, EOF, обратное давление, сроки рукопожатия) и `NativeTlsTestbedTest`
(фасад против TLS-брокера Mosquitto; без переменных `KMQTT_TLS_A`, `KMQTT_TLS_A_BADHOST`, `KMQTT_TLS_B`, `KMQTT_TLS_CERTS`
пропускается, их описание — в KDoc теста). Запуск: `./gradlew :kmqtt-client:mingwX64Test` на Windows, `linuxX64Test` — на
Linux, `macosArm64Test`/`macosX64Test` — на macOS. Тот же TLS-слой собирается во все цели Apple (с Apple-klib OpenSSL 3.5.9);
работает он только на технике Apple и проверяется тестами macOS, а не на Windows или Linux.

### WebSocket (JVM)

```kotlin
val client = MqttClient(
    MqttClientOptions(
        host = "broker.example.org",
        port = 8084, // WebSocket-порт брокера
        webSocket = MqttWebSocket(
            path = "/mqtt", // по умолчанию; Mosquitto принимает любой путь
            headers = mapOf("Authorization" to "Bearer ..."), // необязательные дополнительные заголовки запроса upgrade
        ),
        tls = TLSClientSettings(serverCertificate = "broker.pem"), // с tls: wss://, без: ws://
    )
)
client.config.transport // MqttTransportKind.SecureWebSocket
```

- Транспорт выбирают две опции: `tls` и `webSocket` (`MqttClientOptions.transport` показывает итог: `Tcp`, `Tls`,
  `WebSocket` или `SecureWebSocket`). По умолчанию — обычный TCP.
- Пакеты MQTT идут в двоичных кадрах подпротокола `mqtt` (RFC 6455). Клиент шлёт один замаскированный кадр на запись;
  входящие кадры могут быть фрагментированы, и их байты передаются разборщику MQTT по мере прихода, так что большой кадр
  целиком не буферизуется. На WebSocket-ping клиент отвечает сам, к keep-alive MQTT это отношения не имеет (PINGREQ/PINGRESP
  по-прежнему идут внутри двоичных кадров).
- `wss://` использует те же `TLSClientSettings` и те же правила, что и обычный TLS: закреплённый сертификат или системное
  хранилище, **проверка имени хоста по умолчанию включена**, ALPN и SNI из настроек. Ошибки сертификата и имени хоста приходят
  как `MqttError.Io` с теми же значениями `kind`, что и у обычного TLS (`"ssl"`, `"ssl-hostname"`, `"ssl-trust"`,
  `"ssl-expired"`).
- Как и TLS, WebSocket доступен **только** через фабрики платформ `MqttClient(options)` JVM и JS (см.
  [«WebSocket на JS»](#websocket-на-js-nodejs-и-браузеры)); общая фабрика `MqttClient(options, host, clock)` бросает
  `IllegalArgumentException` при `webSocket != null`.
- Отказ в upgrade или нарушение протокола сервером (замаскированный или текстовый кадр, зарезервированные биты, нарушенная
  последовательность фрагментов, ...) — это `MqttError.Io(kind = "websocket")`. В сообщении максимум код статуса HTTP,
  никогда не фраза причины, заголовки, тело или адрес.
- `MqttWebSocket.toString()` и `MqttClientOptions.toString()` не показывают ни путь (в строке запроса может быть токен), ни
  значения заголовков. Заголовки `Host`, `Upgrade`, `Connection` и `Sec-WebSocket-*` клиент ставит сам; в `headers` их
  задать нельзя. Ответ должен выбрать подпротокол `mqtt` или никакого, и никаких расширений.
- Закрытие: клиент посылает кадр закрытия WebSocket после последних данных (после DISCONNECT) и ждёт кадр закрытия сервера
  или конец потока (в пределах того же запаса на дописывание, около секунды), прежде чем закрыть сокет. Кадр закрытия сервера
  клиент подтверждает тем же кодом, и поток заканчивается, как при TCP FIN.

### WebSocket на JS (Node.js и браузеры)

Фабрика JS `MqttClient(options)` принимает ту же опцию `webSocket`:

- **Node.js** (`NodeHost`): тот же разбор RFC 6455, что на JVM, поверх сокета `net` (`ws://`) или `tls` (`wss://`, те же
  `TLSClientSettings` и проверки, что у [TLS на Node.js](#tls-jvm)): дополнительные заголовки upgrade, один
  замаскированный бинарный кадр на запись, потоковый приём кадров, ответ на ping, кадр закрытия после DISCONNECT, ошибки
  `MqttError.Io(kind = "websocket")` с кодом статуса HTTP, не больше. Ошибки сокета под ним сохраняют свои виды
  (`UnknownHost`, `"ssl"`, `"ECONNREFUSED"`, ...). Для своего хоста — `NodeTransportFactory.webSocket(...)`.
- **Браузер**: там, где нет `process.versions.node` Node, `MqttClient(options)` — это `BrowserMqttClient(options)`:
  браузерный хост крутится на цикле событий, транспорт — WebSocket API среды (браузеры, Node.js 22+, Deno;
  `BrowserMqttClient` можно звать напрямую в любой из них). Сырых сокетов в браузере нет, поэтому `webSocket`
  обязателен. Чего API не умеет, отклоняется с `IllegalArgumentException`, а не пропускается молча: дополнительные
  заголовки (токен — в строку запроса пути или в пароль MQTT) и любые настройки TLS, кроме умолчаний (закреплённый или
  свой сертификат, клиентский сертификат, ALPN, SNI, `checkServerCertificate = false`, `verifyHostname = false`); цепочку
  и имя хоста браузер проверяет сам. Неудачное подключение — `MqttError.Io(kind = "websocket")` с кодом закрытия: браузер
  не отличает неизвестный хост от отказа в подключении или ошибки TLS. Паузы чтения нет (пока клиент приостановил чтение,
  входящие ждут в транспорте), полузакрытия нет (после кадра закрытия сервера ничего не пишется); нештатное закрытие (код
  1006 и др.) — `MqttError.Io(kind = "websocket")`, штатное — конец потока.

```kotlin
// jsMain, браузер
val client = MqttClient(
    MqttClientOptions(
        host = "broker.example.org",
        port = 8084,
        webSocket = MqttWebSocket(path = "/mqtt"), // в браузере без заголовков
        tls = TLSClientSettings(), // wss:// с проверками браузера; без него — ws://
    )
)
```

Проверено только на Node.js (`jsNodeTest`: `NodeWebSocketHostContractTest`, `BrowserHostContractTest` — браузерный хост на
собственном WHATWG `WebSocket` Node, — `JsWebSocketTransportTest`); браузерного раннера у форка нет (JS-таргет собирает
только Node.js, Karma не подключена), поэтому тест в настоящем браузере — задокументированный SKIP. Старый код
`kmqtt-common` и `kmqtt-client` импортирует модули `node:*` при загрузке, поэтому в браузерной сборке их надо
заглушить; готовые заглушки и конфигурация webpack 5 (`resolve.fallback`) лежат в
[`js/browser-stubs`](js/browser-stubs/README.md), а `js/browser-stubs/smoke-node-loader.js` проверяет загрузку
«по-браузерному» (заглушенные `node:*`, нет `process.versions.node`) без самого браузера. Сам новый браузерный хост API
Node не зовёт.

### Расширенная аутентификация (MQTT 5)

```kotlin
val client = MqttClient(
    MqttClientOptions(
        host = "broker.example.org",
        port = 8883,
        tls = TLSClientSettings(serverCertificate = "broker.pem"),
        enhancedAuth = MqttEnhancedAuth(
            method = "SCRAM-SHA-256",
            initialData = { scram.clientFirst() }, // вызывается для каждого соединения: каждый раз новый nonce
            respond = { method, data -> scram.answer(data) }, // suspend; чтобы отказаться — бросьте исключение
        ).verifyingConnack { data -> scram.verifyServerFinal(data) }, // необязательно: проверить подпись брокера в CONNACK
    )
)
```

- CONNECT несёт Authentication Method и данные из `initialData` (ничего, если вернуть `null`). Каждый AUTH брокера с
  «продолжить аутентификацию» (0x18) вызывает `respond(method, data)`; его результат уходит обратно в AUTH 0x18 (байты как
  Authentication Data, `null` для AUTH без данных). Обмен может быть любого числа шагов и заканчивается CONNACK; он
  выполняется заново при каждом переподключении.
- `initialData` выполняется в потоке ввода-вывода, поэтому должна быть быстрой. `respond` выполняется в корутине на
  `Dispatchers.Default`, никогда в потоке ввода-вывода; для блокирующей работы переключайте диспетчер сами.
- Исходы: отказ брокера (CONNACK `NOT_AUTHORIZED`, `BAD_AUTHENTICATION_METHOD`, ...) — это `MqttError.Refused(code)`. Если
  `respond` бросает исключение, клиент отправляет DISCONNECT `NOT_AUTHORIZED`, и попытка заканчивается
  `MqttError.Refused(NOT_AUTHORIZED)`; если исключение бросает `initialData`, ничего не отправляется, а исход тот же. Текст
  исключения не сохраняется и не сообщается.
  Весь обмен ограничен `connackTimeout` от CONNECT: молчащий брокер даёт `MqttError.ConnackTimeout`, а `respond`, не
  ответивший вовремя, даёт `Refused(NOT_AUTHORIZED)`, и его поздний ответ отбрасывается. Ответ, вычисленный для соединения,
  которое за это время оборвалось, никогда не уходит в следующем. Затем клиент переподключается по `ReconnectPolicy`, как
  после любого отказа.
- Успешный CONNACK должен нести тот же Authentication Method, иначе соединение заканчивается
  `MqttError.Protocol(PROTOCOL_ERROR)`; так же — AUTH с другим методом или кодом причины и AUTH после CONNACK.
- В MQTT 3.1.1 пакета AUTH нет: `MqttClientOptions(version = MQTT3_1_1, enhancedAuth = ...)` бросает
  `IllegalArgumentException`.
- Подлинность брокера: `MqttEnhancedAuth(...).verifyingConnack { data -> scram.verifyServerFinal(data) }` проверяет
  Authentication Data успешного CONNACK (например, подпись сервера в SCRAM; `data` равно `null`, если брокер ничего не
  прислал). При `false` или исключении клиент отправляет DISCONNECT `NOT_AUTHORIZED`, попытка заканчивается
  `MqttError.Refused(NOT_AUTHORIZED)`, а соединение никогда не считается установленным. Поэтому `Refused(NOT_AUTHORIZED)`
  может означать и отказ самого клиента (провайдер бросил исключение, не ответил вовремя или брокер не прошёл проверку).
  Без `verifyingConnack` данные игнорируются, и обмен брокера не аутентифицирует; для этого полагайтесь на TLS.
- Повторная аутентификация во время сессии (re-authentication, MQTT 5 §4.12.1): `client.reauthenticate()` начинает
  новый обмен на живом соединении — AUTH с причиной «re-authenticate» (0x19), тем же методом и свежими данными
  `initialData`. Вызовы брокера обслуживает `respond`, как обычно; его завершающий AUTH с «success» (0x00) кончает
  обмен (его Authentication Data проверяет `verifyingConnack`, как данные CONNACK), и вызов возвращает `true`.
  Возвращает `false`, если живой сессии нет, другой обмен ещё идёт или соединение кончилось раньше. Срок всего обмена —
  `connackTimeout`. Повторную аутентификацию начинает только клиент: непрошеный AUTH брокера после CONNACK заканчивает
  соединение с `MqttError.Protocol(PROTOCOL_ERROR)`. Без вызовов ничего не отправляется.
- `MqttEnhancedAuth.toString()` и `MqttClientOptions.toString()` показывают только метод, никогда данные.

### Закрытие

`close()` останавливает клиент по порядку:

1. запросы, уже стоящие в очереди (например, публикации, очищающие retained-сообщения), отправляются первыми, по порядку.
   Публикации, удержанные окном (Receive Maximum брокера или `MqttLimits.maxUnacknowledged`, по умолчанию 64, при
   `persistentSession`), записываются по мере подтверждения брокером предыдущих, не дольше
   `MqttClientOptions.closeDrain` (по умолчанию 3 с); без очереди этот шаг времени не занимает;
2. DISCONNECT отправляется, как только записывать больше нечего (или вышел `closeDrain`);
3. клиент продолжает читать и отбрасывать входящие данные, пока брокер не закроет соединение (после DISCONNECT клиента он
   обязан) или не пройдёт `MqttClientOptions.closeLinger` (по умолчанию 3 с);
4. только потом закрывается сокет и останавливается поток ввода-вывода.

Такой порядок избегает сброса TCP: закрытие сокета, в котором остались непрочитанные данные, заставляет операционную систему
послать RST вместо FIN, а брокер, увидевший RST, может потерять хвост очереди, включая DISCONNECT, и опубликовать will.
После штатного закрытия брокер will не публикует никогда.

- Дозапись оставшихся байт (включая TLS `close_notify`) завершается до закрытия транспорта, примерно за секунду. При
  `closeLinger = 0` клиент не ждёт FIN брокера: все стоящие в очереди PUBLISH и DISCONNECT всё равно записываются, но
  соединение может закончиться RST, если у брокера ещё есть данные в пути. Гарантия «без сброса» действует при
  `closeLinger > 0`.
- Публикации, которые не удалось отправить, получают `Failed(Closed)`, в том числе всё ещё удержанные окном, когда
  вышел `closeDrain` (`closeDrain = 0` — всё сверх окна). Публикации после `close()` сразу получают `Failed(Closed)`,
  SUBSCRIBE и UNSUBSCRIBE после него не пишутся.
- Закрытие до прихода CONNACK закрывает транспорт без DISCONNECT, поэтому брокер, уже принявший CONNECT, может опубликовать
  will.
- `close()` не может повиснуть: общее время ограничено `closeDrain` плюс `closeLinger` плюс около 3 секунд, после чего
  поток останавливается принудительно. Второй вызов ждёт того же завершения.
- Пути ошибок, где нечего отправлять (обрыв соединения, таймаут keep-alive, застрявший потребитель), закрывают сокет сразу.
  О нарушении протокола, найденном клиентом в MQTT 5, брокеру сообщается `DISCONNECT` с кодом причины: этот пакет
  записывается первым (примерно за секунду, как хвост штатного закрытия), затем сокет закрывается, не дожидаясь брокера.

### Ограничения

- **Повторную аутентификацию начинает только клиент** (`client.reauthenticate()`, MQTT 5 §4.12.1): клиент отправляет
  AUTH 0x19, непрошеный AUTH брокера после CONNACK — ошибка протокола, см.
  [«Расширенную аутентификацию»](#расширенная-аутентификация-mqtt-5). Authentication Data итогового CONNACK и
  завершающего AUTH брокера можно проверить через `verifyingConnack`.
- **Продолжение сессии включается явно** (`persistentSession`, см. [«Продолжение сессии»](#продолжение-сессии)); состояние
  хранится только в памяти. QoS 2 публикация, отброшенная при продолжении (истёк срок жизни, `resend = false`), может
  оставить свой идентификатор пакета занятым у брокера, который уже прислал PUBREC (Mosquitto 2.1.2 принимает новую
  публикацию с таким идентификатором как новую). Псевдонимы тем (topic aliases) не используются.
- **QoS 2** (отправка и приём, в том числе при продолжении сессии) реализован и проверен: ядро протокола и движок на
  виртуальном времени (`MqttSessionCoreTest`, `MqttSessionCoreResumeTest`, `EnginePersistentSessionTest`) и против
  настоящего Mosquitto 2.1.2 в приложении, которое использует этот форк (Полный обмен PUBLISH-PUBREC-PUBREL-PUBCOMP и дубликаты при потере каждого из четырёх пакетов, досланные
  PUBLISH или PUBREL одним чтением с CONNACK, PUBREL неизвестного идентификатора пакета, занятый идентификатор пакета; MQTT 5 и 3.1.1). Собственные тесты форка со встроенным
  брокером в основном используют QoS 0 и 1.
- **Хосты ввода-вывода только для JVM, Node.js, браузеров (только WebSocket), mingwX64, Linux и Apple** (см.
  **«Платформы»** выше). Хост Apple — POSIX-хост Linux (`PosixHost`: BSD-сокеты и `poll`, `SO_NOSIGPIPE`), а не
  Network.framework, поэтому сотовый модуль и VPN on demand он не поднимает. Работает он только на технике Apple: его
  тесты (`macosArm64Test`, `macosX64Test` и, с Xcode, тесты симуляторов `iosSimulatorArm64Test`, `iosX64Test`,
  `tvosSimulatorArm64Test`, `tvosX64Test`, `watchosSimulatorArm64Test`, `watchosX64Test`) нужно запускать на Mac, на
  других машинах цели Apple только компилируются, а цели устройств только компилируются везде. TLS есть на JVM, Node.js, mingwX64, Linux и Apple (на OpenSSL 3.5 LTS; Apple-klib тоже
  3.5.9); в браузере — только
  `wss://` с проверками самого браузера. WebSocket — на JVM, Node.js и в браузерах, на mingwX64, Linux и Apple нет. Браузерный
  хост проверен на WHATWG `WebSocket` Node, не в настоящем браузере. На Node.js под Windows операционная система принимает запись любого размера сразу (libuv пишет через IOCP), поэтому
  число «ожидающих записи» остаётся 0, а время `closeLinger`/дозаписи тратит ядро, а не хост. Свой хост реализует
  `MqttHost` (а также `HostDriver`, `MqttTransport`, ...); это экспериментальный API с пометкой `@MqttCoreApi`, он может
  измениться; эталон для нового хоста — набор `MqttHostContractTest` в `commonTest`.
- **Адреса на native** (mingwX64, Linux и прочие native-цели): имена разрешаются с `AF_UNSPEC`, IPv4-адрес
  предпочтительнее, IPv6 берётся, если IPv4-адреса у имени нет или это литерал IPv6 (`::1`, в том числе в скобках,
  `[::1]`); семейство сокета — по адресу. Так работают хосты `WinSockHost` и `PosixHost` и сокеты старого `MQTTClient`
  (`ClientSocket`, `TLSClientSocket`). Native-брокер теперь слушает `Broker.host` (по умолчанию `0.0.0.0` — все адреса
  IPv4, как раньше; `::` — все адреса IPv6 и IPv4, `IPV6_V6ONLY` сбрасывается явно). UDP (MQTT-SN) и исходящие
  соединения кластера у native-брокера остаются только IPv4. Перехода к следующему адресу имени нет: берётся первый
  предпочтительный.
- Параметр `clock` общей фабрики принимается, но время берётся из цикла хоста.
- Клиент, закрывшийся сам, держит свой поток, пока не вызван `close()`.

### Безопасность

- Пароли, данные аутентификации, данные и тема will, темы, фильтры и адреса никогда не попадают в `toString()` публичных
  типов, в `MqttError` и в события `MqttLogger`. `MqttError.Io` печатает только имя класса причины.
- Память ограничена `MqttLimits` (размер пакета, длина темы, входящий буфер, число фильтров, публикации в очереди запросов) и
  `OfflineQueue.capacity`; нарушение даёт типизированный исход, а не `OutOfMemoryError`.
- Клиент никогда не переходит с TLS на открытый текст и проверяет имя хоста, если вы явно не отключили проверку.
- О `checkServerCertificate` и `TLSClientSettings.toString()` см. [«TLS»](#tls-jvm).

### Тестирование вашего кода

- Движок и фасад в тестах этого модуля работают на виртуальном времени с транспортом и хостом в памяти (`FakeTransport`,
  `VirtualHost` в `kmqtt-client/src/jvmTest`, на `kotlinx-coroutines-test`). Сейчас они `internal` для тестов модуля и не
  публикуются; скопируйте их или используйте настоящий брокер.
- Для интеграционных тестов запускайте собственный брокер форка в потоке (`io.github.davidepianca98.mqtt.broker.Broker`,
  модуль `kmqtt-broker`) и подключайтесь к `127.0.0.1`; см. `EmbeddedBrokerSupport.kt` и `MqttClientBrokerTest.kt` в
  `kmqtt-client/src/jvmTest`. Тесты TLS используют тот же брокер с `TLSSettings`, тесты WebSocket — с `webSocketPort` (при
  `TLSSettings` этот порт говорит `wss`), см. `WebSocketBrokerTest.kt`. Для тестов расширенной аутентификации брокеру дают
  `enhancedAuthenticationProviders`, см. `EnhancedAuthBrokerTest.kt`.
- Тесты модуля запускайте из корня форка: `./gradlew :kmqtt-client:jvmTest` (один класс: `--tests '*ReadmeSnippetsTest'`);
  когда форк подключён в более крупную сборку, путь начинается с имени подключённой сборки, например
  `:kmqtt:kmqtt-client:jvmTest`. Контракт хоста и хосты Node/браузера/WinSock/POSIX проверяют `jsNodeTest` (Gradle скачивает Node.js),
  `mingwX64Test` (на Windows), `linuxX64Test` (на Linux x64; на других машинах только компилируется, `linuxArm64` —
  только компиляция), `macosArm64Test`/`macosX64Test` (на Mac этой архитектуры) и тесты симуляторов
  (`iosSimulatorArm64Test`, `iosX64Test`, `tvosSimulatorArm64Test`, `tvosX64Test`, `watchosSimulatorArm64Test`,
  `watchosX64Test`) на Mac с Xcode; на других машинах они только компилируются (например
  `compileTestKotlinIosSimulatorArm64`), цели устройств — только компиляция.
- **Тесты Linux в Docker** (с Windows или macOS): поднять одноразовый контейнер `eclipse-temurin:21-jdk-jammy`, скопировать туда
  форк (смонтирован только для чтения) и запустить собственный `gradlew` форка (Kotlin/Native и Gradle-кэш можно оставить в
  именованных томах), например:

  ```bash
  docker run --rm -v "$PWD:/src:ro" -v kn-gradle:/root/.gradle -v kn-konan:/root/.konan eclipse-temurin:21-jdk-jammy \
    bash -c 'apt-get update -qq && apt-get install -y -qq libncurses5 >/dev/null && mkdir /work && cp -r /src/. /work &&
      cd /work && sed -i "s/\r$//" gradlew && chmod +x gradlew &&
      ./gradlew --no-daemon --console=plain :kmqtt-common:linuxX64Test :kmqtt-client:linuxX64Test'
  ```

#### Как проверить то, что сказано в этом документе

| Возможность | Тест (всё в `kmqtt-client/src/jvmTest/kotlin/io/github/davidepianca98/client`, если не сказано иное) |
|---|---|
| Примеры из этого документа | `ReadmeSnippetsTest` |
| Публикация, результаты, офлайн-очередь | `MqttClientBrokerTest`, `MqttClientFacadeTest`, `core/MqttSessionCoreTest` |
| Группы подписок, отказы, недопустимые фильтры | `engine/EngineSubscriptionsTest`, `engine/TopicFilterTest` |
| Переподключение | `engine/EngineReconnectTest`, `MqttClientVirtualTest` |
| Обратное давление, `ConsumerStalled`, события `MqttLogger` | `internal/IncomingGateTest`, `internal/IncomingGateStressTest`, `MqttClientFacadeTest` |
| Продолжение сессии, `resend` | `PersistentSessionBrokerTest`, `engine/EnginePersistentSessionTest`, `core/MqttSessionCoreResumeTest` |
| QoS 2 | `core/MqttSessionCoreTest`, `core/MqttSessionCoreResumeTest` |
| TLS, закреплённый сертификат, виды отказа, кеш `SSLContext` | `TlsBrokerTest`, `TlsTransportTest` |
| Текст PEM или путь | `commonTest/.../client/PemInputTest`, `TlsTransportTest` |
| WebSocket | `WebSocketBrokerTest`, `WebSocketTransportTest`; JS: `JsWebSocketTransportTest`, `NodeWebSocketHostContractTest`, `BrowserHostContractTest` |
| Расширенная аутентификация, `verifyingConnack`, повторная аутентификация | `EnhancedAuthBrokerTest`, `core/MqttSessionCoreAuthTest`, `engine/EngineAuthTest`, `MqttClientReauthTest` |
| Штатное закрытие | `GracefulCloseTest` |
| Хосты (JVM, Node.js, браузер, mingwX64, Linux, Apple) | `NioActorHostContractTest`, `NioActorHostTest`; `jsTest`: `NodeHostContractTest`, `NodeHostTest`, `NodeTlsTransportTest`, `NodeWebSocketHostContractTest`, `BrowserHostContractTest`; `mingwX64Test`: `WinSockHostContractTest`, `WinSockHostTest`; `posixHostTest` (запускается как `linuxX64Test` на Linux и `macosArm64Test`/`macosX64Test` на macOS): `PosixHostContractTest`, `PosixHostTest`, `Ipv6LoopbackTest`; TLS на mingwX64, Linux и Apple: `NativeTlsSessionTest`, `NativeTlsTransportTest`, `NativeTlsTestbedTest` |
| WebSocket встроенного брокера | `kmqtt-broker/src/jvmTest/.../integration/BrokerSocketFixesTest` |

### Отличия от блокирующего `MQTTClient`

| | `MQTTClient` | `MqttClient` |
|---|---|---|
| Потоки | вы вызываете `run()`/`step()` и блокируете поток | свой поток ввода-вывода, `suspend` API |
| Данные | `UByteArray` | `ByteArray` |
| Переподключение | вручную | встроено, с политикой |
| Подписки | вызовы `subscribe`/`unsubscribe` | декларативные группы, восстанавливаются после переподключения |
| Результат публикации | нет | `Sent`/`Delivered`/`Expired`/`Failed` |
| Входящие | обратный вызов | упорядоченный `Flow` с обратным давлением и эпохами сессий |
| Хосты | все платформы | JVM (в том числе Android), Node.js, браузеры (только WebSocket), mingwX64, Linux, Apple |
| WebSocket | да | да (JVM, Node.js, браузеры) |
| QoS 2, продолжение сессии | состояние сессии в объекте клиента | включается явно через `persistentSession`, в памяти |
| Расширенная аутентификация | да (с повторной аутентификацией) | да (MQTT 5, без повторной аутентификации) |

## Старый блокирующий клиент (`MQTTClient`)

> **Старый (legacy).** Этот раздел описывает блокирующий `MQTTClient` (`io.github.davidepianca98.MQTTClient`). Он остаётся
> как есть (его всё ещё используют тесты самого брокера) и не развивается. Новому коду на JVM, Android, Node.js, Windows,
> Linux и Apple нужен [корутинный клиент](#корутинный-клиент-iogithubdavidepianca98clientmqttclient), описанный выше
> (только в форке).
>
> Maven-координаты ниже — это координаты релиза upstream `1.0.0`. В сборках этого форка они подменяются проектами самого
> форка, см. [«Начало работы с корутинным клиентом»](#начало-работы-с-корутинным-клиентом); у upstream `1.0.0` другой API
> (нет `autoInit`, `onUnsubscribed`, `init`).

### Библиотека

#### Gradle

Если появляется ошибка, что OpenSSL не найден, скопируйте нужный файл из https://github.com/davidepianca98/KMQTT/tree/master/kmqtt-common/src/nativeInterop в главный каталог вашего проекта.

##### Плагин Kotlin Multiplatform
С плагином Kotlin Multiplatform достаточно указать зависимость в общем наборе исходников, платформенные части подключатся автоматически.
```gradle
repositories {
    mavenCentral()
}

kotlin {
    jvm()
    mingwX64()
    macosX64()

    sourceSets {
        commonMain {
            dependencies {
                implementation("io.github.davidepianca98:kmqtt-common:1.0.0")
                implementation("io.github.davidepianca98:kmqtt-client:1.0.0")
            }
        }
    }
}
```

##### Проект для одной платформы
```gradle
repositories {
    mavenCentral()
}
dependencies {
    implementation("io.github.davidepianca98:kmqtt-common-jvm:1.0.0")
    implementation("io.github.davidepianca98:kmqtt-client-jvm:1.0.0")
}
```

Замените jvm на js, linuxx64, linuxarm64, mingwx64, macosx64, macosarm64, iosarm64, iosX64, iossimulatorarm64, tvossimulatorarm64, tvosx64, tvosarm64, watchosarm32, watchosarm64, watchosx64, watchossimulatorarm64 в зависимости от нужной платформы.

#### Быстрый пример
Этот код запускает клиент MQTT на порту 1883 без шифрования TLS. Остальные настройки задаются параметрами конструктора MQTTClient()
```kotlin
fun main() {
    val client = MQTTClient(
        MQTTVersion.MQTT5,
        "test.mosquitto.org",
        1883,
        null
    ) {
        println(it.payload?.toByteArray()?.decodeToString())
    }
    client.subscribe(listOf(Subscription("/randomTopic", SubscriptionOptions(Qos.EXACTLY_ONCE))))
    client.publish(false, Qos.EXACTLY_ONCE, "/randomTopic", "hello".encodeToByteArray().toUByteArray())
    client.run() // Блокирующий метод; если поток блокировать нельзя, используйте step()
}
```

#### Пример с TLS
Сертификаты и ключ должны быть в формате PEM; это может быть путь к файлу PEM или сама строка PEM (на JVM и JS строка, в которой есть `-----BEGIN `, — всегда текст PEM, а не путь; на native-платформах осталась прежняя проверка). Пароль может быть null, если закрытый ключ не защищён.
```kotlin
fun main() {
    val client = MQTTClient(
        MQTTVersion.MQTT5,
        "test.mosquitto.org",
        8883,
        TLSClientSettings(
            serverCertificate = "mosquitto.org.crt",
        )
    ) {
        println(it.payload?.toByteArray()?.decodeToString())
    }
    client.subscribe(listOf(Subscription("/randomTopic", SubscriptionOptions(Qos.EXACTLY_ONCE))))
    client.publish(false, Qos.EXACTLY_ONCE, "/randomTopic", "hello".encodeToByteArray().toUByteArray())
    client.run() // Блокирующий метод; если поток блокировать нельзя, используйте step()
}
```

## Начало работы с брокером

### Исполняемые файлы
Исполняемые файлы для вашей платформы можно скачать на вкладке релизов.

#### Аргументы программы

|     Аргумент      | Значение по умолчанию |                                                       Описание                                                       |
|:-----------------:|:---------------------:|:--------------------------------------------------------------------------------------------------------------------:|
|        -h         |        0.0.0.0        |                                          Адрес интерфейса, к которому привязывается сервер                           |
|        -p         |         1883          |                                                  Порт, который слушает сервер                                         |
| --max-connections |          128          |                                      Максимальное число поддерживаемых TCP-соединений                                |
|    --key-store    |         null          |   Путь к хранилищу PKCS12 с закрытым ключом и сертификатом для TLS; если null, TLS выключен                          |
|  --key-store-psw  |         null          | Пароль хранилища PKCS12 из --key-store; если у хранилища нет пароля, параметр не указывайте                           |
|       --wsp       |         null          |                                              Порт WebSocket, который слушает сервер                                   |

### Библиотека

#### Gradle

Если появляется ошибка, что OpenSSL не найден, скопируйте нужный файл из https://github.com/davidepianca98/KMQTT/tree/master/kmqtt-common/src/nativeInterop в главный каталог вашего проекта.

##### Плагин Kotlin Multiplatform
С плагином Kotlin Multiplatform достаточно указать зависимость в общем наборе исходников, платформенные части подключатся автоматически.
```gradle
repositories {
    mavenCentral()
}

kotlin {
    jvm()
    mingwX64()
    macosX64()
    sourceSets {
        commonMain {
            dependencies {
                implementation("io.github.davidepianca98:kmqtt-common:1.0.0")
                implementation("io.github.davidepianca98:kmqtt-broker:1.0.0")
            }
        }
    }
}
```

##### Проект для одной платформы
```gradle
repositories {
    mavenCentral()
}
dependencies {
    implementation("io.github.davidepianca98:kmqtt-common-jvm:1.0.0")
    implementation("io.github.davidepianca98:kmqtt-broker-jvm:1.0.0")
}
```

Замените jvm на js, linuxx64, linuxarm64, macosarm64, macosx64, mingwx64 в зависимости от нужной платформы.

#### Быстрый пример
Этот код запускает брокер MQTT на порту 1883 без шифрования TLS. Остальные настройки задаются параметрами конструктора Broker()
```kotlin
fun main() {
    Broker().listen()
}
```

#### Пример с TLS
Хранилище ключей должно быть в формате PKCS12, пароль хранилища может быть null
```kotlin
fun main() {
    val broker = Broker(
        tlsSettings = TLSSettings(keyStoreFilePath = "keyStore.p12", keyStorePassword = "password"),
        port = 8883
    )
    broker.listen() // Блокирующий метод; если поток блокировать нельзя, используйте step()
}
```

#### Пример аутентификации
```kotlin
fun main() {
    val broker = Broker(authentication = object : Authentication {
        override fun authenticate(clientId: String, username: String?, password: UByteArray?): Boolean {
            // TODO Реализуйте свой способ аутентификации
            return username == "user" && password?.toByteArray()?.decodeToString() == "pass"
        }
    })
    broker.listen() // Блокирующий метод; если поток блокировать нельзя, используйте step()
}
```

#### Пример авторизации

```kotlin
fun main() {
    val broker = Broker(authorization = object : Authorization {
        override fun authorize(
            clientId: String,
            username: String?,
            password: UByteArray?, // != null только если в конструкторе брокера savePassword = true
            topicName: String,
            isSubscription: Boolean,
            payload: UByteArray?
        ): Boolean {
            // TODO Реализуйте свой способ авторизации
            return topicName == "$clientId/topic"
        }
    })
    broker.listen() // Блокирующий метод; если поток блокировать нельзя, используйте step()
}
```

#### Пример перехватчика сообщений

```kotlin
fun main() {
    val broker = Broker(packetInterceptor = object : PacketInterceptor {
        override fun packetReceived(clientId: String, username: String?, password: UByteArray?, packet: MQTTPacket) {
            when (packet) {
                is MQTTConnect -> println(packet.protocolName)
                is MQTTPublish -> println(packet.topicName)
            }
        }
    })
    broker.listen() // Блокирующий метод; если поток блокировать нельзя, используйте step()
}
```

#### Пример публикации внутри брокера
```kotlin
fun main() {
    val broker = Broker()
    broker.publish(
        retain = false,
        topicName = "test/",
        qos = Qos.AT_MOST_ONCE,
        properties = MQTT5Properties(),
        payload = "testPayload".encodeToByteArray().toUByteArray()
    )
    broker.listen() // Блокирующий метод; если поток блокировать нельзя, используйте step()
}
```

#### WebSocket

Задайте параметр конструктора `webSocketPort` (параметр командной строки `--wsp`), чтобы брокер принимал MQTT поверх
WebSocket на втором порту, рядом с обычным. С `tlsSettings` этот порт говорит `wss`, без него — `ws`.

```kotlin
fun main() {
    val broker = Broker(port = 1883, webSocketPort = 8083)
    broker.listen() // ws://host:8083: клиент должен предложить подпротокол mqtt; путь запроса не проверяется
}
```

Слой WebSocket следует RFC 6455: учитываются бит FIN и кадры продолжения (фрагментированное сообщение
склеивается в поток байт MQTT), pong игнорируется, на кадр закрытия брокер отвечает тем же кодом состояния, а когда
закрывает сам, шлёт правильный кадр из двух байт `0x88 0x00`. Ненулевой бит RSV, кадр продолжения без начатого сообщения,
кадр данных внутри фрагментированного сообщения, фрагментированный или длиннее 125 байт управляющий кадр закрывают соединение
с ошибкой. Проверяют `BrokerSocketFixesTest` (сырые клиенты WebSocket и TCP) и, вместе с корутинным клиентом,
`WebSocketBrokerTest`.

#### Отказ во входе

Когда `authentication` (или сам брокер по другой причине) отказывает в CONNECT клиенту MQTT 3.1.1, клиент получает CONNACK с
кодом возврата (не авторизован, неверное имя пользователя или пароль, неприемлемая версия протокола, идентификатор отклонён,
сервер недоступен), а не оборванный сокет. Клиенты MQTT 5 получают CONNACK с кодом причины, как и раньше. Прочие
ошибки протокола 3.1.1 по-прежнему закрывают сокет без CONNACK, как требует спецификация.

#### Остановка

`broker.stop(serverReference = null, temporarilyMoved = false)` отправляет DISCONNECT подключённым клиентам MQTT 5
(`SERVER_SHUTTING_DOWN`; при заданном `serverReference` — `SERVER_MOVED`, а при `temporarilyMoved = true` —
`USE_ANOTHER_SERVER`) и закрывает слушающие сокеты. Отдельная программа вызывает его из shutdown hook. Если вы запускаете
брокер через `step()` в своём потоке (как тесты клиента), вызывайте `stop()` из того же потока.

#### Другие расширенные возможности

Расширенная аутентификация MQTT 5: задайте параметр конструктора Broker `enhancedAuthenticationProviders`, реализовав
интерфейс провайдера `EnhancedAuthenticationProvider`.

Сохранение сессий: задайте параметр конструктора Broker `persistence`, реализовав интерфейс `Persistence`. Без него сессии
живут только в памяти: после перезапуска брокера клиент, продолжающий сессию, получит Session Present = 0.

Метрики байтов: задайте параметр конструктора Broker `bytesMetrics`, реализовав интерфейс `BytesMetrics`.
