package io.github.davidepianca98.client.core

/**
 * Маркер внутреннего API нового клиента (ядро, фрейминг, движок).
 *
 * Объявления публичны из-за `explicitApi` и раскладки по пакетам, но приложению не нужны: оно работает через фасад
 * `io.github.davidepianca98.client.MqttClient`. Использование вне форка требует явного `@OptIn(MqttCoreApi::class)`.
 */
@RequiresOptIn(
    level = RequiresOptIn.Level.ERROR,
    message = "Ядро/движок нового клиента: внутренний API, приложению не нужен",
)
@Retention(AnnotationRetention.BINARY)
@Target(
    AnnotationTarget.CLASS,
    AnnotationTarget.FUNCTION,
    AnnotationTarget.PROPERTY,
    AnnotationTarget.TYPEALIAS,
)
public annotation class MqttCoreApi
