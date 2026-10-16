package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import kotlin.time.TimeSource

/**
 * Часы клиента: монотонные миллисекунды. Все дедлайны хоста и движка ([HostStep.nextDeadlineMs],
 * [HostAction.Connect.timeoutMs]) считаются в шкале этих часов — от значения `nowMs`, которое хост передаёт в
 * [HostDriver.runOnce].
 */
public interface MqttClock {
    /** Текущее монотонное время, мс. Начало отсчёта не задано: сравнивать можно только значения одних часов. */
    public fun nowMs(): Long

    public companion object {
        /** Монотонные часы процесса (`kotlin.time.TimeSource.Monotonic`, одна отметка на процесс). */
        public val Monotonic: MqttClock get() = MonotonicClock
    }
}

private object MonotonicClock : MqttClock {
    private val origin = TimeSource.Monotonic.markNow()

    override fun nowMs(): Long = origin.elapsedNow().inWholeMilliseconds
}

/**
 * Событие транспорта для [HostDriver.runOnce]. События относятся к текущему транспорту хоста: после
 * [HostAction.CloseTransport] или нового [HostAction.Connect] события прежнего транспорта не доставляются.
 *
 * Тип объявлен здесь (S06b-T04a); движок (S06b-T03a) использует его, а не объявляет свой.
 */
@MqttCoreApi
public sealed interface TransportEvent {
    /** Соединение установлено (TCP; у TLS — после рукопожатия). */
    public data object Connected : TransportEvent

    /** Прочитанные байты. Массив принадлежит получателю (копия, хост его больше не трогает). */
    public class Bytes(public val bytes: ByteArray) : TransportEvent {
        override fun toString(): String = "Bytes(${bytes.size})"
    }

    /**
     * Удалённая сторона закрыла поток (чтение вернуло -1). Канал остаётся открытым, пока драйвер не пришлёт
     * [HostAction.CloseTransport]: так ядро в состоянии `Closing` закрывает сокет только после EOF брокера (H1).
     */
    public data object Eof : TransportEvent

    /**
     * Транспорт не работает: подключение не удалось ([MqttError.UnknownHost], [MqttError.ConnectTimeout],
     * [MqttError.Io]) или ошибка ввода-вывода. Хост уже закрыл канал (пути ошибок закрывают сразу), дальнейших
     * событий этого транспорта не будет; [HostAction.CloseTransport] после него безвреден.
     */
    public data class Failed(val error: MqttError) : TransportEvent
}

/**
 * Действие ввода-вывода, которое драйвер просит выполнить хост ([HostStep.actions]). Действия исполняются по порядку
 * на потоке хоста. Это часть `EngineAction` движка (S06b-T03a), относящаяся к транспорту: фасад (S06b-T05a)
 * переводит `EngineAction` в [HostAction], остальное (выдача входящих, результаты, состояние) исполняет сам.
 */
@MqttCoreApi
public sealed interface HostAction {
    /**
     * Подключиться к [host]:[port]. Прежний транспорт, если есть, закрывается. Резолв имени и TCP-подключение
     * (у TLS — и рукопожатие) должны уложиться в [timeoutMs], иначе [TransportEvent.Failed] с
     * [MqttError.ConnectTimeout]. Результат — [TransportEvent.Connected] или [TransportEvent.Failed].
     */
    public class Connect(public val host: String, public val port: Int, public val timeoutMs: Long) : HostAction {
        // Адрес брокера в toString() не попадает (H11).
        override fun toString(): String = "Connect"
    }

    /**
     * Записать [bytes] после всего, что уже стоит в очереди записи. Массив не копируется: драйвер не должен менять
     * его после передачи. Неполная запись не блокирует поток: остаток дописывается по готовности сокета.
     * Без транспорта (не подключён, уже закрыт или упал) — игнорируется.
     */
    public class Write(public val bytes: ByteArray) : HostAction {
        override fun toString(): String = "Write(${bytes.size})"
    }

    /** Приостановить (`true`) или возобновить (`false`) чтение сокета — обратное давление (W6 §4.5). */
    public data class PauseRead(val paused: Boolean) : HostAction

    /** Закрыть текущий транспорт сразу (неотправленный остаток записи отбрасывается). Без транспорта — ничего. */
    public data object CloseTransport : HostAction
}

/**
 * Транспорт одного подключения (TCP, TLS поверх TCP…), которым владеет хост. Все методы — только на потоке хоста;
 * результаты — событиями ([events]), чтение — тоже событиями ([TransportEvent.Bytes]): отдельного `read` нет,
 * готовность сокета обслуживает хост своей платформенной частью (на JVM — `NioTransport` и селектор).
 *
 * Один экземпляр — одно подключение: после [close] или [TransportEvent.Failed] транспорт не переиспользуется.
 */
@MqttCoreApi
public interface MqttTransport {
    /**
     * Начать подключение (резолв имени — вне потока хоста). [deadlineMs] — срок по часам хоста для всего
     * подключения: по нему [TransportEvent.Failed] с [MqttError.ConnectTimeout].
     */
    public fun connect(host: String, port: Int, deadlineMs: Long)

    /**
     * Поставить [bytes] в очередь записи и записать, сколько возможно, без блокировки. Массив не копируется.
     * До подключения байты ждут в очереди. Возвращает число байтов, записанных сразу.
     */
    public fun write(bytes: ByteArray): Int

    /** Байтов в очереди записи, ещё не отданных ОС. */
    public val pendingWriteBytes: Long

    /** Читать сокет (`true`) или нет (`false`, обратное давление). По умолчанию чтение включено. */
    public fun setReadInterest(enabled: Boolean)

    /** Обработать то, что случилось вне готовности сокета: результат резолва, истечение [connect]-дедлайна. */
    public fun poll(nowMs: Long)

    /** Ближайший собственный дедлайн транспорта (срок подключения) или `null`. */
    public fun nextDeadlineMs(): Long?

    /** Забрать накопленные события (по порядку). */
    public fun events(): List<TransportEvent>

    /** Закрыть канал и освободить ресурсы; идущее подключение прерывается. Повторный вызов безвреден. */
    public fun close()
}

/** Результат одного шага драйвера: действия и ближайший дедлайн (`null` — ждать без срока до события). */
@MqttCoreApi
public class HostStep(
    public val nextDeadlineMs: Long?,
    public val actions: List<HostAction>
) {
    public companion object {
        /** Ничего не делать, ждать события. */
        public val Idle: HostStep = HostStep(null, emptyList())
    }
}

/**
 * Связка «движок ↔ хост»: однопоточный автомат, который хост вызывает на своём потоке. Реализует фасад (S06b-T05a).
 *
 * Хост вызывает [runOnce] на каждом обороте цикла: после [MqttHost.wakeup], готовности сокета, наступления
 * дедлайна, а также после действий, сразу давших события. Лишние вызовы (без событий и до дедлайна) допустимы —
 * драйвер должен быть к ним готов. Команды API драйвер держит в своей очереди (кладут их другие потоки, затем
 * вызывают [MqttHost.wakeup]) и забирает внутри [runOnce].
 */
@MqttCoreApi
public interface HostDriver {
    /**
     * Один шаг: [events] — события транспорта с прошлого шага (по порядку), [nowMs] — время часов хоста.
     * Возвращает действия и ближайший дедлайн. Исключение из [runOnce] останавливает хост
     * (затем [onStopped] с этим исключением).
     */
    public fun runOnce(nowMs: Long, events: List<TransportEvent>): HostStep

    /**
     * Хост остановился: поток завершается, транспорт и селектор закрыты. [cause] — исключение, остановившее цикл
     * (из [runOnce] или самого хоста), `null` — штатный [MqttHost.close]. Вызывается на потоке хоста один раз.
     */
    public fun onStopped(cause: Throwable?) {}
}

/**
 * Хост цикла клиента: поток (или иной исполнитель), который крутит [HostDriver] и исполняет ввод-вывод.
 * Одноразовый: [start] — один раз, после [close] не перезапускается. Реализация по умолчанию — `NioActorHost` (jvm).
 */
public interface MqttHost {
    /** Запустить цикл с драйвером. Повторный вызов или вызов после [close] — `IllegalStateException`. */
    @MqttCoreApi
    public fun start(driver: HostDriver)

    /**
     * Разбудить цикл: ближайший [HostDriver.runOnce] выполнится без ожидания дедлайна. Неблокирующий, из любого
     * потока; пробуждение до `select` не теряется. До [start] и после [close] — ничего.
     */
    public fun wakeup()

    /**
     * Остановить цикл: поток завершается, транспорт и селектор закрываются (идущее подключение прерывается).
     * Приостанавливается до выхода потока, но не дольше [timeoutMs]. Возвращает `true`, если хост остановлен
     * полностью, `false` — истёк таймаут (поток ещё жив). Повторный вызов безвреден. Штатное закрытие MQTT-сессии
     * (DISCONNECT, ожидание EOF) — дело драйвера до вызова [close].
     */
    public suspend fun close(timeoutMs: Long = 5_000): Boolean
}
