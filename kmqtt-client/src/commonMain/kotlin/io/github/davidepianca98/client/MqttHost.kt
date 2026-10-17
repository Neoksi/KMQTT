package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import kotlin.time.TimeSource

/**
 * Clock of the client: monotonic milliseconds. All deadlines of the host and the engine ([HostStep.nextDeadlineMs],
 * [HostAction.Connect.timeoutMs]) are measured on the scale of this clock, starting from the value `nowMs` that the host
 * passes to [HostDriver.runOnce].
 */
public interface MqttClock {
    /** Current monotonic time in milliseconds. The origin is unspecified: only values of the same clock are comparable. */
    public fun nowMs(): Long

    public companion object {
        /** The process-wide monotonic clock (`kotlin.time.TimeSource.Monotonic`, one origin per process). */
        public val Monotonic: MqttClock get() = MonotonicClock
    }
}

private object MonotonicClock : MqttClock {
    private val origin = TimeSource.Monotonic.markNow()

    override fun nowMs(): Long = origin.elapsedNow().inWholeMilliseconds
}

/**
 * Событие транспорта для [HostDriver.runOnce]. События относятся к текущему транспорту хоста: после
 * [HostAction.CloseTransport], [HostAction.FlushAndClose] или нового [HostAction.Connect] события прежнего
 * транспорта не доставляются.
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

    /**
     * Закрыть текущий транспорт сразу (неотправленный остаток записи отбрасывается). Без транспорта — ничего.
     * Для пути ошибок (транспорт мёртв, потребитель завис); штатное закрытие после записи — [FlushAndClose].
     */
    public data object CloseTransport : HostAction

    /**
     * Дописать, затем закрыть (S06b-T05f): текущий транспорт перестаёт быть текущим (как после [CloseTransport]:
     * его события драйверу больше не доставляются, `Write`/`PauseRead` к нему не относятся, новый [Connect] его не
     * трогает), но канал закрывается только когда очередь записи опустела (`MqttTransport.pendingWriteBytes == 0`)
     * или истёк срок [timeoutMs] (по часам хоста от момента действия; остаток тогда отбрасывается). Пока идёт
     * дописывание, чтение включено, а прочитанное отбрасывается — чтобы закрытие с непрочитанными входящими не дало
     * RST (H1). Перед дописыванием хост вызывает `MqttTransport.closeOutput` (у TLS — close_notify в очередь за
     * данными, он уходит последней записью). Пустая очередь — закрытие
     * сразу, как [CloseTransport]. [MqttHost.close] сначала доводит такие транспорты (не дольше их сроков).
     * Без транспорта — ничего.
     */
    public data class FlushAndClose(val timeoutMs: Long) : HostAction
}

/**
 * Транспорт одного подключения (TCP, TLS поверх TCP…), которым владеет хост. Все методы — только на потоке хоста;
 * результаты — событиями ([events]), чтение — тоже событиями ([TransportEvent.Bytes]): отдельного `read` нет,
 * готовность сокета обслуживает хост своей платформенной частью (на JVM — `NioTransport` и селектор, на Node.js — события `net.Socket`, на mingwX64 — `select` WinSock).
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

    /**
     * Запись закончена (S06b-T05f): поставить в очередь записи маркер конца потока протокола транспорта, если он есть
     * (у TLS — close_notify), чтобы он ушёл после всех данных; дальнейшие [write] не ожидаются. Хост вызывает это при
     * [HostAction.FlushAndClose] перед ожиданием `pendingWriteBytes == 0`, затем [close]. По умолчанию — ничего
     * (у TCP конец потока — FIN при [close]).
     */
    public fun closeOutput() {}

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
 * Host of the client loop: a thread (or another executor) that runs the [HostDriver] and performs the I/O. Single-use:
 * [start] is called once and the host cannot be restarted after [close]. The implementations are `NioActorHost` (JVM),
 * `NodeHost` (JS, Node.js event loop, no thread of its own) and `WinSockHost` (mingwX64); each is created by the
 * platform factory `MqttClient(options)` and passes the same contract suite (`MqttHostContractTest`, commonTest).
 * Implementing a custom host is experimental: the driver and transport contracts are marked `@MqttCoreApi` and may
 * change.
 */
public interface MqttHost {
    /** Starts the loop with the driver. A second call, or a call after [close], throws [IllegalStateException]. */
    @MqttCoreApi
    public fun start(driver: HostDriver)

    /**
     * Wakes the loop: the next [HostDriver.runOnce] runs without waiting for a deadline. Non-blocking, callable from any
     * thread; a wake-up that arrives before the loop sleeps is not lost. Does nothing before [start] and after [close].
     */
    public fun wakeup()

    /**
     * Stops the loop: [HostDriver.runOnce] is no longer called, the current transport is closed at once (a connection in
     * progress is aborted), transports handed over with [HostAction.FlushAndClose] are flushed for no longer than their
     * own deadlines, then the thread ends and the selector is closed. Suspends until the thread has exited but for at
     * most [timeoutMs]. Returns `true` when the host stopped completely and `false` when the timeout elapsed (the thread
     * is still alive). A repeated call is harmless. The orderly close of the MQTT session (DISCONNECT, waiting for EOF)
     * is the driver's job and happens before [close].
     */
    public suspend fun close(timeoutMs: Long = 5_000): Boolean
}
