package io.github.davidepianca98.client

import io.github.davidepianca98.client.core.MqttCoreApi
import io.github.davidepianca98.client.node.setImmediate

/**
 * Хост клиента для Node.js: цикл клиента — обороты на цикле событий Node,
 * своего потока нет (в Node он один; блокирующих вызовов нет ни у хоста, ни у [NodeTransport]).
 * Транспорт — TCP, TLS ([NodeTransportFactory.Companion.tls]) или WebSocket поверх них
 * ([NodeTransportFactory.Companion.webSocket]). Сам цикл — [JsEventLoopHost], общий с браузерным хостом.
 *
 * Браузер: сырых сокетов там нет, только WebSocket API — свой хост [BrowserHost].
 */
@OptIn(MqttCoreApi::class)
public class NodeHost internal constructor(
    clock: MqttClock,
    transportFactory: NodeTransportFactory
) : MqttHost by JsEventLoopHost(
    clock,
    { wakeup -> transportFactory.create(NodeTransportContext(wakeup)) },
    { callback -> setImmediate(callback) }
) {

    /** Хост с TCP-транспортом. [clock] должен совпадать с часами драйвера. */
    public constructor(clock: MqttClock = MqttClock.Monotonic) : this(clock, NodeTransportFactory.Tcp)
}
