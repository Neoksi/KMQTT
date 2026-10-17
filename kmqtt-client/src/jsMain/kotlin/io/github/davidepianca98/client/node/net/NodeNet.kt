@file:JsModule("node:net")
@file:JsNonModule

package io.github.davidepianca98.client.node.net

/** `net.connect(options)` Node: `net.Socket` (SEVEN-250). */
internal external fun connect(options: dynamic): dynamic

/** `net.createServer(options, listener)` Node — для тестов. */
internal external fun createServer(options: dynamic, connectionListener: (socket: dynamic) -> Unit): dynamic
