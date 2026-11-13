@file:JsModule("node:tls")
@file:JsNonModule

package io.github.davidepianca98.client.node.tls

/** `tls.connect(options)` Node: `tls.TLSSocket`. */
internal external fun connect(options: dynamic): dynamic

/** `tls.createServer(options, listener)` Node — для тестов. */
internal external fun createServer(options: dynamic, secureConnectionListener: (socket: dynamic) -> Unit): dynamic
