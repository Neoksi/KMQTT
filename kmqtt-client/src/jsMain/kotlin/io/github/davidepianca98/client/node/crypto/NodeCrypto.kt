@file:JsModule("node:crypto")
@file:JsNonModule

package io.github.davidepianca98.client.node.crypto

/** `crypto.X509Certificate` Node: разбор PEM сертификата (исключение — неверный PEM). */
internal external class X509Certificate(buffer: dynamic)

/** `crypto.createPrivateKey(options)` Node: разбор PEM ключа (исключение — неверный ключ или пароль). */
internal external fun createPrivateKey(options: dynamic): dynamic

/** `crypto.createHash(algorithm)` Node: `Sec-WebSocket-Accept` (SHA-1, RFC 6455 §4.2.2). */
internal external fun createHash(algorithm: String): dynamic

/** `crypto.randomFillSync(buffer)` Node: ключ `Sec-WebSocket-Key` и маски кадров WebSocket. */
internal external fun randomFillSync(buffer: dynamic): dynamic
