@file:JsModule("node:crypto")
@file:JsNonModule

package io.github.davidepianca98.client.node.crypto

/** `crypto.X509Certificate` Node: разбор PEM сертификата (исключение — неверный PEM), SEVEN-250. */
internal external class X509Certificate(buffer: dynamic)

/** `crypto.createPrivateKey(options)` Node: разбор PEM ключа (исключение — неверный ключ или пароль). */
internal external fun createPrivateKey(options: dynamic): dynamic
