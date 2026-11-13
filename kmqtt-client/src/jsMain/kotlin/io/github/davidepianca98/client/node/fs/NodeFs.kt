@file:JsModule("node:fs")
@file:JsNonModule

package io.github.davidepianca98.client.node.fs

/** `fs.readFileSync(path)` Node: PEM из файла (как путь в `TLSClientSettings` на JVM). */
internal external fun readFileSync(path: String): dynamic
