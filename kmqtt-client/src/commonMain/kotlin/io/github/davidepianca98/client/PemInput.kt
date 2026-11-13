package io.github.davidepianca98.client

import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

/**
 * Ошибка разбора PEM, переданного строкой. Текст не содержит самой строки: в ней сертификат или ключ.
 */
internal class InvalidPemException(message: String) : IllegalArgumentException(message)

private const val PEM_BEGIN = "-----BEGIN "
private const val PEM_DASHES = "-----"
private const val PEM_LINE = 64

/**
 * Разделяет «PEM-содержимое» и «путь к файлу» для полей `TLSClientSettings` (`serverCertificate`,
 * `clientCertificate`, `clientCertificateKey`) без угадывания по строгому `isValidPem()`.
 *
 * - Строка без `-----BEGIN ` — путь к файлу: возвращается `null`, вызывающий открывает файл как раньше.
 * - Строка с `-----BEGIN ` — всегда PEM-содержимое и никогда не путь: возвращается PEM, приведённый к виду
 *   `BEGIN`/строки по 64 символа/`END` с переводом строки в конце. Допускаются лишние пробелы, отступы, CRLF и
 *   перенос на любую ширину (76, 64…). Не PEM (нет парного `END`, текст до `BEGIN` или после `END`, второй блок,
 *   символы вне base64, пустое или некорректное тело) — [InvalidPemException], к файловой системе обращения нет.
 */
@OptIn(ExperimentalEncodingApi::class)
internal fun inlinePemOrNull(value: String): String? {
    if (!value.contains(PEM_BEGIN)) return null
    val text = value.trim()
    if (!text.startsWith(PEM_BEGIN)) throw InvalidPemException("PEM: text before BEGIN")
    val labelEnd = text.indexOf(PEM_DASHES, startIndex = PEM_BEGIN.length)
    if (labelEnd < 0) throw InvalidPemException("PEM: BEGIN line is not closed")
    val label = text.substring(PEM_BEGIN.length, labelEnd)
    if (label.isEmpty() || label.any { !(it in 'A'..'Z' || it in '0'..'9' || it == ' ') }) {
        throw InvalidPemException("PEM: bad label")
    }
    val end = "-----END $label-----"
    if (!text.endsWith(end)) throw InvalidPemException("PEM: no matching END or text after END")
    val bodyStart = labelEnd + PEM_DASHES.length
    val bodyEnd = text.length - end.length
    if (bodyEnd < bodyStart) throw InvalidPemException("PEM: no matching END or text after END")
    val body = text.substring(bodyStart, bodyEnd).filterNot { it == ' ' || it == '\t' || it == '\r' || it == '\n' }
    if (body.isEmpty()) throw InvalidPemException("PEM: empty body")
    if (body.any { !(it in 'A'..'Z' || it in 'a'..'z' || it in '0'..'9' || it == '+' || it == '/' || it == '=') }) {
        throw InvalidPemException("PEM: body is not base64")
    }
    try {
        Base64.Default.decode(body)
    } catch (_: IllegalArgumentException) {
        throw InvalidPemException("PEM: body is not base64")
    }
    return buildString {
        append(PEM_BEGIN).append(label).append(PEM_DASHES).append('\n')
        body.chunked(PEM_LINE).forEach { append(it).append('\n') }
        append(end).append('\n')
    }
}
