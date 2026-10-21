package io.github.davidepianca98.client

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** строка с `-----BEGIN ` — всегда PEM (приводится к единому виду), без него — путь к файлу. */
class PemInputTest {

    // 100 байт (0..99) -> 136 символов base64: две строки по 64 и хвост из 8
    private val body = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiYw=="
    private val canonical = buildString {
        append("-----BEGIN CERTIFICATE-----\n")
        body.chunked(64).forEach { append(it).append('\n') }
        append("-----END CERTIFICATE-----\n")
    }

    private fun wrapped(width: Int, eol: String = "\n", indent: String = ""): String = buildString {
        append(indent).append("-----BEGIN CERTIFICATE-----").append(eol)
        body.chunked(width).forEach { append(indent).append(it).append(eol) }
        append(indent).append("-----END CERTIFICATE-----").append(eol)
    }

    @Test
    fun canonicalPemStaysTheSame() {
        assertEquals(canonical, inlinePemOrNull(canonical))
        assertEquals(canonical, inlinePemOrNull(canonical.trimEnd()))
    }

    @Test
    fun wrapWidth64And76AreNormalized() {
        assertEquals(canonical, inlinePemOrNull(wrapped(64)))
        assertEquals(canonical, inlinePemOrNull(wrapped(76)))
        assertEquals(canonical, inlinePemOrNull(wrapped(body.length)))
    }

    @Test
    fun indentsCrlfAndSurroundingWhitespaceAreNormalized() {
        assertEquals(canonical, inlinePemOrNull(wrapped(76, indent = "    ")))
        assertEquals(canonical, inlinePemOrNull(wrapped(76, eol = "\r\n")))
        assertEquals(canonical, inlinePemOrNull("\n\n  " + wrapped(64, indent = "\t") + "\n \n"))
    }

    @Test
    fun pathWithoutBeginIsNotPem() {
        assertNull(inlinePemOrNull("/data/certs/ca.pem"))
        assertNull(inlinePemOrNull("C:\\certs\\ca.pem"))
        assertNull(inlinePemOrNull(""))
    }

    @Test
    fun malformedPemIsParseErrorNotAFilePath() {
        val bad = listOf(
            // мусор после END
            canonical + "extra text",
            canonical + canonical,
            // текст до BEGIN
            "ca.pem\n$canonical",
            // нет END, END другого вида, BEGIN не закрыт
            canonical.substringBefore("-----END"),
            canonical.replace("-----END CERTIFICATE-----", "-----END PRIVATE KEY-----"),
            "-----BEGIN CERTIFICATE",
            // пустое тело, не base64, обрезанный base64
            "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----",
            "-----BEGIN CERTIFICATE-----\nnot base64 !!!\n-----END CERTIFICATE-----",
            "-----BEGIN CERTIFICATE-----\nQUJDR\n-----END CERTIFICATE-----",
            // BEGIN в середине строки, похожей на путь
            "/tmp/-----BEGIN CERTIFICATE-----"
        )
        for (value in bad) {
            val e = assertFailsWith<InvalidPemException>(value) { inlinePemOrNull(value) }
            assertTrue(!e.message.orEmpty().contains(body.take(16)), "текст ошибки не содержит данных PEM")
        }
    }
}
