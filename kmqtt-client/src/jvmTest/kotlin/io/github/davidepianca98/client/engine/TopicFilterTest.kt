package io.github.davidepianca98.client.engine

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** допустимость фильтров подписки (MQTT 3.1.1 / 5 §4.7, общие подписки §4.8). */
class TopicFilterTest {

    @Test
    fun validFilters() {
        val valid = listOf(
            "a", "a/b", "/", "/a", "a/", "//", "#", "+", "a/#", "a/+", "+/a", "+/+", "/+/#", "a/+/b/#",
            "\$SYS/#", "\$SYS/broker/+", "\$share/g/a", "\$share/g/a/+/#", "\$share/g/#", "\$share/g/+",
            "\$share", "\$sharex/a", "a b", "привет/#", "\$share/группа/a", "\$share/g/\$share/h"
        )
        for (filter in valid) assertTrue(isValidTopicFilter(filter), "valid: '$filter'")
    }

    @Test
    fun invalidFilters() {
        val invalid = listOf(
            "", "a/#/b", "a#", "#a", "##", "a/##", "a/b#", "#/a", "a+", "+a", "a/+b", "a/b+/c", "a/#/",
            "\$share/", "\$share/g", "\$share//a", "\$share/g/", "\$share/+/a", "\$share/#/a", "\$share/g+/a",
            "\$share/g/a#", "\$share/g/#/a", "a\u0000b", "\u0000"
        )
        for (filter in invalid) assertFalse(isValidTopicFilter(filter), "invalid: '${filter.replace('\u0000', '0')}'")
    }

    @Test
    fun lengthIsMeasuredInUtf8Bytes() {
        assertTrue(isValidTopicFilter("a".repeat(65_535)))
        assertFalse(isValidTopicFilter("a".repeat(65_536)))
        // 'я' — 2 байта: 32 767 символов = 65 534 байта, 32 768 = 65 536
        assertTrue(isValidTopicFilter("я".repeat(32_767)))
        assertFalse(isValidTopicFilter("я".repeat(32_768)))
        assertEquals(true, isValidTopicFilter("a".repeat(21_845) + "я".repeat(10)))
    }
}
