package io.github.davidepianca98.client.engine

private const val SHARE_PREFIX = "\$share/"
private const val MAX_STRING_BYTES = 65_535

// 3 байта UTF-8 на символ UTF-16 — верхняя граница: короче этого размер проверять не нужно
private const val MAX_CHARS_WITHOUT_SIZE_CHECK = MAX_STRING_BYTES / 3

/**
 * Фильтр подписки допустим по MQTT 3.1.1 §4.7 и MQTT 5 §4.7/§4.8: не пустой, без U+0000, не длиннее
 * 65 535 байт UTF-8; `#` — только уровень целиком и последний, `+` — только уровень целиком; общая подписка
 * `$share/<имя>/<фильтр>` — с непустым именем без `/`, `+`, `#` и непустым допустимым фильтром после него.
 * Версию протокола не учитывает: `$share/…` проверяется по структуре всегда.
 */
internal fun isValidTopicFilter(filter: String): Boolean {
    if (filter.isEmpty() || filter.indexOf('\u0000') >= 0) return false
    if (filter.length > MAX_CHARS_WITHOUT_SIZE_CHECK && filter.encodeToByteArray().size > MAX_STRING_BYTES) return false
    var body = filter
    if (filter.startsWith(SHARE_PREFIX)) {
        val nameEnd = filter.indexOf('/', SHARE_PREFIX.length)
        if (nameEnd <= SHARE_PREFIX.length) return false
        val name = filter.substring(SHARE_PREFIX.length, nameEnd)
        if (name.indexOf('+') >= 0 || name.indexOf('#') >= 0) return false
        body = filter.substring(nameEnd + 1)
        if (body.isEmpty()) return false
    }
    val levels = body.split('/')
    for ((index, level) in levels.withIndex()) {
        if (level.indexOf('#') >= 0 && (level != "#" || index != levels.lastIndex)) return false
        if (level.indexOf('+') >= 0 && level != "+") return false
    }
    return true
}
