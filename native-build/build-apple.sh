#!/usr/bin/env bash
# =============================================================================
# Сборка klib OpenSSL 3.5.9 LTS для Apple-целей форка (macOS, iOS, tvOS, watchOS).
#
# Запускать на Mac из КОРНЯ клона форка (там, где лежат kmqtt-common/ и native-build/):
#
#     bash native-build/build-apple.sh
#
# Что делает:
#   1. проверяет окружение (Xcode и SDK каждой платформы, Java, место на диске);
#   2. скачивает OpenSSL (sha256 сверяется) и Kotlin/Native;
#   3. собирает OpenSSL статикой под каждую цель и склеивает libssl.a и libcrypto.a в libopenssl.a:
#        native-build/apple/<цель>/openssl-3.5.9/{include/, libopenssl.a}   (его ждёт openssl.def;
#        каталог промежуточный, в git не хранится);
#   4. проверяет каждую библиотеку: линкует тестовую программу под архитектуру цели, на macOS ещё и запускает;
#   5. собирает klib через cinterop по kmqtt-common/src/nativeInterop/openssl.def и проверяет их;
#   6. кладёт klib в kmqtt-common/src/nativeInterop/ (заменяя прежние).
#
# Цели (имена как в openssl.def):
#   ядро (обязательно; не собралась — код возврата 1):
#     ios-arm64             Configure ios64-xcrun
#     ios-simulator-arm64   Configure iossimulator-arm64-xcrun
#     macos-arm64           Configure darwin64-arm64-cc
#     macos-x64             Configure darwin64-x86_64-cc
#   дополнительные (не собралась — не ошибка, прежний klib остаётся; в OpenSSL 3.5.9 для tvOS и watchOS
#   готовых Configure-таргетов нет, скрипт сам описывает их в Configurations/90-kmqtt.conf):
#     ios-x64               Configure iossimulator-x86_64-xcrun
#     tvos-arm64, tvos-simulator-arm64, tvos-x64
#     watchos-arm64 (arm64_32), watchos-arm32 (armv7k), watchos-simulator-arm64, watchos-x64
#
# Нужно заранее:
#   * macOS и ПОЛНЫЙ Xcode (Xcode.app). Одних Command Line Tools мало: iOS, tvOS и watchOS собираются через
#     `xcrun -sdk <платформа>`, а эти SDK есть только в Xcode.app. Платные учётные записи не нужны.
#     В новых Xcode платформы tvOS и watchOS надо докачать (Xcode, Settings, Components). Нет SDK платформы —
#     дополнительная цель пропускается.
#       sudo xcode-select -s /Applications/Xcode.app/Contents/Developer
#       sudo xcodebuild -license accept
#   * Java 17+ для cinterop:  brew install openjdk@21
#   * ~25 ГБ свободного места и интернет (при первом запуске Kotlin/Native докачивает ~1 ГБ зависимостей).
#   * Время: ядро ≈ 30–50 мин, все цели ≈ 1,5–3 ч.
#
# Параметры:
#   --core-only                    только ядро (без tvOS, watchOS и ios-x64)
#   --only ios-arm64,macos-arm64   только перечисленные цели
#   --force                        пересобрать цели, даже если libopenssl.a уже есть
#   --no-install                   klib собрать, но в kmqtt-common/src/nativeInterop/ не копировать
#   --work DIR                     рабочий каталог (по умолчанию $HOME/kmqtt-apple-build)
#   -h | --help                    эта справка
#
# Повторный запуск безопасен: готовые цели пропускаются (без --force), скачанное не качается заново.
# =============================================================================
set -uo pipefail

OPENSSL_VERSION="3.5.9"
OPENSSL_SHA256="603f5602e2eef00d77fbd429d34dcd5822bb301757a1bc9cdb24c670f1eb859a"
KOTLIN_VERSION="2.4.20"

# Воспроизводимость и отсутствие следов машины сборки: с SOURCE_DATE_EPOCH OpenSSL пишет в libcrypto
# фиксированную дату вместо времени сборки; ZERO_AR_DATE и `libtool -D` обнуляют даты, uid и gid в архиве.
export SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-1760000000}"
export ZERO_AR_DATE=1

# Минимальные версии ОС объектов (Kotlin/Native 2.4: iOS и tvOS 14+, macOS 11+, watchOS 7+).
MIN_MACOS="11.0"; MIN_IOS="14.0"; MIN_TVOS="14.0"; MIN_WATCHOS="7.0"

CORE_TARGETS="ios-arm64 ios-simulator-arm64 macos-arm64 macos-x64"
EXTRA_TARGETS="ios-x64 tvos-arm64 tvos-simulator-arm64 tvos-x64 watchos-arm64 watchos-arm32 watchos-simulator-arm64 watchos-x64"
ALL_TARGETS="$CORE_TARGETS $EXTRA_TARGETS"
ONLY=""; CORE_ONLY=0; FORCE=0; INSTALL=1
WORK="${HOME}/kmqtt-apple-build"

c_red=$'\033[31m'; c_grn=$'\033[32m'; c_ylw=$'\033[33m'; c_off=$'\033[0m'
info() { printf '%s== %s%s\n' "$c_grn" "$*" "$c_off"; }
warn() { printf '%s!! %s%s\n' "$c_ylw" "$*" "$c_off"; }
die()  { printf '%sОШИБКА: %s%s\n' "$c_red" "$*" "$c_off" >&2; exit 1; }
usage() { sed -n '2,/^# =====/p' "$0" | sed '1d;$d' | sed 's/^# \{0,1\}//'; exit 0; }

while [ $# -gt 0 ]; do
  case "$1" in
    --core-only)  CORE_ONLY=1; shift ;;
    --only)       ONLY="${2:-}"; shift 2 ;;
    --force)      FORCE=1; shift ;;
    --no-install) INSTALL=0; shift ;;
    --work)       WORK="${2:-}"; shift 2 ;;
    -h|--help)    usage ;;
    *)            die "неизвестный параметр: $1 (см. --help)" ;;
  esac
done

# ---------- описание целей ----------
is_core() { case " $CORE_TARGETS " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }
configure_target() {
  case "$1" in
    ios-arm64)               echo "ios64-xcrun" ;;
    ios-simulator-arm64)     echo "iossimulator-arm64-xcrun" ;;
    ios-x64)                 echo "iossimulator-x86_64-xcrun" ;;
    macos-arm64)             echo "darwin64-arm64-cc" ;;
    macos-x64)               echo "darwin64-x86_64-cc" ;;
    tvos-arm64)              echo "kmqtt-tvos-arm64" ;;
    tvos-simulator-arm64)    echo "kmqtt-tvos-simulator-arm64" ;;
    tvos-x64)                echo "kmqtt-tvos-x64" ;;
    watchos-arm64)           echo "kmqtt-watchos-arm64-32" ;;
    watchos-arm32)           echo "kmqtt-watchos-armv7k" ;;
    watchos-simulator-arm64) echo "kmqtt-watchos-simulator-arm64" ;;
    watchos-x64)             echo "kmqtt-watchos-x64" ;;
  esac
}
cinterop_target() { echo "${1//-/_}"; }          # ios-arm64 -> ios_arm64 (имена Kotlin/Native)
sdk_of() {
  case "$1" in
    ios-arm64)                                     echo "iphoneos" ;;
    ios-simulator-arm64|ios-x64)                   echo "iphonesimulator" ;;
    tvos-arm64)                                    echo "appletvos" ;;
    tvos-simulator-arm64|tvos-x64)                 echo "appletvsimulator" ;;
    watchos-arm64|watchos-arm32)                   echo "watchos" ;;
    watchos-simulator-arm64|watchos-x64)           echo "watchsimulator" ;;
    *)                                             echo "macosx" ;;
  esac
}
clang_arch() {      # архитектура цели (для clang -arch и проверки lipo)
  case "$1" in
    ios-arm64|ios-simulator-arm64|macos-arm64|tvos-arm64|tvos-simulator-arm64|watchos-simulator-arm64) echo "arm64" ;;
    ios-x64|macos-x64|tvos-x64|watchos-x64)  echo "x86_64" ;;
    watchos-arm64)                           echo "arm64_32" ;;
    watchos-arm32)                           echo "armv7k" ;;
  esac
}
minver_flag() {     # флаг минимальной версии ОС цели
  case "$1" in
    ios-arm64)                echo "-miphoneos-version-min=${MIN_IOS}" ;;
    ios-simulator-arm64|ios-x64) echo "-mios-simulator-version-min=${MIN_IOS}" ;;
    macos-arm64|macos-x64)    echo "-mmacosx-version-min=${MIN_MACOS}" ;;
    tvos-arm64)               echo "-mtvos-version-min=${MIN_TVOS}" ;;
    tvos-simulator-arm64|tvos-x64) echo "-mtvos-simulator-version-min=${MIN_TVOS}" ;;
    watchos-arm64|watchos-arm32) echo "-mwatchos-version-min=${MIN_WATCHOS}" ;;
    watchos-simulator-arm64|watchos-x64) echo "-mwatchos-simulator-version-min=${MIN_WATCHOS}" ;;
  esac
}
# Дополнительные аргументы Configure: версии ОС — только для стандартных таргетов OpenSSL (свои таргеты tvOS и
# watchOS несут флаги в Configurations/90-kmqtt.conf); no-asm — для ILP32 и armv7k (ассемблер OpenSSL там не подходит).
configure_extra() {
  case "$1" in
    ios-arm64|ios-simulator-arm64|ios-x64|macos-arm64|macos-x64) minver_flag "$1" ;;
    watchos-arm64|watchos-arm32) echo "no-asm" ;;
  esac
}

# ---------- 0. где мы ----------
ROOT="$PWD"
[ "$(uname -s)" = "Darwin" ] || die "это скрипт для macOS (uname = $(uname -s))"
[ -f "$ROOT/kmqtt-common/src/nativeInterop/openssl.def" ] \
  || die "запускай из корня клона форка: не вижу kmqtt-common/src/nativeInterop/openssl.def в $ROOT"
[ -f "$ROOT/native-build/openssl-shim.h" ] \
  || die "не вижу native-build/openssl-shim.h: это не тот клон или ветка (нужна new_mqtt_client)"
grep -q "native-build/apple/macos-arm64/openssl-3.5.9" "$ROOT/kmqtt-common/src/nativeInterop/openssl.def" \
  || die "openssl.def в этом клоне не под OpenSSL 3.5.9: обнови ветку new_mqtt_client"

HOST_ARCH="$(uname -m)"
case "$HOST_ARCH" in
  arm64)  KN_ARCH="aarch64" ;;
  x86_64) KN_ARCH="x86_64" ;;
  *)      die "неизвестная архитектура: $HOST_ARCH" ;;
esac

if [ -n "$ONLY" ]; then TARGETS="${ONLY//,/ }"
elif [ "$CORE_ONLY" = 1 ]; then TARGETS="$CORE_TARGETS"
else TARGETS="$ALL_TARGETS"; fi
for t in $TARGETS; do
  case " $ALL_TARGETS " in *" $t "*) ;; *) die "неизвестная цель '$t' (допустимо: $ALL_TARGETS)" ;; esac
done

mkdir -p "$WORK"
info "корень форка: $ROOT; рабочий каталог: $WORK"
info "Mac: $(sw_vers -productName) $(sw_vers -productVersion), $HOST_ARCH; цели: $TARGETS"

# ---------- 1. окружение ----------
info "1. проверка окружения"
command -v clang   >/dev/null || die "нет clang: установи Xcode"
command -v perl    >/dev/null || die "нет perl"
command -v make    >/dev/null || die "нет make: установи Xcode"
command -v xcrun   >/dev/null || die "нет xcrun"
command -v libtool >/dev/null || die "нет libtool (Apple)"
echo "   clang: $(clang --version | head -1)"
echo "   xcode-select: $(xcode-select -p 2>/dev/null)"

SKIPPED=""     # дополнительные цели без SDK платформы
RUNLIST=""
for t in $TARGETS; do
  sdk="$(sdk_of "$t")"
  if xcrun --sdk "$sdk" --show-sdk-path >/dev/null 2>&1; then
    echo "   SDK $sdk ($t): $(xcrun --sdk "$sdk" --show-sdk-path)"
    RUNLIST="$RUNLIST $t"
  elif is_core "$t"; then
    die "нет SDK '$sdk' для обязательной цели $t. Нужен ПОЛНЫЙ Xcode (Xcode.app), не только Command Line Tools.
   После установки:  sudo xcode-select -s /Applications/Xcode.app/Contents/Developer
                     sudo xcodebuild -license accept
   Только macOS-цели:  bash native-build/build-apple.sh --only macos-arm64,macos-x64"
  else
    warn "нет SDK '$sdk' для цели $t: пропускаю (для tvOS и watchOS докачай платформу в Xcode, Settings, Components)"
    SKIPPED="$SKIPPED $t"
  fi
done
TARGETS="${RUNLIST# }"

JAVA_OK=0
if [ -x /usr/libexec/java_home ] && /usr/libexec/java_home -v 17+ >/dev/null 2>&1; then
  export JAVA_HOME="$(/usr/libexec/java_home -v 17+)"; JAVA_OK=1
elif command -v brew >/dev/null 2>&1 && [ -d "$(brew --prefix openjdk@21 2>/dev/null)" ]; then
  export JAVA_HOME="$(brew --prefix openjdk@21)/libexec/openjdk.jdk/Contents/Home"
  [ -x "$JAVA_HOME/bin/java" ] || export JAVA_HOME="$(brew --prefix openjdk@21)"
  JAVA_OK=1
elif command -v java >/dev/null 2>&1 && java -version >/dev/null 2>&1; then
  JAVA_OK=1
fi
[ "$JAVA_OK" = 1 ] || die "не найдена Java 17+. Установи:  brew install openjdk@21"
export PATH="${JAVA_HOME:+$JAVA_HOME/bin:}$PATH"
echo "   java: $(java -version 2>&1 | head -1)"

free_gb="$(df -g "$WORK" | awk 'NR==2{print $4}')"
echo "   свободно на диске: ${free_gb} ГБ"
[ "${free_gb:-0}" -ge 15 ] || warn "мало места (<15 ГБ): все цели могут не поместиться (для --core-only нужно ~8 ГБ)"

# ---------- 2. скачивание ----------
info "2. исходники OpenSSL ${OPENSSL_VERSION} и Kotlin/Native ${KOTLIN_VERSION}"
TARBALL="$WORK/openssl-${OPENSSL_VERSION}.tar.gz"
if [ ! -f "$TARBALL" ]; then
  curl -fL --retry 3 -o "$TARBALL" "https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz" \
    || die "не скачался тарбол OpenSSL"
fi
got="$(shasum -a 256 "$TARBALL" | awk '{print $1}')"
if [ "$got" != "$OPENSSL_SHA256" ]; then
  die "sha256 тарбола не совпал:
   ожидалось: $OPENSSL_SHA256
   получено:  $got
   Сборка не начата. Удали $TARBALL и запусти снова (возможна битая загрузка)."
fi
echo "   sha256 OpenSSL ок"

KN_DIR="$WORK/kotlin-native-prebuilt-macos-${KN_ARCH}-${KOTLIN_VERSION}"
if [ ! -x "$KN_DIR/bin/cinterop" ]; then
  KN_TGZ="$WORK/kotlin-native-${KN_ARCH}.tar.gz"
  curl -fL --retry 3 -o "$KN_TGZ" \
    "https://github.com/JetBrains/kotlin/releases/download/v${KOTLIN_VERSION}/kotlin-native-prebuilt-macos-${KN_ARCH}-${KOTLIN_VERSION}.tar.gz" \
    || die "не скачался Kotlin/Native"
  tar -xzf "$KN_TGZ" -C "$WORK" || die "не распаковался Kotlin/Native"
  rm -f "$KN_TGZ"
fi
[ -x "$KN_DIR/bin/cinterop" ] || die "после распаковки нет $KN_DIR/bin/cinterop"
CINTEROP="$KN_DIR/bin/cinterop"

# ---------- свои Configure-таргеты для tvOS и watchOS (в OpenSSL 3.5.9 их нет) ----------
write_seven_conf() {   # $1 = каталог исходников OpenSSL
  cat > "$1/Configurations/90-kmqtt.conf" <<EOF
# Таргеты tvOS и watchOS: по образцу ios*-xcrun из 15-ios.conf
my %targets = (
    "kmqtt-tvos-arm64" => {
        inherit_from => [ "ios-common" ],
        CC => "xcrun -sdk appletvos cc",
        cflags => add("-arch arm64 -fno-common -mtvos-version-min=${MIN_TVOS}"),
        bn_ops => "SIXTY_FOUR_BIT_LONG RC4_CHAR", asm_arch => 'aarch64', perlasm_scheme => "ios64",
    },
    "kmqtt-tvos-simulator-arm64" => {
        inherit_from => [ "ios-common" ],
        CC => "xcrun -sdk appletvsimulator cc",
        cflags => add("-arch arm64 -fno-common -mtvos-simulator-version-min=${MIN_TVOS}"),
        bn_ops => "SIXTY_FOUR_BIT_LONG", asm_arch => 'aarch64', perlasm_scheme => "ios64",
    },
    "kmqtt-tvos-x64" => {
        inherit_from => [ "ios-common" ],
        CC => "xcrun -sdk appletvsimulator cc",
        cflags => add("-arch x86_64 -fno-common -mtvos-simulator-version-min=${MIN_TVOS}"),
        bn_ops => "SIXTY_FOUR_BIT_LONG", asm_arch => 'x86_64', perlasm_scheme => "macosx",
    },
    "kmqtt-watchos-arm64-32" => {   # arm64_32 (ILP32): без ассемблера, 64-битные BN на long long
        inherit_from => [ "ios-common" ],
        CC => "xcrun -sdk watchos cc",
        cflags => add("-arch arm64_32 -fno-common -mwatchos-version-min=${MIN_WATCHOS}"),
        bn_ops => "BN_LLONG RC4_CHAR",
    },
    "kmqtt-watchos-armv7k" => {     # armv7k (32-бит ARM): без ассемблера
        inherit_from => [ "ios-common" ],
        CC => "xcrun -sdk watchos cc",
        cflags => add("-arch armv7k -fno-common -mwatchos-version-min=${MIN_WATCHOS}"),
        bn_ops => "BN_LLONG RC4_CHAR",
    },
    "kmqtt-watchos-simulator-arm64" => {
        inherit_from => [ "ios-common" ],
        CC => "xcrun -sdk watchsimulator cc",
        cflags => add("-arch arm64 -fno-common -mwatchos-simulator-version-min=${MIN_WATCHOS}"),
        bn_ops => "SIXTY_FOUR_BIT_LONG", asm_arch => 'aarch64', perlasm_scheme => "ios64",
    },
    "kmqtt-watchos-x64" => {
        inherit_from => [ "ios-common" ],
        CC => "xcrun -sdk watchsimulator cc",
        cflags => add("-arch x86_64 -fno-common -mwatchos-simulator-version-min=${MIN_WATCHOS}"),
        bn_ops => "SIXTY_FOUR_BIT_LONG", asm_arch => 'x86_64', perlasm_scheme => "macosx",
    },
);
EOF
}

# ---------- 3. сборка OpenSSL ----------
build_openssl() {
  local t="$1" cfg src dist extra ncpu arch
  cfg="$(configure_target "$t")"; extra="$(configure_extra "$t")"; arch="$(clang_arch "$t")"
  src="$WORK/src-$t"; dist="$ROOT/native-build/apple/$t/openssl-${OPENSSL_VERSION}"
  ncpu="$(sysctl -n hw.ncpu)"
  if [ "$FORCE" = 0 ] && [ -f "$dist/libopenssl.a" ] && [ -d "$dist/include/openssl" ]; then
    echo "   [$t] уже собран: пропускаю (--force пересоберёт)"; return 0
  fi
  echo "   [$t] Configure $cfg $extra"
  rm -rf "$src"; mkdir -p "$src"
  tar -xzf "$TARBALL" -C "$src" --strip-components=1 || return 1
  write_seven_conf "$src"
  ( cd "$src" \
    && ./Configure --release no-shared no-tests no-docs no-apps "$cfg" $extra \
    && make -j"$ncpu" ) || return 1
  [ -f "$src/libssl.a" ] && [ -f "$src/libcrypto.a" ] || { echo "   [$t] нет libssl.a и libcrypto.a после make"; return 1; }

  rm -rf "$dist"; mkdir -p "$dist"
  cp -R "$src/include" "$dist/include" || return 1
  # libtool -static склеивает архивы и строит индекс символов (ar -qc без ranlib линковщик Apple не примет)
  libtool -static -D -o "$dist/libopenssl.a" "$src/libssl.a" "$src/libcrypto.a" || return 1
  echo "   [$t] lipo: $(lipo -info "$dist/libopenssl.a" 2>&1)"
  lipo -info "$dist/libopenssl.a" 2>&1 | grep -q "$arch" \
    || { echo "   [$t] ОШИБКА: в libopenssl.a нет архитектуры $arch"; return 1; }
  [ -f "$dist/include/openssl/configuration.h" ] \
    || { echo "   [$t] ОШИБКА: нет сгенерированного include/openssl/configuration.h"; return 1; }
  printf '   [%s] libopenssl.a: %s Б\n' "$t" "$(stat -f%z "$dist/libopenssl.a")"
  rm -rf "$src"       # исходники и объекты одной цели ~0,5–1 ГБ: после склейки не нужны
}

info "3. сборка OpenSSL (5–20 минут на цель)"
FAILED_CORE=""; FAILED_EXTRA=""
BUILT_SSL=""
for t in $TARGETS; do
  if build_openssl "$t"; then BUILT_SSL="$BUILT_SSL $t"
  else
    warn "цель $t: сборка OpenSSL не удалась"
    if is_core "$t"; then FAILED_CORE="$FAILED_CORE $t"; else FAILED_EXTRA="$FAILED_EXTRA $t"; fi
  fi
done

# ---------- 4. проверка библиотек: линковка (и запуск на macOS) ----------
SMOKE_C="$WORK/smoke.c"
cat > "$SMOKE_C" <<'EOF'
#include <stdio.h>
#include <string.h>
#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/crypto.h>
int main(void) {
    unsigned char d[32]; unsigned int n = 0;
    EVP_Digest("abc", 3, d, &n, EVP_sha256(), NULL);
    SSL_CTX *c = SSL_CTX_new(TLS_client_method());
    printf("OpenSSL %s sha256(abc)=%02x%02x%02x%02x ctx=%d\n", OpenSSL_version(OPENSSL_VERSION_STRING), d[0], d[1], d[2], d[3], c != NULL);
    return (c != NULL && d[0] == 0xba && d[1] == 0x78 && d[2] == 0x16 && d[3] == 0xbf) ? 0 : 1;
}
EOF
smoke_test() {   # $1 = цель
  local t="$1" dist sdk arch bin out
  dist="$ROOT/native-build/apple/$t/openssl-${OPENSSL_VERSION}"; sdk="$(sdk_of "$t")"; arch="$(clang_arch "$t")"
  bin="$WORK/smoke-$t"
  if ! xcrun --sdk "$sdk" clang -arch "$arch" "$(minver_flag "$t")" -I"$dist/include" "$SMOKE_C" "$dist/libopenssl.a" -o "$bin" 2>"$WORK/smoke-$t.err"; then
    echo "   [$t] ЛИНКОВКА не удалась: $(head -3 "$WORK/smoke-$t.err" | tr '\n' ' ')"; return 1
  fi
  case "$t" in
    macos-arm64) [ "$HOST_ARCH" = arm64 ] || { echo "   [$t] слинковано ок (запуск на Intel невозможен)"; return 0; } ;;
    macos-x64)   if [ "$HOST_ARCH" != x86_64 ] && ! arch -x86_64 /usr/bin/true >/dev/null 2>&1; then
                   echo "   [$t] слинковано ок (нет Rosetta, не запускаю)"; return 0; fi ;;
    *)           echo "   [$t] слинковано ок (запуск на Mac для этой платформы невозможен)"; return 0 ;;
  esac
  if [ "$t" = macos-x64 ] && [ "$HOST_ARCH" != x86_64 ]; then out="$(arch -x86_64 "$bin" 2>&1)"; else out="$("$bin" 2>&1)"; fi
  if [ $? -eq 0 ]; then echo "   [$t] запуск ок: $out"; return 0; fi
  echo "   [$t] запуск вернул ошибку: $out"; return 1
}
info "4. проверка библиотек (линковка тестовой программы; на macOS ещё и запуск)"
CHECKED=""
for t in $BUILT_SSL; do
  if smoke_test "$t"; then CHECKED="$CHECKED $t"
  else
    warn "[$t] проверка libopenssl.a не пройдена"
    if is_core "$t"; then FAILED_CORE="$FAILED_CORE $t"; else FAILED_EXTRA="$FAILED_EXTRA $t"; fi
  fi
done

# ---------- 5. cinterop ----------
info "5. cinterop: klib (Kotlin/Native ${KOTLIN_VERSION}; при первом запуске докачивает зависимости, это долго)"
KLIB_OUT="$WORK/klib"; mkdir -p "$KLIB_OUT"
BUILT_KLIB=""
for t in $CHECKED; do
  rm -f "$KLIB_OUT/openssl-$t.klib"
  echo "   [$t] cinterop -target $(cinterop_target "$t")"
  if ( cd "$ROOT" && "$CINTEROP" -def kmqtt-common/src/nativeInterop/openssl.def \
        -target "$(cinterop_target "$t")" -o "$KLIB_OUT/openssl-$t" ) \
     && [ -f "$KLIB_OUT/openssl-$t.klib" ]; then
    BUILT_KLIB="$BUILT_KLIB $t"
  else
    warn "[$t] cinterop не удался"
    if is_core "$t"; then FAILED_CORE="$FAILED_CORE $t"; else FAILED_EXTRA="$FAILED_EXTRA $t"; fi
  fi
done

# ---------- 6. проверка klib ----------
info "6. проверка klib"
OKLIST=""
for t in $BUILT_KLIB; do
  k="$KLIB_OUT/openssl-$t.klib"; size="$(stat -f%z "$k")"
  inc="$(unzip -l "$k" 2>/dev/null | grep -c 'included/libopenssl.a')"
  if [ "$inc" -lt 1 ]; then
    warn "[$t] в klib нет included/libopenssl.a: статика не вошла"
    if is_core "$t"; then FAILED_CORE="$FAILED_CORE $t"; else FAILED_EXTRA="$FAILED_EXTRA $t"; fi
    continue
  fi
  [ "$size" -ge 3000000 ] || warn "[$t] klib подозрительно мал ($size Б < 3 МБ)"
  echo "   [$t] ок: $size Б, libopenssl.a внутри"
  OKLIST="$OKLIST $t"
done

# Следы этого Mac в klib (домашний каталог, логин, имя хоста): не блокируют сборку, но такой klib лучше не публиковать
info "   поиск следов этого Mac в klib"
PRIV_PATTERNS=()
[ -n "${HOME:-}" ] && [ "${#HOME}" -ge 4 ] && PRIV_PATTERNS+=("$HOME")
[ -n "${USER:-}" ] && [ "${#USER}" -ge 4 ] && PRIV_PATTERNS+=("/Users/$USER")
_h="$(hostname -s 2>/dev/null || true)"; [ "${#_h}" -ge 4 ] && PRIV_PATTERNS+=("$_h")
_c="$(scutil --get ComputerName 2>/dev/null || true)"; [ "${#_c}" -ge 4 ] && PRIV_PATTERNS+=("$_c")
PRIV_DIRTY=""
for t in $OKLIST; do
  k="$KLIB_OUT/openssl-$t.klib"; hits=0
  for p in "${PRIV_PATTERNS[@]+"${PRIV_PATTERNS[@]}"}"; do
    n="$(unzip -p "$k" 2>/dev/null | LC_ALL=C grep -a -c -F -- "$p" || true)"
    if [ "${n:-0}" -gt 0 ]; then hits=$((hits + n)); warn "[$t] найдено '$p' в ${n} строках"; fi
  done
  [ "$hits" -eq 0 ] || PRIV_DIRTY="$PRIV_DIRTY $t"
done
[ -z "$PRIV_DIRTY" ] && echo "   следов не найдено"

# ---------- 7. установка ----------
if [ "$INSTALL" = 1 ] && [ -n "$OKLIST" ]; then
  info "7. klib → kmqtt-common/src/nativeInterop/"
  for t in $OKLIST; do
    cp "$KLIB_OUT/openssl-$t.klib" "$ROOT/kmqtt-common/src/nativeInterop/openssl-$t.klib" && echo "   openssl-$t.klib"
  done
elif [ "$INSTALL" = 0 ]; then
  info "7. --no-install: klib остались в $KLIB_OUT"
fi

# ---------- итог ----------
echo
for t in $OKLIST; do
  k="$KLIB_OUT/openssl-$t.klib"
  printf '  %s  %s  openssl-%s.klib\n' "$(shasum -a 256 "$k" | awk '{print $1}')" "$(stat -f%z "$k")" "$t"
done
[ -z "$SKIPPED" ]      || warn "пропущены (нет SDK платформы):$SKIPPED"
[ -z "$FAILED_EXTRA" ] || warn "дополнительные цели с ошибками (прежний klib остался):$FAILED_EXTRA"
[ -z "$PRIV_DIRTY" ]   || warn "klib со следами этого Mac:$PRIV_DIRTY"
if [ -n "$FAILED_CORE" ]; then
  warn "цели ядра с ошибками:$FAILED_CORE"
  exit 1
fi
info "готово:${OKLIST}"
