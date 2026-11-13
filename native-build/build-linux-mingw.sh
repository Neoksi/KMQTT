#!/usr/bin/env bash
# =============================================================================
# Сборка klib OpenSSL 3.5.9 LTS для linux_x64, linux_arm64 и mingw_x64 форка (в Docker).
#
# Запуск из любого каталога, нужен Docker:
#
#     bash native-build/build-linux-mingw.sh
#
# Что делает:
#   1. собирает образ kmqtt-openssl-3.5:local (Dockerfile фиксирует версии OpenSSL и Kotlin/Native,
#      sha256 тарбола проверяется внутри): OpenSSL под три цели, libopenssl.a, cinterop;
#   2. копирует три klib в native-build/out/ (каталог в git не хранится);
#   3. кладёт их в kmqtt-common/src/nativeInterop/, заменяя прежние.
#
# Параметры:
#   --no-install   klib собрать, но в kmqtt-common/src/nativeInterop/ не копировать
#   -h | --help    эта справка
#
# Образ можно задать переменной KMQTT_OPENSSL_IMAGE.
# =============================================================================
set -euo pipefail

INSTALL=1
while [ $# -gt 0 ]; do
    case "$1" in
        --no-install) INSTALL=0; shift ;;
        -h|--help)    sed -n '2,/^# =====/p' "$0" | sed '1d;$d' | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)            echo "неизвестный параметр: $1 (см. --help)" >&2; exit 1 ;;
    esac
done

cd "$(dirname "$0")"

IMAGE="${KMQTT_OPENSSL_IMAGE:-kmqtt-openssl-3.5:local}"
OUT_DIR="$PWD/out"
DEST_DIR="$PWD/../kmqtt-common/src/nativeInterop"

export MSYS_NO_PATHCONV=1
host_path() {
    if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else printf '%s' "$1"; fi
}
sha256() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$@"; else shasum -a 256 "$@"; fi
}

echo "== docker build $IMAGE"
docker build -t "$IMAGE" .

mkdir -p "$OUT_DIR"
rm -f "$OUT_DIR"/openssl-*.klib

echo "== klib в $OUT_DIR"
docker run --rm -v "$(host_path "$OUT_DIR"):/out" "$IMAGE"

if [ "$INSTALL" = 1 ]; then
    echo "== klib в kmqtt-common/src/nativeInterop/"
    cp "$OUT_DIR"/openssl-*.klib "$DEST_DIR"/
fi

echo "== готово:"
( cd "$OUT_DIR" && sha256 openssl-*.klib && ls -la openssl-*.klib )
