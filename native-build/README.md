# Воспроизводимая сборка klib OpenSSL 3.5 LTS для форка

Сборка трёх klib (`openssl-linux-x64.klib`, `openssl-linux-arm64.klib`, `openssl-mingw-x64.klib`)
из официальных исходников OpenSSL 3.5 LTS для целей `linuxX64`, `linuxArm64`, `mingwX64`.
Заменяет klib OpenSSL 1.1.1w в `kmqtt-common/src/nativeInterop/`. Apple-klib (macOS, iOS, tvOS, watchOS) строит
парный скрипт `build-apple.sh` на Mac (раздел «Apple» ниже).

## Что фиксировано

| Что | Значение |
|---|---|
| OpenSSL | `openssl-3.5.9.tar.gz` с `https://www.openssl.org/source/` (LTS до 08.04.2030) |
| sha256 тарбола | `603f5602e2eef00d77fbd429d34dcd5822bb301757a1bc9cdb24c670f1eb859a` |
| Kotlin/Native | `2.4.20` (версия проекта, совпадает с корневым `gradle/libs.versions.toml`) |
| Базовый образ | `ubuntu:22.04` |
| Конфигурация | `no-shared no-tests no-docs no-apps --release` (статические libssl/libcrypto, без приложений и тестов) |
| Таргеты | `linux-x86_64`, `linux-aarch64` (`--cross-compile-prefix=aarch64-linux-gnu-`), `mingw64` (`--cross-compile-prefix=x86_64-w64-mingw32-`) |

Проверка sha256 тарбола выполняется внутри Dockerfile (`sha256sum -c`), при расхождении сборка падает.

## Сборка

```bash
bash native-build/build-linux-mingw.sh   # нужен Docker; --no-install — не копировать в nativeInterop
```

`build-linux-mingw.sh` собирает образ `kmqtt-openssl-3.5:local`, копирует три `.klib` в `native-build/out/`
и кладёт их в `kmqtt-common/src/nativeInterop/` взамен старых (имена совпадают).

## Как устроено

1. Dockerfile ставит тулчейны (`gcc`, `gcc-aarch64-linux-gnu`, `mingw-w64`), качает и
   распаковывает `kotlin-native-prebuilt-linux-x86_64-2.4.20`.
2. Тарбол OpenSSL проверяется по sha256, распаковывается в три каталога; каждый таргет
   конфигурируется сокращённо (`no-tests no-docs no-apps` экономят время и размер, `no-shared`
   даёт статические архивы без динамических провайдеров — провайдеры линкуются в `libcrypto.a`).
3. `libssl.a` и `libcrypto.a` склеиваются `ar` в единый `libopenssl.a` (порядок объектов
   внутри одного архива для линковщика не важен — тот же приём, что и для 1.1.1w
   в `kmqtt-broker/src/linuxX64Main/resources/Dockerfile`).
4. `cinterop` со своим `openssl-3.5.def` собирает klib по каждой цели (`linux_x64`,
   `linux_arm64`, `mingw_x64`); заголовки и `libopenssl.a` упаковываются в klib.
   Нюанс 3.5: `./Configure`/`make build_generated` превращает типизированные функции стека
   (`sk_<T>_num/value/pop_free/...`) в function-like макросы, которые cinterop не индексирует.
   `openssl-shim.h` (последний в списке headers) возвращает нужные форку четыре функции
   (`sk_X509_INFO_num/value/pop_free`, `sk_X509_OBJECT_num`) как `static inline` с именами
   и сигнатурами 1.1.1w — код форка (`TLSClientEngine`, `OpenSslSession`) не меняется.
5. Файл `kmqtt-common/src/nativeInterop/openssl.def` (Apple-цели) этим скриптом не используется;
   Apple-klib строит `build-apple.sh` (см. ниже).

## Apple: `build-apple.sh`

```bash
cd <корень форка>                 # папка kmqtt
bash native-build/build-apple.sh  # --help, --core-only, --only ios-arm64,macos-arm64, --force, --no-install
```

Нужны macOS и **полный Xcode** (Xcode.app; одних Command Line Tools мало: iOS, tvOS и watchOS собираются через
`xcrun -sdk <платформа>`), Java 17+ (`brew install openjdk@21`), ~25 ГБ места и интернет. Скрипт качает OpenSSL 3.5.9
(sha256 сверяется) и Kotlin/Native 2.4.20, собирает статический OpenSSL под 12 целей (ядро: iosArm64,
iosSimulatorArm64, macosArm64, macosX64; остальные: iosX64, tvOS и watchOS), склеивает `libssl.a` и `libcrypto.a` в
`libopenssl.a` (`libtool -static -D`) в `native-build/apple/<цель>/openssl-3.5.9/` (промежуточное дерево, которое ждёт
`openssl.def`; в git не хранится), проверяет каждую библиотеку (линковка тестовой программы, на macOS ещё и запуск),
собирает klib через cinterop с тем же `openssl-shim.h`, проверяет klib (в том числе на следы машины сборки) и
кладёт их в `kmqtt-common/src/nativeInterop/`. В OpenSSL 3.5.9 нет готовых Configure-таргетов для tvOS и watchOS:
скрипт сам пишет `Configurations/90-kmqtt.conf`. Воспроизводимость: `SOURCE_DATE_EPOCH` (в libcrypto «built on» —
фиксированное значение, не реальное время), `ZERO_AR_DATE=1`, минимальные версии ОС macOS 11 / iOS и tvOS 14 /
watchOS 7.

Сборка 2026-10-21 (Mac arm64, Xcode 26.3, Apple clang 17.0.0): все 12 целей собрались и прошли проверку (на macOS
arm64 и x64 тестовая программа запущена: OpenSSL 3.5.9, SHA-256("abc") = `ba7816bf…`; архитектура и платформа внутри
каждой библиотеки сверены с целью). Размеры klib 3,9–4,7 МБ.
