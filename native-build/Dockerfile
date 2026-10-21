FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Тулчейны: нативный gcc, кросс aarch64 (linuxArm64), кросс mingw-w64 (mingwX64).
# libncurses5 — для LLVM-тулчейна Kotlin/Native на jammy.
RUN apt-get update \
    && apt-get -y install --no-install-recommends \
        bash \
        wget \
        ca-certificates \
        tar \
        perl \
        make \
        gcc \
        libc6-dev \
        gcc-aarch64-linux-gnu \
        libc6-dev-arm64-cross \
        mingw-w64 \
        libncurses5 \
        openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

####################################################
# Версии (см. native-build/README.md)              #
####################################################
ENV OPENSSL_VERSION="3.5.9" \
    OPENSSL_SHA256="603f5602e2eef00d77fbd429d34dcd5822bb301757a1bc9cdb24c670f1eb859a" \
    KOTLIN_VERSION="2.4.20"

WORKDIR /build

# Kotlin/Native компилятор cinterop — версии проекта
RUN wget -q -O kotlin-native.tar.gz \
        "https://github.com/JetBrains/kotlin/releases/download/v${KOTLIN_VERSION}/kotlin-native-prebuilt-linux-x86_64-${KOTLIN_VERSION}.tar.gz" \
    && tar -xzf kotlin-native.tar.gz \
    && rm kotlin-native.tar.gz \
    && mv "kotlin-native-prebuilt-linux-x86_64-${KOTLIN_VERSION}" kotlin-native

# Исходники OpenSSL с проверкой sha256
RUN wget -q -O "openssl-${OPENSSL_VERSION}.tar.gz" \
        "https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz" \
    && echo "${OPENSSL_SHA256}  openssl-${OPENSSL_VERSION}.tar.gz" | sha256sum -c - \
    && mkdir -p src/linux-x86_64 src/linux-aarch64 src/mingw64 \
    && tar -xzf "openssl-${OPENSSL_VERSION}.tar.gz" -C src/linux-x86_64 --strip-components=1 \
    && tar -xzf "openssl-${OPENSSL_VERSION}.tar.gz" -C src/linux-aarch64 --strip-components=1 \
    && tar -xzf "openssl-${OPENSSL_VERSION}.tar.gz" -C src/mingw64 --strip-components=1 \
    && rm "openssl-${OPENSSL_VERSION}.tar.gz"

# no-shared — статические архивы, провайдеры линкуются в libcrypto.a;
# no-tests/no-docs/no-apps сокращают сборку; --release — без отладочной информации.
RUN cd src/linux-x86_64 \
    && ./Configure --release no-shared no-tests no-docs no-apps linux-x86_64 \
    && make -j"$(nproc)" \
    && mkdir -p /build/dist/linux-x86_64 \
    && cp -r include /build/dist/linux-x86_64/ \
    && mkdir /build/merge-linux-x86_64 \
    && cd /build/merge-linux-x86_64 \
    && ar -x /build/src/linux-x86_64/libssl.a \
    && ar -x /build/src/linux-x86_64/libcrypto.a \
    && ar -qc /build/dist/linux-x86_64/libopenssl.a $(find . -mindepth 1 -type f \( -name '*.o' -o -name '*.obj' \))

RUN cd src/linux-aarch64 \
    && ./Configure --release no-shared no-tests no-docs no-apps --cross-compile-prefix=aarch64-linux-gnu- -mno-outline-atomics linux-aarch64 \
    && make -j"$(nproc)" \
    && mkdir -p /build/dist/linux-aarch64 \
    && cp -r include /build/dist/linux-aarch64/ \
    && mkdir /build/merge-linux-aarch64 \
    && cd /build/merge-linux-aarch64 \
    && ar -x /build/src/linux-aarch64/libssl.a \
    && ar -x /build/src/linux-aarch64/libcrypto.a \
    && ar -qc /build/dist/linux-aarch64/libopenssl.a $(find . -mindepth 1 -type f \( -name '*.o' -o -name '*.obj' \))

RUN cd src/mingw64 \
    && ./Configure --release no-shared no-tests no-docs no-apps --cross-compile-prefix=x86_64-w64-mingw32- mingw64 \
    && make -j"$(nproc)" \
    && mkdir -p /build/dist/mingw64 \
    && cp -r include /build/dist/mingw64/ \
    && mkdir /build/merge-mingw64 \
    && cd /build/merge-mingw64 \
    && ar -x /build/src/mingw64/libssl.a \
    && ar -x /build/src/mingw64/libcrypto.a \
    && ar -qc /build/dist/mingw64/libopenssl.a $(find . -mindepth 1 -type f \( -name '*.o' -o -name '*.obj' \))

COPY openssl-3.5.def openssl-shim.h /build/

# Три klib: имена совпадают с файлами в kmqtt-common/src/nativeInterop/
RUN mkdir -p /out-klib \
    && /build/kotlin-native/bin/cinterop -def /build/openssl-3.5.def -target linux_x64 -o /out-klib/openssl-linux-x64 \
    && /build/kotlin-native/bin/cinterop -def /build/openssl-3.5.def -target linux_arm64 -o /out-klib/openssl-linux-arm64 \
    && /build/kotlin-native/bin/cinterop -def /build/openssl-3.5.def -target mingw_x64 -o /out-klib/openssl-mingw-x64 \
    && ls -la /out-klib

# build-linux-mingw.sh копирует артефакты в native-build/out на хосте
CMD ["sh", "-c", "cp /out-klib/*.klib /out/ && sha256sum /out/*.klib"]
