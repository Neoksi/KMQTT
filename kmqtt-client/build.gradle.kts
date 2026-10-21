plugins {
    alias(libs.plugins.kotlin.multiplatform)
    id("convention.publication")
    id("org.jetbrains.kotlinx.atomicfu")
}

kotlin {
    explicitApi()
    jvm {
        compilerOptions {
            jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_1_8)
        }
    }
    js {
        nodejs {
            binaries.executable()
            // контрактные тесты хоста идут в реальном времени (8 МиБ записи, 200 циклов, DNS до 30 с);
            // их сценарии ограничены изнутри (scenarioTimeout 30–120 с), поэтому общий предел Mocha жёстче:
            // зависший тест падает через 60 с, а не через 180 с
            testTask {
                useMocha {
                    timeout = "60s"
                }
            }
        }
    }
    mingwX64 {}
    linuxX64 {}
    linuxArm64 {}
    iosX64 {}
    iosArm64 {}
    iosSimulatorArm64 {}
    macosX64 {}
    macosArm64 {}
    tvosX64 {}
    tvosSimulatorArm64 {}
    tvosArm64 {}
    watchosArm32 {}
    watchosArm64 {}
    watchosSimulatorArm64 {}
    watchosX64 {}

    sourceSets {
        all {
            languageSettings.apply {
                optIn("kotlin.ExperimentalUnsignedTypes")
                optIn("kotlinx.cinterop.ExperimentalForeignApi")
                // ядро/движок нового клиента (@MqttCoreApi) — opt-in только для тестов, main объявляет явно
                if (name.endsWith("Test")) {
                    optIn("io.github.davidepianca98.client.core.MqttCoreApi")
                }
            }
        }
        val commonMain by getting {
            dependencies {
                implementation(kotlin("stdlib-common"))
                // типы kmqtt-common (Qos, ReasonCode, MQTTVersion, TLSClientSettings) и kotlinx.coroutines
                // (Flow, StateFlow, Deferred, Job) — в публичном API клиента, поэтому api, а не implementation
                api(project(":kmqtt-common"))
                api(libs.kotlinx.coroutines.core)
            }
        }
        val commonTest by getting {
            dependencies {
                implementation(kotlin("test-common"))
                implementation(kotlin("test-annotations-common"))
                // общий контракт хоста (commonTest/hostcontract) — runTest + реальное время
                implementation(libs.kotlinx.coroutines.test)
            }
        }
        val jvmMain by getting {}
        val jvmTest by getting {
            dependencies {
                implementation(kotlin("test"))
                implementation(kotlin("test-junit"))
                implementation(libs.kotlinx.coroutines.test)
                // встроенный брокер для интеграционных тестов нового клиента
                implementation(project(":kmqtt-broker"))
            }
        }
        val jsMain by getting {
            dependencies {
                implementation(libs.kotlin.node)
            }
        }
        val jsTest by getting {
            dependencies {
                implementation(kotlin("test-js"))
            }
        }
        val posixMain by creating {
            dependsOn(commonMain)
            dependencies {
                implementation(libs.atomicfu)
            }
        }
        // TLS нового клиента на native-хостах (OpenSSL memory-BIO): общий слой без OpenSSL (транспорт-обёртка,
        // контракт сессии) — для mingwX64, Linux и Apple (iOS/macOS). Код на OpenSSL — в src/nativeTlsOpenssl/kotlin:
        // klib OpenSSL подключён файлом каждой цели и не коммонизован, поэтому каталог компилируется в каждой цели отдельно.
        val nativeTlsMain by creating {
            dependsOn(posixMain)
        }
        val mingwX64Main by getting {
            dependsOn(nativeTlsMain)
            kotlin.srcDir("src/nativeTlsOpenssl/kotlin")
        }
        // Хост нового клиента на POSIX (PosixHost, poll): общий для Linux и Apple; различия платформ — expect/actual
        // в linuxMain и appleTlsMain
        val posixHostMain by creating {
            dependsOn(nativeTlsMain)
        }
        val linuxMain by creating {
            dependsOn(posixHostMain)
        }
        val linuxX64Main by getting {
            dependsOn(linuxMain)
            kotlin.srcDir("src/nativeTlsOpenssl/kotlin")
        }
        val linuxArm64Main by getting {
            dependsOn(linuxMain)
            kotlin.srcDir("src/nativeTlsOpenssl/kotlin")
        }
        // Контракт хоста POSIX и его платформенные тесты (общие для Linux и Apple); запускаются linuxX64Test (на Linux,
        // см. Readme), macosArm64Test/macosX64Test и тесты симуляторов Apple (на macOS), цели устройств — только компиляция
        val posixHostTest by creating {
            dependsOn(commonTest)
        }
        val linuxTest by creating {
            dependsOn(posixHostTest)
        }
        val linuxX64Test by getting {
            dependsOn(linuxTest)
            kotlin.srcDir("src/nativeTlsTest/kotlin")
        }
        val linuxArm64Test by getting {
            dependsOn(linuxTest)
            kotlin.srcDir("src/nativeTlsTest/kotlin")
        }
        // тесты TLS native (OpenSSL в тестах — тоже по цели): mingwX64Test на Windows, linuxX64Test на Linux
        val mingwX64Test by getting {
            kotlin.srcDir("src/nativeTlsTest/kotlin")
        }
        // Apple (macOS, iOS, tvOS, watchOS): тот же хост POSIX (SIGPIPE подавляет SO_NOSIGPIPE на сокете) и TLS на OpenSSL;
        // фабрика MqttClient(options) и корни доверия — в src/appleTlsMain. Компилируется на любой ОС (klib), запускается
        // только на macOS (macosArm64Test, macosX64Test, тесты симуляторов iOS/tvOS/watchOS)
        val appleTlsMain by creating {
            dependsOn(posixHostMain)
        }
        val iosX64Main by getting {
            dependsOn(appleTlsMain)
            kotlin.srcDir("src/nativeTlsOpenssl/kotlin")
        }
        val iosArm64Main by getting {
            dependsOn(appleTlsMain)
            kotlin.srcDir("src/nativeTlsOpenssl/kotlin")
        }
        val iosSimulatorArm64Main by getting {
            dependsOn(appleTlsMain)
            kotlin.srcDir("src/nativeTlsOpenssl/kotlin")
        }
        val macosX64Main by getting {
            dependsOn(appleTlsMain)
            kotlin.srcDir("src/nativeTlsOpenssl/kotlin")
        }
        val macosArm64Main by getting {
            dependsOn(appleTlsMain)
            kotlin.srcDir("src/nativeTlsOpenssl/kotlin")
        }
        val tvosX64Main by getting {
            dependsOn(appleTlsMain)
            kotlin.srcDir("src/nativeTlsOpenssl/kotlin")
        }
        val tvosArm64Main by getting {
            dependsOn(appleTlsMain)
            kotlin.srcDir("src/nativeTlsOpenssl/kotlin")
        }
        val tvosSimulatorArm64Main by getting {
            dependsOn(appleTlsMain)
            kotlin.srcDir("src/nativeTlsOpenssl/kotlin")
        }
        val watchosX64Main by getting {
            dependsOn(appleTlsMain)
            kotlin.srcDir("src/nativeTlsOpenssl/kotlin")
        }
        val watchosArm32Main by getting {
            dependsOn(appleTlsMain)
            kotlin.srcDir("src/nativeTlsOpenssl/kotlin")
        }
        val watchosArm64Main by getting {
            dependsOn(appleTlsMain)
            kotlin.srcDir("src/nativeTlsOpenssl/kotlin")
        }
        val watchosSimulatorArm64Main by getting {
            dependsOn(appleTlsMain)
            kotlin.srcDir("src/nativeTlsOpenssl/kotlin")
        }
        val appleTest by creating {
            dependsOn(posixHostTest)
        }
        val iosX64Test by getting {
            dependsOn(appleTest)
            kotlin.srcDir("src/nativeTlsTest/kotlin")
        }
        val iosArm64Test by getting {
            dependsOn(appleTest)
            kotlin.srcDir("src/nativeTlsTest/kotlin")
        }
        val iosSimulatorArm64Test by getting {
            dependsOn(appleTest)
            kotlin.srcDir("src/nativeTlsTest/kotlin")
        }
        val macosX64Test by getting {
            dependsOn(appleTest)
            kotlin.srcDir("src/nativeTlsTest/kotlin")
        }
        val macosArm64Test by getting {
            dependsOn(appleTest)
            kotlin.srcDir("src/nativeTlsTest/kotlin")
        }
        val tvosX64Test by getting {
            dependsOn(appleTest)
            kotlin.srcDir("src/nativeTlsTest/kotlin")
        }
        val tvosArm64Test by getting {
            dependsOn(appleTest)
            kotlin.srcDir("src/nativeTlsTest/kotlin")
        }
        val tvosSimulatorArm64Test by getting {
            dependsOn(appleTest)
            kotlin.srcDir("src/nativeTlsTest/kotlin")
        }
        val watchosX64Test by getting {
            dependsOn(appleTest)
            kotlin.srcDir("src/nativeTlsTest/kotlin")
        }
        val watchosArm32Test by getting {
            dependsOn(appleTest)
            kotlin.srcDir("src/nativeTlsTest/kotlin")
        }
        val watchosArm64Test by getting {
            dependsOn(appleTest)
            kotlin.srcDir("src/nativeTlsTest/kotlin")
        }
        val watchosSimulatorArm64Test by getting {
            dependsOn(appleTest)
            kotlin.srcDir("src/nativeTlsTest/kotlin")
        }
    }
}

// TLS-стенд для тестов native (NativeTlsTestbedTest) — переменные окружения или свойства Gradle
// (-Pkmqtt.tls.a=..., имя свойства — переменная в нижнем регистре с точками); без них тесты стенда пропускаются
tasks.withType<org.jetbrains.kotlin.gradle.targets.native.tasks.KotlinNativeTest>().configureEach {
    listOf("KMQTT_TLS_A", "KMQTT_TLS_A_BADHOST", "KMQTT_TLS_B", "KMQTT_TLS_CERTS").forEach { name ->
        val value = providers.environmentVariable(name).orNull
            ?: providers.gradleProperty(name.lowercase().replace('_', '.')).orNull
        if (value != null) environment(name, value)
    }
}

// Fix Gradle warning about signing tasks using publishing task outputs without explicit dependencies
// https://github.com/gradle/gradle/issues/26091
tasks.withType<AbstractPublishToMaven>().configureEach {
    val signingTasks = tasks.withType<Sign>()
    mustRunAfter(signingTasks)
}

publishing {
    repositories {
        maven {
            name = "github"
            url = uri("https://maven.pkg.github.com/davidepianca98/KMQTT")
            credentials(PasswordCredentials::class)
        }
    }
}

