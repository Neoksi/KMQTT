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
            // SEVEN-250: контрактные тесты хоста идут в реальном времени (8 МиБ записи, 200 циклов, DNS до 30 с)
            testTask {
                useMocha {
                    timeout = "180s"
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
                // SEVEN-220: ядро/движок нового клиента (@MqttCoreApi) — opt-in только для тестов, main объявляет явно
                if (name.endsWith("Test")) {
                    optIn("io.github.davidepianca98.client.core.MqttCoreApi")
                }
            }
        }
        val commonMain by getting {
            dependencies {
                implementation(kotlin("stdlib-common"))
                // SEVEN-255 (D3): типы kmqtt-common (Qos, ReasonCode, MQTTVersion, TLSClientSettings) и kotlinx.coroutines
                // (Flow, StateFlow, Deferred, Job) — в публичном API клиента, поэтому api, а не implementation
                api(project(":kmqtt-common"))
                api(libs.kotlinx.coroutines.core)
            }
        }
        val commonTest by getting {
            dependencies {
                implementation(kotlin("test-common"))
                implementation(kotlin("test-annotations-common"))
                // SEVEN-250: общий контракт хоста (commonTest/hostcontract) — runTest + реальное время
                implementation(libs.kotlinx.coroutines.test)
            }
        }
        val jvmMain by getting {}
        val jvmTest by getting {
            dependencies {
                implementation(kotlin("test"))
                implementation(kotlin("test-junit"))
                implementation(libs.kotlinx.coroutines.test)
                // SEVEN-220: встроенный брокер для интеграционных тестов нового клиента (T05c)
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
        val mingwX64Main by getting {
            dependsOn(posixMain)
        }
        val linuxX64Main by getting {
            dependsOn(posixMain)
        }
        val linuxArm64Main by getting {
            dependsOn(posixMain)
        }
        val iosX64Main by getting {
            dependsOn(posixMain)
        }
        val iosArm64Main by getting {
            dependsOn(posixMain)
        }
        val iosSimulatorArm64Main by getting {
            dependsOn(posixMain)
        }
        val macosX64Main by getting {
            dependsOn(posixMain)
        }
        val macosArm64Main by getting {
            dependsOn(posixMain)
        }
        val tvosX64Main by getting {
            dependsOn(posixMain)
        }
        val tvosArm64Main by getting {
            dependsOn(posixMain)
        }
        val tvosSimulatorArm64Main by getting {
            dependsOn(posixMain)
        }
        val watchosX64Main by getting {
            dependsOn(posixMain)
        }
        val watchosArm32Main by getting {
            dependsOn(posixMain)
        }
        val watchosArm64Main by getting {
            dependsOn(posixMain)
        }
        val watchosSimulatorArm64Main by getting {
            dependsOn(posixMain)
        }
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

