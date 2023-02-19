plugins {
    id("org.jetbrains.kotlin.jvm") version "1.8.10"
    id("io.ktor.plugin") version "2.2.3"
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.ton:ton-kotlin:0.2.14")
    implementation("org.jetbrains.kotlinx:atomicfu:0.19.0")
    implementation("io.ktor:ktor-client-cio:2.2.3")
}

testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useKotlinTest()
        }
    }
}

jib {
    to.image = "kton"
}

application {
    mainClass.set("io.github.andreypfau.kton.AppKt")
}

ktor {
    docker {
        jreVersion.set(io.ktor.plugin.features.JreVersion.JRE_17)
        portMappings.set(
            listOf(
                io.ktor.plugin.features.DockerPortMapping(
                    80,
                    8080,
                    io.ktor.plugin.features.DockerPortMappingProtocol.TCP
                )
            )
        )
        providers.gradleProperty("docker.imageName").orNull?.let {
            localImageName.set(it)
        }
        providers.gradleProperty("docker.imageTag").orNull?.let {
            imageTag.set(it)
        }
    }
}
