plugins {
    kotlin("jvm") version "1.9.22"
    `maven-publish`
}

group = "com.attardo.assertainty"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.jetbrains.kotlin:kotlin-test")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}

subprojects {
    apply(plugin = "kotlin")
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")

    repositories {
        mavenCentral()
    }

    group = rootProject.group
    version = rootProject.version

    if(path != ":example") {
        dependencies {
            testImplementation("org.jetbrains.kotlin:kotlin-test")
            testImplementation("io.mockk:mockk:1.13.11")
            testImplementation("org.junit.jupiter:junit-jupiter-params:5.10.2")
            testImplementation("org.xerial:sqlite-jdbc:3.44.1.0")
            if (path != ":core") {
                api(project(":core"))
                testImplementation(testFixtures(project(":core")))
            }
        }
        publishing {
            publications {
                create<MavenPublication>(project.name) {
                    groupId = rootProject.group.toString()
                    artifactId = project.name
                    version = rootProject.version.toString()

                    from(components["java"])
                }
            }
        }
    }

    tasks.test {
        useJUnitPlatform()
    }
    kotlin {
        jvmToolchain(15)
    }
}