import org.gradle.kotlin.dsl.support.uppercaseFirstChar
import java.io.FileInputStream
import java.util.*

plugins {
    kotlin("jvm") version "1.9.22"
    `maven-publish`
    signing
    id("com.gradleup.nmcp") version "0.0.7"

}

group = "io.github.peterattardo.assertainty"
version = "0.2.0"

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

val prop = Properties().apply {
    load(FileInputStream(File(rootProject.rootDir, "local.properties")))
}

nmcp {
    publishAllProjectsProbablyBreakingProjectIsolation {
        username = prop.getProperty("SONATYPE_USERNAME")
        password = prop.getProperty("SONATYPE_TOKEN")
        publicationType = "USER_MANAGED"
//        publicationType = "AUTOMATIC"
    }
}

subprojects {
    apply(plugin = "kotlin")
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")
    apply(plugin = "signing")

    repositories {
        mavenCentral()
    }

    tasks.jar {
        archiveBaseName.set(buildString {
            append(project.name)
            if(project.path != ":core") append("-plugin")
        })
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
        java {
            withJavadocJar()
            withSourcesJar()
        }
        signing {
            project.setProperty("signing.gnupg.keyName", prop.getProperty("SIGNING_KEY_ID"))
            project.setProperty("signing.gnupg.passphrase", prop.getProperty("SIGNING_PASSWORD"))

            useGpgCmd()
            sign(publishing.publications)
        }
        publishing {
            publications {
                create<MavenPublication>(project.name) {
                    groupId = rootProject.group.toString()
                    artifactId = buildString {
                        append(project.name)
                        if(project.path != ":core") append("-plugin")
                    }
                    version = rootProject.version.toString()

                    from(components["java"])
                    pom {
                        name = buildString {
                            append("Assertainty")
                            append(" - ")
                            append(project.name.uppercaseFirstChar())
                            if(project.path != ":core") append(" Plugin")
                        }
                        description = "Kotlin library for writing data quality tests."
                        url = "https://github.com/PeterAttardo/Assertainty"
                        licenses {
                            license {
                                name = "The Apache License, Version 2.0"
                                url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
                            }
                        }
                        developers {
                            developer {
                                id = "PeterAttardo"
                                name = "Peter Attardo"
                                url = "https://github.com/PeterAttardo/"
                            }
                        }
                        scm {
                            connection = "scm:git:git://github.com/PeterAttardo/Assertainty.git"
                            developerConnection = "scm:git:ssh://github.com:PeterAttardo/Assertainty.git"
                            url = "https://github.com/PeterAttardo/Assertainty/tree/master"
                        }
                    }
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