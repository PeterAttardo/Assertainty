plugins {
    kotlin("jvm")
}

dependencies {
    api("org.jetbrains.exposed:exposed-core:0.50.1")
    testImplementation("org.jetbrains.exposed:exposed-jdbc:0.50.1")
}