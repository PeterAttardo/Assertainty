plugins {
    kotlin("jvm")
}

dependencies {
    testImplementation("org.jetbrains.kotlin:kotlin-test")

    testImplementation(project(":junit"))
    testImplementation(project(":kotest"))
    testImplementation(project(":spark"))
    testImplementation(project(":rawsql"))
    testImplementation(project(":ktorm"))
    testImplementation(project(":exposed"))
    testImplementation("org.jetbrains.exposed:exposed-jdbc:0.50.1")

    testImplementation("org.xerial:sqlite-jdbc:3.44.1.0")
}