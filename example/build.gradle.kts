plugins {
    kotlin("jvm")
}

val assertaintyVersion = "0.1.0"
dependencies {
    testImplementation("org.jetbrains.kotlin:kotlin-test")

    testImplementation("io.github.peterattardo.assertainty:junit-plugin:$assertaintyVersion")
    testImplementation("io.github.peterattardo.assertainty:kotest-plugin:$assertaintyVersion")
    testImplementation("io.github.peterattardo.assertainty:spark-plugin:$assertaintyVersion")
    testImplementation("io.github.peterattardo.assertainty:rawsql-plugin:$assertaintyVersion")
    testImplementation("io.github.peterattardo.assertainty:ktorm-plugin:$assertaintyVersion")
    testImplementation("io.github.peterattardo.assertainty:exposed-plugin:$assertaintyVersion")
    testImplementation("org.jetbrains.exposed:exposed-jdbc:0.50.1")

    testImplementation("org.xerial:sqlite-jdbc:3.44.1.0")
}