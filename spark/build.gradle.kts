plugins {
    kotlin("jvm")
}

dependencies {
    api("org.jetbrains.kotlinx.spark:kotlin-spark-api_3.3.2_2.13:1.2.4")
    compileOnlyApi("org.apache.spark:spark-sql_2.13:3.3.2")
}