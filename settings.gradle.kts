plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.5.0"
}
rootProject.name = "KDataAssertions"
include("spark")
include("core")
include("example")
include("junit")
include("exposed")
//include("dataframe")
include("kotest")
include("ktorm")
include("rawsql")
