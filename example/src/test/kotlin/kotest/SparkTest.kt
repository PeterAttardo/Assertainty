package kotest

import com.attardo.datavalidation.kotest.invoke
import com.attardo.datavalidation.spark.assert
import io.kotest.core.spec.style.StringSpec
import org.apache.spark.SparkConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.jetbrains.kotlinx.spark.api.eq
import org.jetbrains.kotlinx.spark.api.gt

class SparkTest : StringSpec({
    lateinit var sparkSession: SparkSession
    beforeSpec {
        sparkSession = SparkSession.builder()
            .master(SparkConf().get("spark.master", "local[*]"))
            .appName("DataAssertions")
            .getOrCreate()
    }
    afterSpec {
        sparkSession.stop()
    }

    val path = javaClass.getResource("/organizations-10000.csv")?.path

    "test1"(this) {
        val table = sparkSession.read().option("header", true).csv(path)
        table.assert {
            +"Industry"

            min_avg(Column("Number of employees"), 30, description = "Employees > 30")
            max_avg(Column("Number of employees"), 50, description = "Employees < 50")
            max_when(Column("Number of employees") gt 400, 50, description = "No more than 50 large companies")
        }
    }
    "test2"(this) {
        val table = sparkSession.read().option("header", true).csv(path)
        table.assert {
            always(Column("Website").startsWith("https://"), description = "Website secure")
            never_null(Column("Organization Id"))
            always(functions.length(Column("Organization Id")) eq 15, description = "ID valid")
        }
    }
})