package junit

import com.attardo.datavalidation.junit.dataAssertionTestFactory
import com.attardo.datavalidation.spark.assert
import org.apache.spark.SparkConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.jetbrains.kotlinx.spark.api.eq
import org.jetbrains.kotlinx.spark.api.gt
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.TestInstance


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SparkTest {
    lateinit var sparkSession: SparkSession

    @BeforeAll
    fun setup() {
        sparkSession = SparkSession
            .builder()
            .master(SparkConf().get("spark.master", "local[*]"))
            .appName("DataAssertions")
            .getOrCreate()
    }

    @AfterAll
    fun teardown() {
        sparkSession.stop()
    }

    @TestFactory
    fun factory() = dataAssertionTestFactory {
        val path = javaClass.getResource("/organizations-10000.csv")?.path
        "test1" {
            val table = sparkSession.read().option("header", true).csv(path)
            table.assert {
                +"Industry"

                min_avg(Column("Number of employees"), 30, description = "Employees > 30")
                max_avg(Column("Number of employees"), 50, description = "Employees < 50")
                max_when(Column("Number of employees") gt 400, 50, description = "No more than 50 large companies")
            }
        }
        "test2" {
            val table = sparkSession.read().option("header", true).csv(path)
            table.assert {
                always(Column("Website").startsWith("https://"), description = "Website secure")
                never_null(Column("Organization Id"))
                always(functions.length(Column("Organization Id")) eq 15, description = "ID valid")
            }
        }
    }
}

