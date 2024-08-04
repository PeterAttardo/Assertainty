package junit

import com.attardo.assertainty.junit.assertaintyTestFactory
import com.attardo.assertainty.spark.assert
import org.apache.spark.SparkConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.jetbrains.kotlinx.spark.api.eq
import org.jetbrains.kotlinx.spark.api.lt
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.TestInstance
import java.util.*


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SparkTest {
    lateinit var sparkSession: SparkSession

    @BeforeAll
    fun setup() {
        sparkSession = SparkSession
            .builder()
            .master(SparkConf().get("spark.master", "local[*]"))
            .appName("Assertainty")
            .getOrCreate()
    }

    @AfterAll
    fun teardown() {
        sparkSession.stop()
    }

    @TestFactory
    fun factory() = assertaintyTestFactory {
        "test1" {
            val table = sparkSession.read()
                .option("driver", "org.sqlite.JDBC")
                .jdbc("jdbc:sqlite::resource:chinook.db", "tracks", Properties())
            table.assert {
                +"AlbumId"
                +"Composer"
                minSum(Column("Milliseconds"), 30 * 60 * 1000, description = "Album length > 30 min")
                maxSum(Column("Milliseconds"), 50 * 60 * 1000, description = "Album length < 50 min")
            }
        }
        "test2" {
            val table = sparkSession.read()
                .option("driver", "org.sqlite.JDBC")
                .jdbc("jdbc:sqlite::resource:chinook.db", "tracks", Properties())
            table.assert {
                always(Column("UnitPrice") eq 0.99, description = "All priced at $0.99")
                neverNull(Column("Composer"))
                always(functions.length(Column("Name")) lt 15, description = "Name not excessively long")
            }
        }
    }
}

