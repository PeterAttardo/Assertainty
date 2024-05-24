package kotest

import com.attardo.assertainty.kotest.invoke
import com.attardo.assertainty.spark.assert
import io.kotest.core.spec.style.StringSpec
import org.apache.spark.SparkConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.jetbrains.kotlinx.spark.api.eq
import org.jetbrains.kotlinx.spark.api.gt
import org.jetbrains.kotlinx.spark.api.lt
import java.util.*

class SparkTest : StringSpec({
    lateinit var sparkSession: SparkSession
    beforeSpec {
        sparkSession = SparkSession.builder()
            .master(SparkConf().get("spark.master", "local[*]"))
            .appName("Assertainty")
            .getOrCreate()
    }
    afterSpec {
        sparkSession.stop()
    }

    "test1"(this) {
        val table = sparkSession.read()
            .option("driver", "org.sqlite.JDBC")
            .jdbc("jdbc:sqlite::resource:chinook.db", "tracks", Properties())
        table.assert {
            +"AlbumId"
            +"Composer"
            min_sum(Column("Milliseconds"), 30 * 60 * 1000, description = "Album length > 30 min")
            max_sum(Column("Milliseconds"), 50 * 60 * 1000, description = "Album length < 50 min")
        }
    }
    "test2"(this) {
        val table = sparkSession.read()
            .option("driver", "org.sqlite.JDBC")
            .jdbc("jdbc:sqlite::resource:chinook.db", "tracks", Properties())
        table.assert {
            always(Column("UnitPrice") eq 0.99, description = "All priced at $0.99")
            never_null(Column("Composer"))
            always(functions.length(Column("Name")) lt 15, description = "Name not excessively long")
        }
    }
})