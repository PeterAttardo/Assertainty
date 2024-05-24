import com.attardo.datavalidation.spark.DelayedAggregation
import com.attardo.datavalidation.spark.agg
import com.attardo.datavalidation.spark.singleQuery
import org.apache.spark.SparkConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.sum
import org.jetbrains.kotlinx.spark.api.dfOf
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle
import kotlin.test.assertContentEquals

@TestInstance(Lifecycle.PER_CLASS)
class DelayedAggregationKtTest {
    lateinit var spark: SparkSession

    @BeforeAll
    fun setup() {
        spark = SparkSession
            .builder()
            .master(SparkConf().get("spark.master", "local[*]"))
            .appName("Assertainty")
            .getOrCreate()
    }

    @AfterAll
    fun teardown() {
        spark.stop()
    }

    @Test
    fun agg() {
        val table = testDataFrame()
        table.groupBy(column1).agg(listOf(
            DelayedAggregation(avg(metric1).`as`("avg_metric1")){
                assertContentEquals(
                    arrayOf("column1", "avg_metric1"),
                    it.columns()
                )
            }
        ))
        table.groupBy(column1, column2).agg(listOf(
            DelayedAggregation(avg(metric1).`as`("avg_metric1")){
                assertContentEquals(
                    arrayOf("column1", "column2", "avg_metric1"),
                    it.columns()
                )
            },
            DelayedAggregation(avg(metric1).`as`("avg_metric1"), sum(metric2).`as`("sum_metric2")){
                assertContentEquals(
                    arrayOf("column1", "column2", "avg_metric1", "sum_metric2"),
                    it.columns()
                )
            }
        ))
    }

    @Test
    fun singleQuery() {
        val table = testDataFrame()
        table.groupBy(column1).singleQuery {
            agg(sum(metric1)){
                assertContentEquals(
                    arrayOf("column1", "sum(metric1)"),
                    it.columns()
                )
            }
        }
        table.groupBy(column1, column2).singleQuery {
            agg(avg(metric1)){
                assertContentEquals(
                    arrayOf("column1", "column2", "avg(metric1)"),
                    it.columns()
                )
            }
            agg(avg(metric1), sum(metric2)){
                assertContentEquals(
                    arrayOf("column1", "column2", "avg(metric1)", "sum(metric2)"),
                    it.columns()
                )
            }
        }
    }

    val column1 = Column("column1")
    val column2 = Column("column2")
    val column3 = Column("column3")
    val metric1 = Column("metric1")
    val metric2 = Column("metric2")
    val metric3 = Column("metric3")

    fun testDataFrame(): Dataset<Row> {
        return listOf(
            listOf("")
        ).toDF(spark, "column1", "column2", "column3", "metric1", "metric2", "metric3")
    }

}

fun List<List<Any>>.toDF(spark: SparkSession, vararg columnNames: String) : Dataset<Row> {
    return spark.dfOf(columnNames as Array<String>,
        TestRow("a", "alpha", "one", 1, 1.0, 1),
        TestRow("a", "alpha", "one", 1, 1.0, 1),
        TestRow("a", "alpha", "one", 1, 1.0, 1),
        TestRow("a", "alpha", "one", 1, 1.0, 1),
        TestRow("a", "alpha", "one", 1, 1.0, 1),
        TestRow("a", "alpha", "one", 1, 1.0, 1),
        TestRow("a", "alpha", "one", 1, 1.0, 1)
    )
}

data class TestRow(val column1: String, val column2: String, val column3: String, val metric1: Int, val metric2: Double, val metric3: Long)