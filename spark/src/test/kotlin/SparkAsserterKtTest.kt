import com.attardo.datavalidation.core.DataAssertion
import com.attardo.datavalidation.core.MaxConditionCountDataAssertion
import com.attardo.datavalidation.core.MinCountDataAssertion
import com.attardo.datavalidation.spark.SparkColumnMethods
import com.attardo.datavalidation.spark.computeSeparateQueries
import com.attardo.datavalidation.spark.computeSingleQuery
import com.attardo.datavalidation.spark.getName
import io.mockk.MockKMatcherScope
import io.mockk.every
import io.mockk.spyk
import io.mockk.verify
import org.apache.spark.SparkConf
import org.apache.spark.sql.*
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataTypes.*
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.jetbrains.kotlinx.spark.api.gt
import org.jetbrains.kotlinx.spark.api.lit
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import kotlin.test.assertContentEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SparkAsserterKtTest {
    lateinit var spark: SparkSession

    @BeforeAll
    fun setup() {
        spark = SparkSession
            .builder()
            .master(SparkConf().get("spark.master", "local[*]"))
            .appName("DataAssertions")
            .getOrCreate()
    }

    @AfterAll
    fun teardown() {
        spark.stop()
    }

    object table {
        val column1 = Column("column1")
        val column2 = Column("column2")
        val column3 = Column("column3")
        val metric1 = Column("metric1")
        val metric2 = Column("metric2")

        val schema = listOf(column1, column2, column3, metric1, metric2)
    }

    @ParameterizedTest
    @MethodSource("computeArgs")
    fun computeSingleQuery(table: Dataset<Row>, groupColumns: List<Column>, results: Map<DataAssertion<Column>, List<List<Any>>>) {
        val allMetrics: List<Column> = results.flatMap { (assertion, _) -> assertion.metricColumns  }

        lateinit var relationalGroupedDataset : RelationalGroupedDataset
        val ds = spyk(table) {
            every { this@spyk.groupBy(*anyVararg()) } answers {
                spyk(invocation.originalCall() as RelationalGroupedDataset).also { relationalGroupedDataset = it }
            }
        }

        val expected = expected(groupColumns, results)
        val actual = computeSingleQuery(ds, groupColumns, results.keys.toList() )
        assertEquals(expected, actual)

        verify(exactly = 1) { ds.groupBy(*groupColumns.toTypedArray()) }
        verify(exactly = 1){ relationalGroupedDataset.agg(a(allMetrics.first()), *a(allMetrics.drop(1).toTypedArray())) }
    }

    @ParameterizedTest
    @MethodSource("computeArgs")
    fun compute(table: Dataset<Row>, groupColumns: List<Column>, results: Map<DataAssertion<Column>, List<List<Any>>>) {
        lateinit var relationalGroupedDataset : RelationalGroupedDataset
        val ds = spyk(table) {
            every { this@spyk.groupBy(*anyVararg()) } answers {
                spyk(invocation.originalCall() as RelationalGroupedDataset).also { relationalGroupedDataset = it }
            }
        }

        val expected = expected(groupColumns, results)
        val actual = computeSeparateQueries(ds, groupColumns, results.keys.toList() )
        assertContentEquals(expected.asIterable(), actual.asIterable())

        verify(exactly = 1) { ds.groupBy(*groupColumns.toTypedArray()) }
        results.forEach { assertion, _ ->
            verify(exactly = 1){ relationalGroupedDataset.agg(assertion.metricColumns.first(), *assertion.metricColumns.drop(1).toTypedArray()) }
        }
    }

    private fun computeArgs() : List<Arguments> {
        val df = listOf(
            listOf("alpha", "A", "a", 22, 2),
            listOf("beta", "A", "a", 44, 2),
            listOf("beta", "A", "a", 1, 2)
        ).toDf(spark, *table.schema.map(Column::getName).toTypedArray())
        return listOf(
            listOf(
                listOf(table.column1),
                mapOf(
                    MinCountDataAssertion(2, SparkColumnMethods) to listOf(
                        listOf("alpha", 1L),
                        listOf("beta", 2L)
                    ),
                    MaxConditionCountDataAssertion(table.metric1 gt lit(20), 15, SparkColumnMethods) to listOf(
                        listOf("alpha", 1L),
                        listOf("beta", 1L)
                    )
                )
            ),
            listOf(
                listOf<Column>(),
                mapOf(
                    MinCountDataAssertion(2, SparkColumnMethods) to listOf(
                        listOf(3L)
                    ),
                    MaxConditionCountDataAssertion(table.metric1 gt lit(20), 15, SparkColumnMethods) to listOf(
                        listOf(2L)
                    )
                )
            )
        ).map { Arguments.of(df, *it.toTypedArray()) }
    }
}

fun MockKMatcherScope.a(column: Column) : Column {
    return match<Column> { it.toString().substringBefore(" AS") == column.toString().substringBefore(" AS") }
}

fun MockKMatcherScope.a(column: Array<Column>) : Array<Column> {
    return varargAll<Column> { it.toString().substringBefore(" AS") == column[position].toString().substringBefore(" AS") }
}

fun List<List<Any>>.toDf(spark: SparkSession, vararg names: String): Dataset<Row> =
    spark.createDataFrame(map { GenericRow(it.toTypedArray()) }, this[0].structType(*names))

fun List<Any>.structType(vararg names: String) : StructType {
    val zipped = names zip map {
        when (it) {
            is Int -> IntegerType
            is Long -> LongType
            is Double -> DoubleType
            is Float -> FloatType
            is String -> StringType
            else -> error("Unsupported type")
        }
    }
    return StructType(zipped.map { StructField(it.first, it.second, true, Metadata.empty()) }.toTypedArray())
}