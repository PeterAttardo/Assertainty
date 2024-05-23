import com.attardo.datavalidation.core.*
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockkStatic
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.reflect.KFunction
import kotlin.test.assertEquals

class DataAssertionsTest {
    object testMethods : ColumnMethods<String> {
        override fun String.minus(other: String): String = "($this - $other)"
        override fun String.div(other: String): String = "($this / $other)"
        override fun String.asDouble(): String = "($this as Double)"
        override fun avg(column: String): String = "avg($column)"
        override fun sum(column: String): String = "sum($column)"
        override fun count(): String = "count()"
        override fun isNull(column: String): String = "($column is NULL)"
        override fun countDistinct(column: String): String = "count_distinct($column)"
        override fun countWhen(column: String): String = "count_when($column)"
    }

    val doubleThreshold = 10.0
    val longThreshold = 10L
    val double1 = 1.0
    val long1 = 1L
    val double2 = 2.0
    val long2 = 2L

    fun mockkAssertions() {
        mockkStatic(::assertGreaterThanOrEqualTo)
        mockkStatic(::assertLessThanOrEqualTo)
        val equalsFunc : (Any, Any, String) -> Unit = ::assertEquals
        mockkStatic(equalsFunc as KFunction<*>)
        every { assertGreaterThanOrEqualTo(any(), any()) } returns Unit
        every { assertLessThanOrEqualTo(any(), any()) } returns Unit
        every { assertEquals<Any>(any(), any()) } returns Unit
    }

    @BeforeEach
    fun beforeEach() {
        clearAllMocks()
    }

    @Test
    fun testMinLiteralDataAssertion() {
        val assertion = MinLiteralDataAssertion("column", doubleThreshold)
        assertEquals("column", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), double1)
        verify { assertGreaterThanOrEqualTo(doubleThreshold, double1) }

    }

    @Test
    fun testMaxLiteralDataAssertion() {
        val assertion = MaxLiteralDataAssertion("column", doubleThreshold)
        assertEquals("column", assertion.metricColumn)

        mockkAssertions ()
        assertion.assert(mapOf(), double1)
        verify { assertLessThanOrEqualTo(doubleThreshold, double1) }

    }

    @Test
    fun testEqualsLiteralDataAssertion() {
        val assertion = EqualsLiteralDataAssertion("column", doubleThreshold)
        assertEquals("column", assertion.metricColumn)

        mockkAssertions ()
        assertion.assert(mapOf(), double1)
        verify { assertEquals(doubleThreshold, double1) }

    }

    @Test
    fun testEqualsDataAssertion() {
        val assertion = EqualsDataAssertion<String, Double>("column1", "column2")
        assertEquals("column1", assertion.metricColumn1)
        assertEquals("column2", assertion.metricColumn2)

        mockkAssertions ()
        assertion.assert(mapOf(), double1, double2)
        verify { assertEquals(double1, double2) }

    }



    @Test
    fun testMinSumDataAssertion() {
        val assertion = MinSumDataAssertion("column", doubleThreshold, testMethods)
        assertEquals("sum(column)", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), double1)
        verify { assertGreaterThanOrEqualTo(doubleThreshold, double1) }
    }

    @Test
    fun testMaxSumDataAssertion() {
        val assertion = MaxSumDataAssertion("column", doubleThreshold, testMethods)
        assertEquals("sum(column)", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), double1)
        verify { assertLessThanOrEqualTo(doubleThreshold, double1) }
    }

    @Test
    fun testMinAvgDataAssertion() {
        val assertion = MinAvgDataAssertion("column", doubleThreshold, testMethods)
        assertEquals("avg(column)", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), double1)
        verify { assertGreaterThanOrEqualTo(doubleThreshold, double1) }
    }

    @Test
    fun testMaxAvgDataAssertion() {
        val assertion = MaxAvgDataAssertion("column", doubleThreshold, testMethods)
        assertEquals("avg(column)", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), double1)
        verify { assertLessThanOrEqualTo(doubleThreshold, double1) }
    }

    @Test
    fun testMinCountDataAssertion() {
        val assertion = MinCountDataAssertion(doubleThreshold, testMethods)
        assertEquals("count()", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), double1)
        verify { assertGreaterThanOrEqualTo(doubleThreshold, double1) }
    }

    @Test
    fun testMaxCountDataAssertion() {
        val assertion = MaxCountDataAssertion(doubleThreshold, testMethods)
        assertEquals("count()", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), double1)
        verify { assertLessThanOrEqualTo(doubleThreshold, double1) }
    }

    @Test
    fun testMinConditionCountDataAssertion() {
        val assertion = MinConditionCountDataAssertion("column", doubleThreshold, testMethods)
        assertEquals("count_when(column)", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), double1)
        verify { assertGreaterThanOrEqualTo(doubleThreshold, double1) }
    }

    @Test
    fun testMaxConditionCountDataAssertion() {
        val assertion = MaxConditionCountDataAssertion("column", doubleThreshold, testMethods)
        assertEquals("count_when(column)", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), double1)
        verify { assertLessThanOrEqualTo(doubleThreshold, double1) }
    }

    @Test
    fun testNeverDataAssertion() {
        val assertion = NeverDataAssertion("column", testMethods)
        assertEquals("count_when(column)", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), long1)
        verify { assertEquals(0L, long1) }
    }

    @Test
    fun testAlwaysDataAssertion() {
        val assertion = AlwaysDataAssertion("column", testMethods)
        assertEquals("count()", assertion.metricColumn1)
        assertEquals("count_when(column)", assertion.metricColumn2)

        mockkAssertions()
        assertion.assert(mapOf(), long1, long2)
        verify { assertEquals(long1, long2) }
    }

    @Test
    fun testMinDistinctCountDataAssertion() {
        val assertion = MinDistinctCountDataAssertion("column", doubleThreshold, testMethods)
        assertEquals("count_distinct(column)", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), double1)
        verify { assertGreaterThanOrEqualTo(doubleThreshold, double1) }
    }

    @Test
    fun testMaxDistinctCountDataAssertion() {
        val assertion = MaxDistinctCountDataAssertion("column", doubleThreshold, testMethods)
        assertEquals("count_distinct(column)", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), double1)
        verify { assertLessThanOrEqualTo(doubleThreshold, double1) }
    }

    @Test
    fun testMinConditionRatioDataAssertion() {
        val assertion = MinConditionRatioDataAssertion("column", doubleThreshold, testMethods)
        assertEquals("((count_when(column) as Double) / count())", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), double1)
        verify { assertGreaterThanOrEqualTo(doubleThreshold, double1) }
    }

    @Test
    fun testMaxConditionRatioDataAssertion() {
        val assertion = MaxConditionRatioDataAssertion("column", doubleThreshold, testMethods)
        assertEquals("((count_when(column) as Double) / count())", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), double1)
        verify { assertLessThanOrEqualTo(doubleThreshold, double1) }
    }

    open class NeverNullDataAssertion<Column>(
        column: Column,
        methods: ColumnMethods<Column>,
        description: String? = null
    ) : NeverDataAssertion<Column>(methods.isNull(column), methods, description)


    @Test
    fun testNeverNullDataAssertion() {
        val assertion = NeverNullDataAssertion("column", testMethods)
        assertEquals("count_when((column is NULL))", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), long1)
        verify { assertEquals(0L, long1) }
    }

    @Test
    fun testMaxDuplicateCountDataAssertion() {
        val assertion = MaxDuplicateCountDataAssertion("column", longThreshold, testMethods)
        assertEquals("(count() - count_distinct(column))", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), long1)
        verify { assertLessThanOrEqualTo(longThreshold, long1) }
    }

    @Test
    fun testMaxDuplicateRatioDataAssertion() {
        val assertion = MaxDuplicateRatioDataAssertion("column", doubleThreshold, testMethods)
        assertEquals("(((count() - count_distinct(column)) as Double) / count())", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), double1)
        verify { assertLessThanOrEqualTo(doubleThreshold, double1) }
    }


    @Test
    fun testUniqueDataAssertion() {
        val assertion = UniqueDataAssertion("column", testMethods)
        assertEquals("(count() - count_distinct(column))", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), long1)
        verify { assertLessThanOrEqualTo(0L, long1) }
    }


    @Test
    fun testMaxIsNullRatioDataAssertion() {
        val assertion = MaxIsNullRatioDataAssertion("column", doubleThreshold, testMethods)
        assertEquals("((count_when((column is NULL)) as Double) / count())", assertion.metricColumn)

        mockkAssertions()
        assertion.assert(mapOf(), double1)
        verify { assertLessThanOrEqualTo(doubleThreshold, double1) }
    }
}