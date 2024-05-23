import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DefaultSqlDialectTest {

    @Test
    fun minus() {
        val left = "left"
        val right = "right"
        val expected = "(left - right)"
        val actual = with(DefaultSqlDialect) { left - right }
        assertEquals(expected, actual)
    }

    @Test
    fun div() {
        val left = "left"
        val right = "right"
        val expected = "(left / right)"
        val actual = with(DefaultSqlDialect) { left / right }
        assertEquals(expected, actual)
    }

    @Test
    fun asDouble() {
        val col = "column"
        val expected = "CAST(column AS DOUBLE)"
        val actual = with(DefaultSqlDialect) { col.asDouble() }
        assertEquals(expected, actual)
    }

    @Test
    fun avg() {
        val col = "column"
        val expected = "AVG(column)"
        val actual = with(DefaultSqlDialect) { avg(col) }
        assertEquals(expected, actual)
    }

    @Test
    fun sum() {
        val col = "column"
        val expected = "SUM(column)"
        val actual = with(DefaultSqlDialect) { sum(col) }
        assertEquals(expected, actual)
    }

    @Test
    fun count() {
        val expected = "COUNT(*)"
        val actual = with(DefaultSqlDialect) { count() }
        assertEquals(expected, actual)
    }

    @Test
    fun countWhen() {
        val col = "column"
        val expected = "SUM(CASE WHEN (column) THEN 1 ELSE 0 END)"
        val actual = with(DefaultSqlDialect) { countWhen(col) }
        assertEquals(expected, actual)
    }

    @Test
    fun countDistinct() {
        val col = "column"
        val expected = "COUNT(DISTINCT(column))"
        val actual = with(DefaultSqlDialect) { countDistinct(col) }
        assertEquals(expected, actual)
    }

    @Test
    fun isNull() {
        val col = "column"
        val expected = "(column IS NULL)"
        val actual = with(DefaultSqlDialect) { isNull(col) }
        assertEquals(expected, actual)
    }

    @ParameterizedTest
    @MethodSource("selectParams")
    fun select(selectColumns : List<String>, from: String, groupColumns: List<String>, expected: String) {
        val actual = with(DefaultSqlDialect) { select(selectColumns, from, groupColumns) }
        assertEquals(expected, actual)
    }

    fun selectParams() : List<Arguments> = listOf(
        listOf(listOf("column", "column2"), "table", listOf("column3", "column4"), "SELECT column, column2 FROM (table) GROUP BY column3, column4"),
        listOf(listOf("column", "column2"), "table", listOf<String>(), "SELECT column, column2 FROM (table)")
    ).map {
        Arguments.of(*it.toTypedArray())
    }
}