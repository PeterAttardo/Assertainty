import com.attardo.assertainty.spark.SparkColumnMethods
import com.attardo.assertainty.spark.toKotlinList
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.test.assertContentEquals

class SparkColumnMethodsTest {
    @Test
    fun minus() {
        val left = Column("left")
        val right = Column("right")
        assertEquals("(left - right)", with(SparkColumnMethods){ left - right }.toString())
    }

    @Test
    fun div() {
        val left = Column("left")
        val right = Column("right")
        assertEquals("(left / right)", with(SparkColumnMethods){ left / right }.toString())
    }

    @Test
    fun asDouble() {
        val col = Column("column")
        assertEquals("CAST(column AS DOUBLE)", with(SparkColumnMethods){ col.asDouble() }.toString())
    }

    @Test
    fun avg() {
        val col = Column("column")
        assertEquals("avg(column)", with(SparkColumnMethods){ avg(col)}.toString())
    }

    @Test
    fun sum() {
        val col = Column("column")
        assertEquals("sum(column)", with(SparkColumnMethods){ sum(col)}.toString())
    }

    @Test
    fun count() {
        val col = with(SparkColumnMethods) { count() }
        assertEquals("count(1)", col.toString())
    }

    @Test
    fun countWhen() {
        val col = Column("column")
        assertEquals("sum(CASE WHEN column THEN 1 ELSE 0 END)", with(SparkColumnMethods){ countWhen(col)}.toString())
    }

    @Test
    fun countDistinct() {
        val col = with(SparkColumnMethods) { countDistinct(Column("column")) }
        val expr = col.expr()
        assertEquals("count(column)", col.toString())
        assertTrue(expr is UnresolvedFunction)
        assertTrue((expr as UnresolvedFunction).isDistinct)
        assertContentEquals(listOf("count"), (expr as UnresolvedFunction).nameParts().toKotlinList())
    }

    @Test
    fun isNull() {
        val col = Column("column")
        assertEquals("(column IS NULL)", with(SparkColumnMethods){ isNull(col) }.toString())
    }
}