import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.ktorm.expression.*
import org.ktorm.schema.*

class KtormColumnMethodsTest {
    val table = Table("table")

    @Test
    fun minus() {
        val left = table.int("left")
        val right = table.int("right")
        val expected = BinaryExpression(BinaryExpressionType.MINUS, left.asExpression(), right.asExpression(), left.sqlType)
        val actual = with(KtormColumnMethods) { left - right }
        assertEquals(expected, actual)
    }

    @Test
    fun div() {
        val left = table.int("left")
        val right = table.int("right")
        val expected = BinaryExpression(BinaryExpressionType.DIV, left.asExpression(), right.asExpression(), left.sqlType)
        val actual = with(KtormColumnMethods) { left / right }
        assertEquals(expected, actual)
    }

    @Test
    fun asDouble() {
        val col = table.int("column")
        val expected = CastingExpression(col.asExpression(), DoubleSqlType)
        val actual = with(KtormColumnMethods) { col.asDouble() }
        assertEquals(expected, actual)
    }

    @Test
    fun avg() {
        val col = table.int("column")
        val expected = AggregateExpression(AggregateType.AVG, col.asExpression(), false, DoubleSqlType)
        val actual = with(KtormColumnMethods) { avg(col) }
        assertEquals(expected, actual)
    }

    @Test
    fun sum() {
        val col = table.int("column")
        val expected = AggregateExpression(AggregateType.SUM, col.asExpression(), false, col.sqlType)
        val actual = with(KtormColumnMethods) { sum(col) }
        assertEquals(expected, actual)
    }

    @Test
    fun count() {
        val expected = AggregateExpression(AggregateType.COUNT, null, false, IntSqlType)
        val actual = with(KtormColumnMethods) { count() }
        assertEquals(expected, actual)
    }

    @Test
    fun countWhen() {
        val col = table.boolean("column")
        val expected = AggregateExpression(
            AggregateType.SUM,
            CaseWhenExpression(
                null,
                listOf(col.asExpression() to ArgumentExpression(1, IntSqlType)),
                ArgumentExpression(0, IntSqlType),
                IntSqlType
            ),
            false,
            IntSqlType
        )
        val actual = with(KtormColumnMethods) { countWhen(col) }
        assertEquals(expected, actual)
    }

    @Test
    fun countDistinct() {
        val col = table.int("column")
        val expected = AggregateExpression(AggregateType.COUNT, col.asExpression(), true, IntSqlType)
        val actual = with(KtormColumnMethods) { countDistinct(col) }
        assertEquals(expected, actual)
    }

    @Test
    fun isNull() {
        val col = table.int("column")
        val expected = UnaryExpression(UnaryExpressionType.IS_NULL, col.asExpression(), BooleanSqlType)
        val actual = with(KtormColumnMethods) { isNull(col) }
        assertEquals(expected, actual)
    }
}