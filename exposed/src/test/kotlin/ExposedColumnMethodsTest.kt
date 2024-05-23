import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExposedColumnMethodsTest {
    lateinit var db: Database

    @BeforeAll
    fun setup() {
        db = Database.connect(
            url = "jdbc:sqlite::resource:chinook.db",
            driver = "org.sqlite.JDBC"
        )
    }

    @Test
    fun minus() {
        transaction {
            val table = Table("table")
            val left = table.integer("left")
            val right = table.integer("right")
            val expected = MinusOp(left, right, left.columnType)
            val actual = with(ExposedColumnMethods) { left - right }
            assertEquals(expected, actual)
        }
    }

    @Test
    fun div() {
        transaction {
            val table = Table("table")
            val left = table.integer("left")
            val right = table.integer("right")
            val expected = DivideOp(left, right, left.columnType)
            val actual = with(ExposedColumnMethods) { left / right }
            assertEquals(expected, actual)
        }
    }

    @Test
    fun asDouble() {
        transaction {
            val table = Table("table")
            val col = table.integer("column")
            val expected = Cast(col, DoubleColumnType())
            val actual = with(ExposedColumnMethods) { col.asDouble() }
            assertEquals(expected, actual)
        }
    }

    @Test
    fun avg() {
        transaction {
            val table = Table("table")
            val col = table.integer("column")
            val expected = Avg(col, 2)
            val actual = with(ExposedColumnMethods) { avg(col) }
            assertEquals(expected, actual)
        }
    }

    @Test
    fun sum() {
        transaction {
            val table = Table("table")
            val col = table.integer("column")
            val expected = Sum(col, col.columnType)
            val actual = with(ExposedColumnMethods) { sum(col) }
            assertEquals(expected, actual)
        }
    }

    @Test
    fun count() {
        transaction {
            val table = Table("table")
            val expected = Count(LiteralOp(TextColumnType(), "*"))
            val actual = with(ExposedColumnMethods) { count() }
            assertEquals(expected, actual)
        }
    }

    @Test
    fun countWhen() {
        transaction {
            val table = Table("table")
            val col = table.bool("column")
            val expected = Sum(
                CaseWhenElse(
                    CaseWhen<Int>(null).When(
                        col,
                        LiteralOp(IntegerColumnType(), 1)
                    ),
                    LiteralOp(IntegerColumnType(), 0)
                ), IntegerColumnType()
            )
            val actual = with(ExposedColumnMethods) { countWhen(col) }
            assertEquals(expected, actual)
        }
    }

    @Test
    fun countDistinct() {
        transaction {
            val table = Table("table")
            val col = table.integer("column")
            val expected = Count(col, true)
            val actual = with(ExposedColumnMethods) { countDistinct(col) }
            assertEquals(expected, actual)
        }
    }

    @Test
    fun isNull() {
        transaction {
            val table = Table("table")
            val col = table.integer("column")
            val expected = IsNullOp(col)
            val actual = with(ExposedColumnMethods) { isNull(col) }
            assertEquals(expected, actual)
        }
    }
}