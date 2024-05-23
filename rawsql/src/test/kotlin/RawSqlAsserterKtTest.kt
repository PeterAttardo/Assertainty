import com.attardo.datavalidation.core.DataAssertion
import com.attardo.datavalidation.core.MaxConditionCountDataAssertion
import com.attardo.datavalidation.core.MinCountDataAssertion
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import kotlin.test.assertContentEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KtormAsserterKtTest {
    lateinit var db: SqlWrapper

    @BeforeAll
    fun setup() {
        db = JDBCSqlWrapper("jdbc:sqlite::resource:chinook.db")
    }

    @ParameterizedTest
    @MethodSource("computeArgs")
    fun compute(table: String, groupColumns: List<String>, results: Map<DataAssertion<String>, List<List<Any>>>) {
        val expected = expected(groupColumns, results)
        val actual = compute(db, table, groupColumns, results.keys.toList())
        assertContentEquals(expected.asIterable(), actual.asIterable())
    }

    private fun computeArgs() : List<Arguments> {
        val table = "tracks"
        return listOf(
            listOf(
                listOf("GenreId"),
                mapOf(
                    MinCountDataAssertion(100, DefaultSqlDialect) to listOf(
                        listOf(1, 1297),
                        listOf(2, 130),
                        listOf(3, 374),
                        listOf(4, 332),
                        listOf(5, 12),
                        listOf(6, 81),
                        listOf(7, 579),
                        listOf(8, 58),
                        listOf(9, 48),
                        listOf(10, 43),
                        listOf(11, 15),
                        listOf(12, 24),
                        listOf(13, 28),
                        listOf(14, 61),
                        listOf(15, 30),
                        listOf(16, 28),
                        listOf(17, 35),
                        listOf(18, 13),
                        listOf(19, 93),
                        listOf(20, 26),
                        listOf(21, 64),
                        listOf(22, 17),
                        listOf(23, 40),
                        listOf(24, 74),
                        listOf(25, 1),
                    ),
                    MaxConditionCountDataAssertion("Milliseconds > 30000", 15, DefaultSqlDialect) to listOf(
                        listOf(1, 1296),
                        listOf(2, 130),
                        listOf(3, 374),
                        listOf(4, 328),
                        listOf(5, 12),
                        listOf(6, 81),
                        listOf(7, 579),
                        listOf(8, 58),
                        listOf(9, 48),
                        listOf(10, 43),
                        listOf(11, 15),
                        listOf(12, 24),
                        listOf(13, 28),
                        listOf(14, 61),
                        listOf(15, 30),
                        listOf(16, 28),
                        listOf(17, 32),
                        listOf(18, 13),
                        listOf(19, 93),
                        listOf(20, 26),
                        listOf(21, 64),
                        listOf(22, 17),
                        listOf(23, 40),
                        listOf(24, 74),
                        listOf(25, 1),
                    )
                )
            ),
            listOf(
                listOf<String>(),
                mapOf(
                    MinCountDataAssertion(2, DefaultSqlDialect) to listOf(
                        listOf(3503)
                    ),
                    MaxConditionCountDataAssertion("Milliseconds > 30000", 15, DefaultSqlDialect) to listOf(
                        listOf(3495)
                    )
                )
            )
        ).map { Arguments.of(table, *it.toTypedArray()) }
    }
}