import com.attardo.assertainty.core.DataAssertion
import com.attardo.assertainty.core.MaxConditionCountDataAssertion
import com.attardo.assertainty.core.MinCountDataAssertion
import io.mockk.every
import io.mockk.spyk
import io.mockk.verify
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.Expression
import org.jetbrains.exposed.sql.Query
import org.jetbrains.exposed.sql.SqlExpressionBuilder.greater
import org.jetbrains.exposed.sql.Table
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import kotlin.test.assertContentEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExposedAsserterKtTest {
    lateinit var db: Database

    @BeforeAll
    fun setup() {
        db = Database.connect(
            url = "jdbc:sqlite::resource:chinook.db",
            driver = "org.sqlite.JDBC"
        )
    }

    object tracks: Table() {
        val trackId = integer("TrackId")
        val name = varchar("Name", 200)
        val albumId = integer("AlbumId")
        val mediaTypeId = integer("MediaTypeId")
        val genreId = integer("GenreId")
        val composer = varchar("Composer", 220)
        val milliseconds = integer("Milliseconds")
        val bytes = integer("Bytes")
        val unitPrice = double("UnitPrice")
    }

    @ParameterizedTest
    @MethodSource("computeArgs")
    fun compute(table: Table, groupColumns: List<Expression<*>>, results: Map<DataAssertion<Expression<*>>, List<List<Any>>>) {
        lateinit var query : Query
        val ds = spyk(table) QuerySource@{
            every { this@QuerySource.select(any<List<Expression<*>>>())} answers {
                spyk(invocation.originalCall() as Query).also { query = it }
            }
        }

        val expected = expected(groupColumns, results)
        val actual = compute(ds, null, groupColumns, results.keys.toList(), db)

        assertContentEquals(expected.asIterable(), actual.asIterable())

        verify(exactly = 1) { query.groupBy(*groupColumns.toTypedArray()) }
        verify(exactly = 1) { ds.select(groupColumns + results.keys.flatMap { it.metricColumns }) }
    }

    private fun computeArgs() : List<Arguments> {
        val table = tracks
        return listOf(
            listOf(
                listOf(tracks.genreId),
                mapOf(
                    MinCountDataAssertion(100, ExposedColumnMethods) to listOf(
                        listOf(1.toInt(), 1297L).apply {  forEach { println(it::class) }},
                        listOf(2.toInt(), 130L),
                        listOf(3.toInt(), 374L),
                        listOf(4.toInt(), 332L),
                        listOf(5.toInt(), 12L),
                        listOf(6.toInt(), 81L),
                        listOf(7.toInt(), 579L),
                        listOf(8.toInt(), 58L),
                        listOf(9.toInt(), 48L),
                        listOf(10.toInt(), 43L),
                        listOf(11.toInt(), 15L),
                        listOf(12.toInt(), 24L),
                        listOf(13.toInt(), 28L),
                        listOf(14.toInt(), 61L),
                        listOf(15.toInt(), 30L),
                        listOf(16.toInt(), 28L),
                        listOf(17.toInt(), 35L),
                        listOf(18.toInt(), 13L),
                        listOf(19.toInt(), 93L),
                        listOf(20.toInt(), 26L),
                        listOf(21.toInt(), 64L),
                        listOf(22.toInt(), 17L),
                        listOf(23.toInt(), 40L),
                        listOf(24.toInt(), 74L),
                        listOf(25.toInt(), 1L),
                    ),
                    MaxConditionCountDataAssertion(tracks.milliseconds greater 30000, 15, ExposedColumnMethods) to listOf(
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
                listOf<Expression<*>>(),
                mapOf(
                    MinCountDataAssertion(2, ExposedColumnMethods) to listOf(
                        listOf(3503L)
                    ),
                    MaxConditionCountDataAssertion(tracks.milliseconds greater 30000, 15, ExposedColumnMethods) to listOf(
                        listOf(3495)
                    )
                )
            )
        ).map { Arguments.of(table, *it.toTypedArray()) }
    }
}