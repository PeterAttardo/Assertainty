import com.attardo.datavalidation.core.DataAssertion
import com.attardo.datavalidation.core.MaxConditionCountDataAssertion
import com.attardo.datavalidation.core.MinCountDataAssertion
import io.mockk.every
import io.mockk.mockkStatic
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.ktorm.database.Database
import org.ktorm.dsl.*
import org.ktorm.schema.*
import kotlin.reflect.KFunction
import kotlin.test.Test
import kotlin.test.assertContentEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KtormAsserterKtTest {
    lateinit var db: Database

    @BeforeAll
    fun setup() {
        db = Database.connect(
            url = "jdbc:sqlite::resource:chinook.db",
            driver = "org.sqlite.JDBC"
        )
    }

    object tracks: Table<Nothing>("tracks") {
        val trackId = int("TrackId")
        val name = varchar("Name")
        val albumId = int("AlbumId")
        val mediaTypeId = int("MediaTypeId")
        val genreId = int("GenreId")
        val composer = varchar("Composer")
        val milliseconds = int("Milliseconds")
        val bytes = int("Bytes")
        val unitPrice = double("UnitPrice")
    }

    @Test
    fun boop() {
        val col = sum(tracks.milliseconds).aliased("sumbum")
        db.from(tracks).select(tracks.trackId, col).groupBy(tracks.trackId).asQuerySource(tracks).select(tracks.trackId, col).where(col gt 30000).sql.also(::println)
    }

    @ParameterizedTest
    @MethodSource("computeArgs")
    fun compute(table: QuerySource, where: ColumnDeclaring<Boolean>?, groupColumns: List<ColumnDeclaring<*>>, results: Map<DataAssertion<ColumnDeclaring<*>>, List<List<Any>>>) {
        lateinit var query : Query
        val ds = spyk(table) QuerySource@{
            val sel: QuerySource.(Collection<ColumnDeclaring<*>>) -> Query = QuerySource::select
            mockkStatic(sel as KFunction<*>)
            every { this@QuerySource.select(any<List<ColumnDeclaring<*>>>())} answers {
                spyk(invocation.originalCall() as Query).also { query = it }
            }
        }

        val expected = expected(groupColumns, results)
        val actual = compute(ds, where, groupColumns, results.keys.toList() )
        assertContentEquals(expected.asIterable(), actual.asIterable())

        verify(exactly = 1) { query.groupBy(groupColumns) }
        verify(exactly = 1) { ds.select(groupColumns + results.keys.flatMap { it.metricColumns }) }
    }

    private fun computeArgs() : List<Arguments> {
        val table = db.from(tracks)
        return listOf(
            listOf(
                null,
                listOf(tracks.genreId),
                mapOf(
                    MinCountDataAssertion(100, KtormColumnMethods) to listOf(
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
                    MaxConditionCountDataAssertion(tracks.milliseconds gt 30000, 15, KtormColumnMethods) to listOf(
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
                null,
                listOf<ColumnDeclaring<*>>(),
                mapOf(
                    MinCountDataAssertion(2, KtormColumnMethods) to listOf(
                        listOf(3503)
                    ),
                    MaxConditionCountDataAssertion(tracks.milliseconds gt 30000, 15, KtormColumnMethods) to listOf(
                        listOf(3495)
                    )
                )
            )
        ).map { Arguments.of(table, *it.toTypedArray()) }
    }
}