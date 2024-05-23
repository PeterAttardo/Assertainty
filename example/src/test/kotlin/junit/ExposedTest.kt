package junit

import assert
import columnSerializer
import com.attardo.datavalidation.junit.dataAssertionTestFactory
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.SqlExpressionBuilder.greater
import org.jetbrains.exposed.sql.SqlExpressionBuilder.less
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.charLength
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.TestInstance


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExposedTest {

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

    @TestFactory
    fun factory() = dataAssertionTestFactory {
        columnSerializer = db.columnSerializer()

        "test1" {
            tracks.assert(db) {
                +tracks.albumId
                +tracks.composer
                min_sum(tracks.milliseconds, 30 * 60 * 1000, description = "Album length > 30 min")
                max_sum(tracks.milliseconds, 50 * 60 * 1000, description = "Album length < 50 min")
            }
        }
        "test2" {
            tracks.assert(db) {
                always(tracks.unitPrice eq 0.99, description = "All priced at $0.99")
                never_null(tracks.composer)
                always(tracks.name.charLength() less 15, description = "Name not excessively long")
            }
        }
    }
}