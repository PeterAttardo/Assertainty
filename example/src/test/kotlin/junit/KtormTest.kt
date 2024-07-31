package junit

import assert
import com.attardo.assertainty.junit.assertaintyTestFactory
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.TestInstance
import org.ktorm.database.Database
import org.ktorm.dsl.eq
import org.ktorm.dsl.from
import org.ktorm.dsl.less
import org.ktorm.expression.FunctionExpression
import org.ktorm.schema.*


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KtormTest {

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

    @TestFactory
    fun factory() = assertaintyTestFactory {
        "test1" {
            db.from(tracks).assert {
                +tracks.albumId
                +tracks.composer
                min_sum(tracks.milliseconds, 30 * 60 * 1000, description = "Album length > 30 min")
                max_sum(tracks.milliseconds, 50 * 60 * 1000, description = "Album length < 50 min")
            }
        }
        "test2" {
            db.from(tracks).assert {
                always(tracks.unitPrice eq 0.99, description = "All priced at $0.99")
                never_null(tracks.composer)
                always(tracks.name.length() less 15, description = "Name not excessively long")
            }
        }
    }
}

fun Column<String>.length(): FunctionExpression<Int> {
    return FunctionExpression("length", listOf(this.asExpression()), IntSqlType)
}