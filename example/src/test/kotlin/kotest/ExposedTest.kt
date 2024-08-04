package kotest

import assert
import columnSerializer
import com.attardo.assertainty.kotest.invoke
import io.kotest.core.spec.style.StringSpec
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.Expression
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.SqlExpressionBuilder.less
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.charLength


class ExposedTest : StringSpec({
    val db = Database.connect(
        url = "jdbc:sqlite::resource:chinook.db",
        driver = "org.sqlite.JDBC"
    )


    "test1"(this, db.columnSerializer()) {
        tracks.assert(db) {
            +tracks.albumId
            +tracks.composer
            minSum(tracks.milliseconds, 30 * 60 * 1000, description = "Album length > 30 min")
            maxSum(tracks.milliseconds, 50 * 60 * 1000, description = "Album length < 50 min")
        }
    }

    "test2"(this, db.columnSerializer()) {
        tracks.assert(db) {
            always(tracks.unitPrice eq 0.99, description = "All priced at $0.99")
            neverNull(tracks.composer)
            always(tracks.name.charLength() less 15, description = "Name not excessively long")
        }
    }
}) {
    object tracks: Table("tracks") {
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
}