package kotest

import assert
import com.attardo.assertainty.kotest.invoke
import io.kotest.core.spec.style.StringSpec
import org.ktorm.database.Database
import org.ktorm.dsl.eq
import org.ktorm.dsl.from
import org.ktorm.dsl.less
import org.ktorm.expression.FunctionExpression
import org.ktorm.schema.*


class KtormTest : StringSpec({
    val db = Database.connect(
        url = "jdbc:sqlite::resource:chinook.db",
        driver = "org.sqlite.JDBC"
    )

    "test1"(this) {
        tracks.assert(db) {
            +tracks.albumId
            +tracks.composer
            minSum(tracks.milliseconds, 30 * 60 * 1000, description = "Album length > 30 min")
            maxSum(tracks.milliseconds, 50 * 60 * 1000, description = "Album length < 50 min")
        }
    }

    "test2"(this) {
        tracks.assert(db) {
            always(tracks.unitPrice eq 0.99, description = "All priced at $0.99")
            neverNull(tracks.composer)
            always(tracks.name.length() less 15, description = "Name not excessively long")
        }
    }
}) {
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
}

fun Column<String>.length(): FunctionExpression<Int> {
    return FunctionExpression("length", listOf(this.asExpression()), IntSqlType)
}