package kotest

import JDBCSqlWrapper
import assert
import com.attardo.assertainty.kotest.invoke
import io.kotest.core.spec.style.StringSpec


class RawSqlTest : StringSpec({
    val wrapper = JDBCSqlWrapper("jdbc:sqlite::resource:chinook.db")

    "test1"(this) {
        "tracks".assert(wrapper) {
            +"AlbumId"
            +"Composer"
            minSum("Milliseconds", 30 * 60 * 1000, description = "Album length > 30 min")
            maxSum("Milliseconds", 50 * 60 * 1000, description = "Album length < 50 min")
        }
    }

    "test2"(this) {
        "tracks".assert(wrapper) {
            always("UnitPrice=0.99", description = "All priced at $0.99")
            neverNull("Composer")
            always("length(Name) < 15", description = "Name not excessively long")
        }
    }
})