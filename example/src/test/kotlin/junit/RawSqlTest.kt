package junit

import JDBCSqlWrapper
import SqlWrapper
import assert
import com.attardo.assertainty.junit.assertaintyTestFactory
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.TestInstance


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RawSqlTest {

    lateinit var wrapper: SqlWrapper

    @BeforeAll
    fun setup() {
        wrapper = JDBCSqlWrapper("jdbc:sqlite::resource:chinook.db")
    }

    @TestFactory
    fun factory() = assertaintyTestFactory {
        "test1" {
            "tracks".assert(wrapper) {
                +"AlbumId"
                +"Composer"
                minSum("Milliseconds", 30 * 60 * 1000, description = "Album length > 30 min")
                maxSum("Milliseconds", 50 * 60 * 1000, description = "Album length < 50 min")
            }
        }
        "test2" {
            "tracks".assert(wrapper) {
                always("UnitPrice=0.99", description = "All priced at $0.99")
                neverNull("Composer")
                always("length(Name) < 15", description = "Name not excessively long")
            }
        }
    }
}