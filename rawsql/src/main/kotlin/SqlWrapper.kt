import java.sql.DriverManager
import java.sql.ResultSet

fun interface SqlWrapper {
    fun executeSelect(selectStatement: String) : List<List<Any>>
}

class JDBCSqlWrapper(val connectionUrl: String) : SqlWrapper {
    val connection = DriverManager.getConnection(connectionUrl)
    override fun executeSelect(selectStatement: String): List<List<Any>> =
        connection.createStatement().apply { execute(selectStatement) }.resultSet.map { set ->
            (1.. set.metaData.columnCount).map { set.getObject(it) }
        }.toList()
}

fun <T> ResultSet.map(block: (ResultSet) -> T) : Sequence<T> = generateSequence {
    when(this.next()) {
        true -> block(this)
        false -> null
    }
}