import com.attardo.assertainty.core.ColumnMethods

interface SqlDialect : ColumnMethods<String> {
    fun select(columns: List<String>, from: String, groupBy: List<String>): String
}

object DefaultSqlDialect : SqlDialect {
    override fun select(columns: List<String>, from: String, groupBy: List<String>): String {
        val groupByString = when(groupBy.isNotEmpty()) {
            true -> " GROUP BY ${groupBy.joinToString()}"
            false -> ""
        }
        return "SELECT ${columns.joinToString()} FROM ($from)$groupByString"
    }

    override fun String.minus(other: String): String = "($this - $other)"

    override fun String.div(other: String): String = "($this / $other)"

    override fun String.asDouble(): String = "CAST($this AS DOUBLE)"

    override fun avg(column: String): String = "AVG($column)"

    override fun sum(column: String): String = "SUM($column)"

    override fun count(): String = "COUNT(*)"

    override fun countWhen(column: String): String = "SUM(CASE WHEN ($column) THEN 1 ELSE 0 END)"

    override fun countDistinct(column: String): String = "COUNT(DISTINCT($column))"

    override fun isNull(column: String): String = "($column IS NULL)"
}