import com.attardo.assertainty.core.AssertBlockResults
import com.attardo.assertainty.core.ComputeBlockResults
import com.attardo.assertainty.core.Computed
import com.attardo.assertainty.core.DataAssertion

fun assertAndCapture(
    wrapper: SqlWrapper,
    table: String,
    groupingColumns: List<String>,
    dataAssertions: List<DataAssertion<String>>,
    dialect: SqlDialect = DefaultSqlDialect
) : AssertBlockResults<String> =
    compute(wrapper, table, groupingColumns, dataAssertions, dialect).mapValues { (key, value) -> value.map(key::assertAndCapture) }


fun compute(
    wrapper: SqlWrapper,
    table: String,
    groupingColumns: List<String>,
    dataAssertions: List<DataAssertion<String>>,
    dialect: SqlDialect = DefaultSqlDialect
) : ComputeBlockResults<String> {
    val columns = groupingColumns + dataAssertions.flatMap { it.metricColumns }
    return wrapper.executeSelect(dialect.select(columns, table, groupingColumns))
        .flatMap { row ->
            dataAssertions.map { assertion ->
                assertion to Computed(
                    groups = groupingColumns.associateWith { row[columns.indexOf(it)] },
                    metrics = assertion.metricColumns.associateWith { row[columns.indexOf(it)] }
                )
            }
        }.groupBy({ it.first }, { it.second })
}