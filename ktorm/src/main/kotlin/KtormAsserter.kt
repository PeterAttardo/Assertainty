import com.attardo.assertainty.core.AssertBlockResults
import com.attardo.assertainty.core.ComputeBlockResults
import com.attardo.assertainty.core.Computed
import com.attardo.assertainty.core.DataAssertion
import org.ktorm.database.Database
import org.ktorm.dsl.*
import org.ktorm.schema.BaseTable
import org.ktorm.schema.ColumnDeclaring

fun assertAndCapture(
    table: QuerySource,
    where: ColumnDeclaring<Boolean>?,
    groupingColumns: List<ColumnDeclaring<*>>,
    dataAssertions: List<DataAssertion<ColumnDeclaring<*>>>
) : AssertBlockResults<ColumnDeclaring<*>> =
    compute(table, where, groupingColumns, dataAssertions).mapValues { (key, value) -> value.map(key::assertAndCapture) }


fun compute(
    table: QuerySource,
    where: ColumnDeclaring<Boolean>?,
    groupingColumns: List<ColumnDeclaring<*>>,
    dataAssertions: List<DataAssertion<ColumnDeclaring<*>>>,
) : ComputeBlockResults<ColumnDeclaring<*>> {
    val columns = groupingColumns + dataAssertions.flatMap { it.metricColumns }
    return table.select(columns)
        .run { where?.let(::where) ?: this }
        .groupBy(groupingColumns)
        .flatMap { row ->
            dataAssertions.map { assertion ->
                assertion to Computed(
                    groups = groupingColumns.associateWith { it.sqlType.getResult(row, columns.indexOf(it) + 1) },
                    metrics = assertion.metricColumns.associateWith { it.sqlType.getResult(row, columns.indexOf(it) + 1) }
                )
            }
        }.groupBy({ it.first }, { it.second })
}

fun Query.asQuerySource(sourceTable: BaseTable<*>) : QuerySource {
    return QuerySource(database, sourceTable, expression)
}