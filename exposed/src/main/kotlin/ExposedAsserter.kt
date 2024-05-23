import com.attardo.datavalidation.core.AssertBlockResults
import com.attardo.datavalidation.core.ComputeBlockResults
import com.attardo.datavalidation.core.Computed
import com.attardo.datavalidation.core.DataAssertion
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.Expression
import org.jetbrains.exposed.sql.Op
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.transactions.transaction

fun assertAndCapture(
    table: Table,
    where: Op<Boolean>?,
    groupingColumns: List<Expression<*>>,
    dataAssertions: List<DataAssertion<Expression<*>>>,
    db: Database? = null
) : AssertBlockResults<Expression<*>> =
    compute(table, where, groupingColumns, dataAssertions, db).mapValues { (key, value) -> value.map(key::assertAndCapture) }


fun compute(
    table: Table,
    where: Op<Boolean>?,
    groupingColumns: List<Expression<*>>,
    dataAssertions: List<DataAssertion<Expression<*>>>,
    db: Database? = null
) : ComputeBlockResults<Expression<*>> = db.transaction {
    table.select(groupingColumns + dataAssertions.flatMap { it.metricColumns })
        .run { where?.let(::where) ?: this }
        .groupBy(*groupingColumns.toTypedArray())
        .flatMap { row ->
            dataAssertions.map { assertion ->
                assertion to Computed(
                    groups = groupingColumns.associateWith { row[it] },
                    metrics = assertion.metricColumns.associateWith { row[it] }
                )
            }
        }.groupBy({ it.first }, { it.second })
}

fun <T> Database?.transaction(block: ()-> T) : T = this?.let {
    transaction(it) {
        block()
    }
} ?: block()

/*
This function exists because calling `toString` on some subclasses
of `Expression` will raise an exception if done outside of a
`transaction` block. Once that bug is fixed, this method will be
deprecated.
 */
fun Database.columnSerializer(): (Expression<*>) -> String {
    return { transaction(this){ it.toString() } }
}