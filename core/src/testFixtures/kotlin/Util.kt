import com.attardo.assertainty.core.ComputeBlockResults
import com.attardo.assertainty.core.Computed
import com.attardo.assertainty.core.DataAssertion

fun <Column> expected(
    assertions: List<DataAssertion<Column>>,
    groupColumns: List<Column>,
    rows: List<List<Any>>
): ComputeBlockResults<Column> {
    return assertions.mapIndexed { index, dataAssertion ->
        val offset = assertions.foldIndexed(0){ index2, count, item ->
            when {
                index2 < index -> count + item.metricColumns.size
                else -> count
            }
        }
        dataAssertion to rows.map { row -> Computed(
            groupColumns.mapIndexed { index, column -> column to row[index] }.toMap(),
            dataAssertion.metricColumns.mapIndexed { index, columnDeclaring -> columnDeclaring to row[groupColumns.size + offset + index] }.toMap()
        ) }
    }.toMap()
}

fun <Column> expected(
    groupColumns: List<Column>,
    results: Map<DataAssertion<Column>, List<List<Any>>>
): ComputeBlockResults<Column> {
    return results.mapValues { (assertion, result) ->
        result.map { row ->
            Computed(
                groupColumns.mapIndexed { index, column -> column to row[index] }.toMap(),
                assertion.metricColumns.mapIndexed { index, column -> column to row[groupColumns.size + index] }.toMap())
        }
    }
}