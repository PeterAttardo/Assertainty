import com.attardo.assertainty.core.AssertBlockResults
import com.attardo.assertainty.core.ColumnMethods
import com.attardo.assertainty.core.ComputeBlockResults
import com.attardo.assertainty.core.TableScope


fun String.assert(wrapper: SqlWrapper, dialect: SqlDialect = DefaultSqlDialect, init: RawSqlTableScope.() -> Unit): AssertBlockResults<String> {
    val scope = RawSqlTableScope(this, dialect)
    scope.init()
    return assertAndCapture(
        wrapper = wrapper,
        table = scope.table,
        groupingColumns = scope.groupingColumns,
        dataAssertions = scope.dataAssertions,
        dialect = dialect
    )
}

fun String.compute(wrapper: SqlWrapper, dialect: SqlDialect = DefaultSqlDialect, init: RawSqlTableScope.() -> Unit): ComputeBlockResults<String> {
    val scope = RawSqlTableScope(this, dialect)
    scope.init()
    return compute(
        wrapper = wrapper,
        table = scope.table,
        groupingColumns = scope.groupingColumns,
        dataAssertions = scope.dataAssertions,
        dialect = dialect
    )
}

operator fun SqlDialect.invoke(block: () -> Unit) {
    with(object {
        fun String.assert(wrapper: SqlWrapper, init: RawSqlTableScope.() -> Unit): AssertBlockResults<String> =
            assert(wrapper, this@invoke, init)
        fun String.compute(wrapper: SqlWrapper, dialect: ColumnMethods<String> = DefaultSqlDialect, init: RawSqlTableScope.() -> Unit): ComputeBlockResults<String> =
            compute(wrapper, this@invoke, init)
    }) {
        block()
    }
}

class RawSqlTableScope(table: String, dialect: ColumnMethods<String> = DefaultSqlDialect) : TableScope<String, String, String, String>(table, dialect)