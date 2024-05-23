import com.attardo.datavalidation.core.AssertBlockResults
import com.attardo.datavalidation.core.ComputeBlockResults
import com.attardo.datavalidation.core.TableScope
import org.ktorm.database.Database
import org.ktorm.dsl.QuerySource
import org.ktorm.dsl.from
import org.ktorm.schema.BaseTable
import org.ktorm.schema.ColumnDeclaring


fun QuerySource.assert(where: ColumnDeclaring<Boolean>? = null, init: KtormTableScope.() -> Unit): AssertBlockResults<ColumnDeclaring<*>> {
    val scope = KtormTableScope(this)
    scope.init()
    return assertAndCapture(
        table = scope.table,
        where = where,
        groupingColumns = scope.groupingColumns,
        dataAssertions = scope.dataAssertions
    )
}

fun BaseTable<*>.assert(db: Database, where: ColumnDeclaring<Boolean>? = null, init: KtormTableScope.() -> Unit): AssertBlockResults<ColumnDeclaring<*>> =
    db.from(this).assert(where, init)

fun QuerySource.compute(where: ColumnDeclaring<Boolean>? = null, init: KtormTableScope.() -> Unit): ComputeBlockResults<ColumnDeclaring<*>> {
    val scope = KtormTableScope(this)
    scope.init()
    return compute(
        table = scope.table,
        where = where,
        groupingColumns = scope.groupingColumns,
        dataAssertions = scope.dataAssertions
    )
}

fun BaseTable<*>.compute(db: Database, where: ColumnDeclaring<Boolean>? = null, init: KtormTableScope.() -> Unit): ComputeBlockResults<ColumnDeclaring<*>> =
    db.from(this).compute(where, init)

class KtormTableScope(table: QuerySource) : TableScope<QuerySource, ColumnDeclaring<*>, ColumnDeclaring<out Number>, ColumnDeclaring<out Boolean>>(table, KtormColumnMethods)