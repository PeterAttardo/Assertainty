import com.attardo.datavalidation.core.AssertBlockResults
import com.attardo.datavalidation.core.ComputeBlockResults
import com.attardo.datavalidation.core.TableScope
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.Expression
import org.jetbrains.exposed.sql.Op
import org.jetbrains.exposed.sql.Table


fun <T: Table> T.assert(db: Database? = null, where: Op<Boolean>? = null, init: ExposedTableScope<T>.() -> Unit): AssertBlockResults<Expression<*>> {
    val scope = ExposedTableScope(this)
    scope.init()
    return assertAndCapture(
        db = db,
        where = where,
        table = scope.table,
        groupingColumns = scope.groupingColumns,
        dataAssertions = scope.dataAssertions
    )
}

fun <T: Table> T.compute(db: Database? = null, where: Op<Boolean>? = null, init: ExposedTableScope<T>.() -> Unit): ComputeBlockResults<Expression<*>> {
    val scope = ExposedTableScope(this)
    scope.init()
    return compute(
        db = db,
        where = where,
        table = scope.table,
        groupingColumns = scope.groupingColumns,
        dataAssertions = scope.dataAssertions
    )
}



class ExposedTableScope<T : Table>(table: T) : TableScope<T, Expression<*>, Expression<out Number>, Expression<out Boolean>>(table, ExposedColumnMethods)