import com.attardo.datavalidation.core.ColumnMethods
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.div
import org.jetbrains.exposed.sql.SqlExpressionBuilder.isNull
import org.jetbrains.exposed.sql.SqlExpressionBuilder.minus

object ExposedColumnMethods : ColumnMethods<Expression<*>> {

    override fun Expression<*>.minus(other: Expression<*>): Expression<*> = (this as ExpressionWithColumnType<Number>) minus (other as ExpressionWithColumnType<Number>)

    override fun Expression<*>.div(other: Expression<*>): Expression<*> = (this as ExpressionWithColumnType<Number>) div (other as ExpressionWithColumnType<Number>)

    override fun Expression<*>.asDouble(): Expression<*> = this.castTo(DoubleColumnType())

    override fun avg(column: Expression<*>): Expression<*> = (column as ExpressionWithColumnType<Comparable<Any>>).avg()

    override fun sum(column: Expression<*>): Expression<*> = (column as ExpressionWithColumnType<*>).sum()

    override fun count(): Expression<*> = stringLiteral("*").count()

    override fun countWhen(column: Expression<*>): Expression<*> = Case()
            .When(column as Expression<Boolean>, intLiteral(1))
            .Else(intLiteral(0))
            .sum()

    override fun countDistinct(column: Expression<*>): Expression<*> = (column as Column<*>).countDistinct()

    override fun isNull(column: Expression<*>): Expression<*> = column.isNull()
}