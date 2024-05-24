import com.attardo.assertainty.core.ColumnMethods
import org.ktorm.dsl.*
import org.ktorm.schema.ColumnDeclaring
import org.ktorm.schema.DoubleSqlType


object KtormColumnMethods : ColumnMethods<ColumnDeclaring<*>> {
    override fun ColumnDeclaring<*>.minus(other: ColumnDeclaring<*>): ColumnDeclaring<*> = (this as ColumnDeclaring<Number>) minus (other as ColumnDeclaring<Number>)

    override fun ColumnDeclaring<*>.div(other: ColumnDeclaring<*>): ColumnDeclaring<*> = (this as ColumnDeclaring<Number>) div (other as ColumnDeclaring<Number>)

    override fun ColumnDeclaring<*>.asDouble(): ColumnDeclaring<*> = this.cast(DoubleSqlType)

    override fun avg(column: ColumnDeclaring<*>): ColumnDeclaring<*> = org.ktorm.dsl.avg(column as ColumnDeclaring<Number>)

    override fun sum(column: ColumnDeclaring<*>): ColumnDeclaring<*> = org.ktorm.dsl.sum(column as ColumnDeclaring<Number>)

    override fun count(): ColumnDeclaring<*> = org.ktorm.dsl.count()

    override fun countWhen(column: ColumnDeclaring<*>): ColumnDeclaring<*> =
        sum(CASE().WHEN(column as ColumnDeclaring<Boolean>).THEN(1).ELSE(0).END())

    override fun countDistinct(column: ColumnDeclaring<*>): ColumnDeclaring<*> = org.ktorm.dsl.countDistinct(column)

    override fun isNull(column: ColumnDeclaring<*>): ColumnDeclaring<*> = column.isNull()
}