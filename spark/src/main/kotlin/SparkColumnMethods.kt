package com.attardo.datavalidation.spark

import com.attardo.datavalidation.core.ColumnMethods
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.DataTypes

object SparkColumnMethods : ColumnMethods<Column> {

    override fun Column.minus(other: Column): Column = this - other

    override fun Column.div(other: Column): Column = this.divide(other)

    override fun Column.asDouble(): Column = this.cast(DataTypes.DoubleType)

    override fun avg(column: Column): Column = functions.avg(column)

    override fun sum(column: Column): Column = functions.sum(column)

    override fun count(): Column = functions.count("*")

    override fun countWhen(column: Column): Column = functions.sum(functions.`when`(column, 1).otherwise(0))

    override fun countDistinct(column: Column): Column = functions.count_distinct(column)

    override fun isNull(column: Column): Column = functions.isnull(column)
}