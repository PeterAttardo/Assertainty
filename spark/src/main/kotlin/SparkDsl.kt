package com.attardo.datavalidation.spark

import com.attardo.datavalidation.core.AssertBlockResults
import com.attardo.datavalidation.core.ComputeBlockResults
import com.attardo.datavalidation.core.TableScope
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset

fun <T> Dataset<T>.assertSeparateQueries(init: SparkTableScope<T>.() -> Unit): AssertBlockResults<Column> {
    val scope = SparkTableScope(this)
    scope.init()
    return assertAndCaptureSeparateQueries(
        table = scope.table,
        groupingColumns = scope.groupingColumns,
        dataAssertions = scope.dataAssertions
    )
}

fun <T> Dataset<T>.assert(init: SparkTableScope<T>.() -> Unit): AssertBlockResults<Column> {
    val scope = SparkTableScope(this)
    scope.init()
    return assertAndCaptureSingleQuery(
        table = scope.table,
        groupingColumns = scope.groupingColumns,
        dataAssertions = scope.dataAssertions
    )
}

fun <T> Dataset<T>.computeSeparateQueries(init: SparkTableScope<T>.() -> Unit): ComputeBlockResults<Column> {
    val scope = SparkTableScope(this)
    scope.init()
    return computeSeparateQueries(
        table = scope.table,
        groupingColumns = scope.groupingColumns,
        dataAssertions = scope.dataAssertions
    )
}



class SparkTableScope<T>(table: Dataset<T>) : TableScope<Dataset<T>, Column, Column, Column>(table, SparkColumnMethods) {
    operator fun String.unaryPlus() = groupingColumns.add(Column(this))
}