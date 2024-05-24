package com.attardo.assertainty.spark

import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.RelationalGroupedDataset
import org.apache.spark.sql.Row

class DelayedAggregation<T>(val exprs: List<Column>, val onAggregation: (dataset: Dataset<Row>) -> T){
    constructor(expr: Column, vararg exprs: Column, onAggregation: (dataset: Dataset<Row>) -> T) : this(listOf(expr, *exprs), onAggregation)
}


fun <T> RelationalGroupedDataset.agg(aggregations: List<DelayedAggregation<T>>) : List<T> {
    val allColumnsInitial = aggregations.flatMap { it.exprs }
    val allColumns = allColumnsInitial.mapIndexed { index, column -> column.`as`("$index") }
    val df = agg(allColumns.first(), *allColumns.drop(1).toTypedArray())
    val groupingColumns = this.groupingExprs().toKotlinList().map(::Column)
    return aggregations.map { aggregation ->
        val columns = groupingColumns + aggregation.exprs.map { Column(allColumnsInitial.indexOf(it).toString()) }
        aggregation.onAggregation(df.select(*columns.toTypedArray()).let {
//            df.select(*aggregation.exprs.toTypedArray())
            aggregation.exprs.fold(it) { df, expr ->
                df.withColumnRenamed(allColumnsInitial.indexOf(expr).toString(), expr.toString())
//                df.withColumn(allColumnsInitial.indexOf(expr).toString(), expr)
            }
        })
    }
}

class DelayedAggregationScope<T> {
    val aggregations: MutableList<DelayedAggregation<T>> = mutableListOf()

    fun agg(exprs: List<Column>, onAggregation: (dataset: Dataset<Row>) -> T) {
        aggregations.add(DelayedAggregation(exprs, onAggregation))
    }

    fun agg(expr: Column, vararg exprs: Column, onAggregation: (dataset: Dataset<Row>) -> T) {
        aggregations.add(DelayedAggregation(expr, *exprs, onAggregation = onAggregation))
    }
}


fun <T> RelationalGroupedDataset.singleQuery(block: DelayedAggregationScope<T>.() -> Unit) : List<T> {
    val scope = DelayedAggregationScope<T>()
    scope.block()
    return this.agg(scope.aggregations)
}