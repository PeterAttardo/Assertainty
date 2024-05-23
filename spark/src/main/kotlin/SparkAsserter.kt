package com.attardo.datavalidation.spark

import com.attardo.datavalidation.core.AssertBlockResults
import com.attardo.datavalidation.core.ComputeBlockResults
import com.attardo.datavalidation.core.Computed
import com.attardo.datavalidation.core.DataAssertion
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset

fun <T> assertAndCaptureSeparateQueries(
    table: Dataset<T>,
    groupingColumns: List<Column>,
    dataAssertions: List<DataAssertion<Column>>
) : AssertBlockResults<Column> =
    computeSeparateQueries(table, groupingColumns, dataAssertions).mapValues { (key, value) -> value.map(key::assertAndCapture) }

fun <T> assertAndCaptureSingleQuery(
    table: Dataset<T>,
    groupingColumns: List<Column>,
    dataAssertions: List<DataAssertion<Column>>
) : AssertBlockResults<Column> =
    computeSingleQuery(table, groupingColumns, dataAssertions).mapValues { (key, value) -> value.map(key::assertAndCapture) }


fun <T> computeSeparateQueries(
    table: Dataset<T>,
    groupingColumns: List<Column>,
    dataAssertions: List<DataAssertion<Column>>
) : ComputeBlockResults<Column> =
    table.groupBy(*groupingColumns.toTypedArray()).let {
        dataAssertions.associateWith { assertion ->
            val metricColumns = assertion.metricColumns
            it.agg(metricColumns.first(), *metricColumns.drop(1).toTypedArray())
                .toListofLists()
                .map { row ->
                    Computed(
                        groups = (groupingColumns zip row.take(groupingColumns.size)).toMap(),
                        metrics = (metricColumns zip row.takeLast(metricColumns.size)).toMap()
                    )
                }
        }
    }

fun <T> computeSingleQuery(
    table: Dataset<T>,
    groupingColumns: List<Column>,
    dataAssertions: List<DataAssertion<Column>>
) : ComputeBlockResults<Column> =
    table.groupBy(*groupingColumns.toTypedArray()).singleQuery {
        dataAssertions.forEach { assertion ->
            val metricColumns = assertion.metricColumns
            agg(metricColumns) { ds ->
                println(ds)
                assertion to ds.toListofLists().map { row ->
                    Computed(
                        groups = (groupingColumns zip row.take(groupingColumns.size)).toMap(),
                        metrics = (metricColumns zip row.takeLast(metricColumns.size)).toMap()
                    )
                }
            }
        }
    }.toMap()