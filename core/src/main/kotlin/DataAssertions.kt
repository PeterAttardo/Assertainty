package com.attardo.assertainty.core

import kotlin.test.assertEquals

interface ColumnMethods<Column> {
    operator fun Column.minus(other: Column): Column
    operator fun Column.div(other: Column): Column
    fun Column.asDouble(): Column
    fun avg(column: Column): Column
    fun sum(column: Column): Column
    fun count(): Column
    fun countWhen(column: Column): Column
    fun countDistinct(column: Column): Column
    fun isNull(column: Column): Column
}

open class MinLiteralDataAssertion<Column, N : Number>(column: Column, minValue: N, description: String? = null) :
    DataAssertion1<Column, N>(column, description, { _, metric ->
        assertGreaterThanOrEqualTo(minValue, metric)
    })

open class MaxLiteralDataAssertion<Column, N : Number>(column: Column, maxValue: N, description: String? = null) :
    DataAssertion1<Column, N>(column, description, { _, metric ->
        assertLessThanOrEqualTo(maxValue, metric)
    })

open class EqualsLiteralDataAssertion<Column, N : Number>(column: Column, value: N, description: String? = null) :
    DataAssertion1<Column, N>(column, description, { _, metric ->
        assertEquals(value, metric)
    })

open class EqualsDataAssertion<Column, N : Number>(column1: Column, column2: Column, description: String? = null) :
    DataAssertion2<Column, N, N>(column1, column2, description, { _, metric1, metric2 ->
        assertEquals(metric1, metric2)
    })

open class MinSumDataAssertion<Column, N : Number>(
    column: Column,
    minValue: N,
    methods: ColumnMethods<Column>,
    description: String? = null
) : MinLiteralDataAssertion<Column, N>(methods.sum(column), minValue, description)

open class MaxSumDataAssertion<Column, N : Number>(
    column: Column,
    maxValue: N,
    methods: ColumnMethods<Column>,
    description: String? = null
) : MaxLiteralDataAssertion<Column, N>(methods.sum(column), maxValue, description)

open class MinAvgDataAssertion<Column, N : Number>(
    column: Column,
    minValue: N,
    methods: ColumnMethods<Column>,
    description: String? = null
) : MinLiteralDataAssertion<Column, N>(methods.avg(column), minValue, description)

open class MaxAvgDataAssertion<Column, N : Number>(
    column: Column,
    maxValue: N,
    methods: ColumnMethods<Column>,
    description: String? = null
) : MaxLiteralDataAssertion<Column, N>(methods.avg(column), maxValue, description)

open class MinCountDataAssertion<Column, N : Number>(
    minValue: N,
    methods: ColumnMethods<Column>,
    description: String? = null
) : MinLiteralDataAssertion<Column, N>(methods.count(), minValue, description)

open class MaxCountDataAssertion<Column, N : Number>(
    maxValue: N,
    methods: ColumnMethods<Column>,
    description: String? = null
) : MaxLiteralDataAssertion<Column, N>(methods.count(), maxValue, description)

open class MinConditionCountDataAssertion<Column, N : Number>(
    column: Column,
    minValue: N,
    methods: ColumnMethods<Column>,
    description: String? = null
) : MinLiteralDataAssertion<Column, N>(methods.countWhen(column), minValue, description)

open class MaxConditionCountDataAssertion<Column, N : Number>(
    condition: Column,
    maxValue: N,
    methods: ColumnMethods<Column>,
    description: String? = null
) : MaxLiteralDataAssertion<Column, N>(methods.countWhen(condition), maxValue, description)

open class NeverDataAssertion<Column>(
    condition: Column,
    methods: ColumnMethods<Column>,
    description: String? = null
) : EqualsLiteralDataAssertion<Column, Long>(methods.countWhen(condition), 0, description)

open class AlwaysDataAssertion<Column>(
    condition: Column,
    methods: ColumnMethods<Column>,
    description: String? = null
) : EqualsDataAssertion<Column, Long>(methods.count(), methods.countWhen(condition), description)

open class MinDistinctCountDataAssertion<Column, N : Number>(
    column: Column,
    minValue: N,
    methods: ColumnMethods<Column>,
    description: String? = null
) : MinLiteralDataAssertion<Column, N>(with(methods) { countDistinct(column) }, minValue, description)

open class MaxDistinctCountDataAssertion<Column, N : Number>(
    column: Column,
    maxValue: N,
    methods: ColumnMethods<Column>,
    description: String? = null
) : MaxLiteralDataAssertion<Column, N>(with(methods) { countDistinct(column) }, maxValue, description)

open class MinConditionRatioDataAssertion<Column>(
    condition: Column,
    threshold: Double,
    methods: ColumnMethods<Column>,
    description: String? = null
) : MinLiteralDataAssertion<Column, Double>(with(methods) { countWhen(condition).asDouble() / count() }, threshold, description)

open class MaxConditionRatioDataAssertion<Column>(
    condition: Column,
    threshold: Double,
    methods: ColumnMethods<Column>,
    description: String? = null
) : MaxLiteralDataAssertion<Column, Double>(with(methods) { countWhen(condition).asDouble() / count() }, threshold, description)

open class NeverNullDataAssertion<Column>(
    column: Column,
    methods: ColumnMethods<Column>,
    description: String? = null
) : NeverDataAssertion<Column>(methods.isNull(column), methods, description)

open class MaxDuplicateCountDataAssertion<Column>(
    column: Column,
    maxCount: Long,
    methods: ColumnMethods<Column>,
    description: String? = null
) : MaxLiteralDataAssertion<Column, Long>(with(methods){count() - countDistinct(column)}, maxCount, description)

open class MaxDuplicateRatioDataAssertion<Column>(
    column: Column,
    threshold: Double,
    methods: ColumnMethods<Column>,
    description: String? = null
) : MaxLiteralDataAssertion<Column, Double>(with(methods) { (count() - countDistinct(column)).asDouble() / count() }, threshold, description)


open class UniqueDataAssertion<Column>(
    column: Column,
    methods: ColumnMethods<Column>,
    description: String? = null
) : MaxDuplicateCountDataAssertion<Column>(column, 0, methods, description)

open class MaxIsNullRatioDataAssertion<Column>(
    column: Column,
    threshold: Double,
    methods: ColumnMethods<Column>,
    description: String? = null
) : MaxConditionRatioDataAssertion<Column>(methods.isNull(column), threshold, methods, description)