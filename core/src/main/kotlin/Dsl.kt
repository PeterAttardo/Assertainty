package com.attardo.assertainty.core


@DslMarker
annotation class AssertionDslMarker

@AssertionDslMarker
abstract class TableScope<Table : Any, Column, NumericColumn: Column, BooleanColumn: Column>(val table: Table, val methods: ColumnMethods<Column>) {
    val groupingColumns: MutableList<Column> = mutableListOf()
    val dataAssertions: MutableList<DataAssertion<Column>> = mutableListOf()

    operator fun Column.unaryPlus() = groupBy(this)
    fun groupBy(vararg column: Column) = groupingColumns.addAll(column)
    fun groupBy(columns: List<Column>) = groupingColumns.addAll(columns)

    operator fun DataAssertion<Column>.unaryPlus() = assertion(this)
    fun assertion(vararg assertion: DataAssertion<Column>) = dataAssertions.addAll(assertion)
    fun assertion(assertions: List<DataAssertion<Column>>) = dataAssertions.addAll(assertions)

    fun min(column: NumericColumn, minValue: Number, description : String? = null) {
        +MinLiteralDataAssertion<Column, Number>(column, minValue, description)
    }

    fun max(column: NumericColumn, maxValue: Number, description : String? = null) {
        +MaxLiteralDataAssertion<Column, Number>(column, maxValue, description)
    }

    fun equal(column: NumericColumn, value: Number, description : String? = null) {
        +EqualsLiteralDataAssertion<Column, Number>(column, value, description)
    }

    fun equal(column1: Column, column2: Column, description : String? = null) {
        +EqualsDataAssertion<Column, Number>(column1, column2, description)
    }

    fun min_sum(column: NumericColumn, minValue: Number, description : String? = null) {
        +MinSumDataAssertion(column, minValue, methods, description)
    }

    fun max_sum(column: NumericColumn, maxValue: Number, description : String? = null) {
        +MaxSumDataAssertion(column, maxValue, methods, description)
    }

    fun min_avg(column: NumericColumn, minValue: Number, description : String? = null) {
        +MinAvgDataAssertion(column, minValue, methods, description)
    }

    fun max_avg(column: NumericColumn, maxValue: Number, description : String? = null) {
        +MaxAvgDataAssertion(column, maxValue, methods, description)
    }

    fun min_count(minValue: Long, description : String? = null) {
        +MinCountDataAssertion(minValue, methods, description)
    }

    fun max_count(maxValue: Long, description : String? = null) {
        +MaxCountDataAssertion(maxValue, methods, description)
    }

    fun min_when(condition: BooleanColumn, minCount: Long, description : String? = null) {
        +MinConditionCountDataAssertion(condition, minCount, methods, description)
    }

    fun max_when(condition: BooleanColumn, maxCount: Long, description : String? = null) {
        +MaxConditionCountDataAssertion(condition, maxCount, methods, description)
    }

    fun never(condition: BooleanColumn, description : String? = null) {
        +NeverDataAssertion(condition, methods, description)
    }

    fun always(condition: BooleanColumn, description : String? = null) {
        +AlwaysDataAssertion(condition, methods, description)
    }

    fun min_distinct(column: Column, minCount: Long, description : String? = null) {
        +MinDistinctCountDataAssertion(column, minCount, methods, description)
    }

    fun max_distinct(column: Column, maxCount: Long, description : String? = null) {
        +MaxDistinctCountDataAssertion(column, maxCount, methods, description)
    }

    fun min_ratio_when(condition: BooleanColumn, threshold: Double, description : String? = null) {
        +MinConditionRatioDataAssertion(condition, threshold, methods, description)
    }

    fun max_ratio_when(condition: BooleanColumn, threshold: Double, description : String? = null) {
        +MaxConditionRatioDataAssertion(condition, threshold, methods, description)
    }

    fun never_null(column: Column, description : String? = null) {
        +NeverNullDataAssertion(column, methods, description)
    }

    fun max_duplicate_ratio(column: Column, threshold: Double, description : String? = null) {
        +MaxDuplicateRatioDataAssertion(column, threshold, methods, description)
    }

    fun max_duplicates(column: Column, maxCount: Long, description : String? = null) {
        +MaxDuplicateCountDataAssertion(column, maxCount, methods, description)
    }

    fun unique(column: Column, description : String? = null) {
        +UniqueDataAssertion(column, methods, description)
    }

    fun max_null_ratio(column: Column, threshold: Double, description : String? = null) {
        +MaxIsNullRatioDataAssertion(column, threshold, methods, description)
    }

    fun assertion(vararg metricColumns: Column, description: String? = null, block: (Computed<Column>) -> Unit) {
        +object : DataAssertion<Column>(*metricColumns, description = description) {
            override fun assert(computed: Computed<Column>) = block(computed)
        }
    }

    fun <A> assertion1(metricColumn: Column, description: String? = null, block: (computedGroups: Map<Column, Any?>, computedMetric: A) -> Unit) {
        +DataAssertion1(metricColumn, description, block)
    }

    fun <A, B> assertion2(metricColumn1: Column, metricColumn2: Column, description: String? = null, block: (computedGroups: Map<Column, Any?>, computedMetric1: A, computedMetric2: B) -> Unit) {
        +DataAssertion2(metricColumn1, metricColumn2, description, block)
    }
}