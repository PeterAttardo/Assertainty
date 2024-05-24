package com.attardo.assertainty.core

abstract class DataAssertion<Column>(val metricColumns: List<Column>, val description : String? = null) {
    constructor(vararg metricColumns: Column, description: String? = null) : this(metricColumns.toList(), description)
    abstract fun assert(computed: Computed<Column>)
    fun assertAndCapture(computed: Computed<Column>) : DataAssertionResult<Column> {
        return DataAssertionResult(
            computed = computed,
            assertionError = try {
                assert(computed)
                null
            } catch (e: AssertionError) {
                e
            }
        )
    }
}

open class DataAssertion1<Column, A>(
    val metricColumn: Column,
    description: String? = null,
    val assert: (
        computedGroups: Map<Column, Any?>,
        computedMetric: A
    ) -> Unit
) : DataAssertion<Column>(metricColumn, description = description) {
    override fun assert(computed: Computed<Column>) = assert(
        computed.groups,
        computed.metrics[metricColumn] as A
    )
}

open class DataAssertion2<Column, A, B>(
    val metricColumn1: Column,
    val metricColumn2: Column,
    description: String? = null,
    val assert: (
        computedGroups: Map<Column, Any?>,
        computedMetric1: A,
        computedMetric2: B
    ) -> Unit
) : DataAssertion<Column>(metricColumn1, metricColumn2, description = description) {
    override fun assert(computed: Computed<Column>) = assert(
        computed.groups,
        computed.metrics[metricColumn1] as A,
        computed.metrics[metricColumn2] as B
    )
}