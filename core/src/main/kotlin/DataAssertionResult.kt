package com.attardo.datavalidation.core

data class Computed<Column>(
    val groups: Map<Column, Any?>,
    val metrics: Map<Column, Any?>
)

data class DataAssertionResult<Column>(
    val computed: Computed<Column>,
    val assertionError: AssertionError? = null
) {
    val isFailure: Boolean
        get() = assertionError != null
    val isSuccess: Boolean
        get() = !isFailure
}
