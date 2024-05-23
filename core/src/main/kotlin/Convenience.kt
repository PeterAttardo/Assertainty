package com.attardo.datavalidation.core

//Type returned by calls to <Table>#assert(...){...}
typealias AssertBlockResults<Column> = Map<DataAssertion<Column>, List<DataAssertionResult<Column>>>

//Type returned by calls to <Table>#compute(...){...}
typealias ComputeBlockResults<Column> = Map<DataAssertion<Column>, List<Computed<Column>>>

fun <Column> AssertBlockResults<Column>.anyFailed() : Boolean = any { (_, results) -> results.any { it.isFailure } }

fun <Column> AssertBlockResults<Column>.filterFailed() : AssertBlockResults<Column> =
    mapValues { (_, results) ->
        results.filter(DataAssertionResult<Column>::isFailure)
    }.filter { (_, results) ->
        results.isNotEmpty()
    }

fun <Column> AssertBlockResults<Column>.forEachFailed(block: (DataAssertion<Column>, DataAssertionResult<Column>) -> Unit) =
    forEach { (assertion, results) -> results.forEach { if(it.isFailure) { block(assertion, it) } } }