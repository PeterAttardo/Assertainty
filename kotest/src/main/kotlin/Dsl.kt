package com.attardo.assertainty.kotest

import com.attardo.assertainty.core.AssertBlockResults
import io.kotest.core.names.TestName
import io.kotest.core.spec.style.scopes.ContainerScope
import io.kotest.core.spec.style.scopes.FunSpecContainerScope
import io.kotest.core.spec.style.scopes.RootScope
import io.kotest.core.spec.style.scopes.addContainer

operator fun <Column> String.invoke(scope: RootScope, columnSerializer: ((Column) -> String)? = null, block: () -> AssertBlockResults<Column>) =
    scope.addDataContainer(this, columnSerializer, block)

fun <Column> RootScope.addDataContainer(name: String, columnSerializer: ((Column) -> String)? = null, block: () -> AssertBlockResults<Column>) =
    addContainer(name) {
        addDataTests(columnSerializer, block)
    }

fun RootScope.addContainer(name: String, block: suspend ContainerScope.() -> Unit) =
    addContainer(TestName(name), false, null, block)

suspend fun <Column> ContainerScope.addDataTests(columnSerializer: ((Column) -> String)? = null, block: () -> AssertBlockResults<Column>) {
    val serializer = columnSerializer ?: {it.toString()}
    block().forEach { (assertion, results) ->
        val name = assertion.description ?: "${assertion::class.simpleName} ${assertion.metricColumns.map(serializer)}"
        when (results.size) {
            1 -> {
                registerTest(TestName(name), false, null) {
                    results[0].assertionError?.let { throw it }
                }
            }
            else -> {
                registerContainer(TestName(name), false, null) {
                    FunSpecContainerScope(this).apply {
                        results.map {
                            registerTest(TestName(it.computed.groups.mapKeys {(k, v) -> serializer(k) }.toString()), false, null) {
                                it.assertionError?.let { throw it }
                            }
                        }
                    }
                }
            }
        }
    }
}