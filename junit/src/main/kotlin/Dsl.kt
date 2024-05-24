package com.attardo.assertainty.junit

import com.attardo.assertainty.core.AssertBlockResults
import com.attardo.assertainty.core.DataAssertionResult
import org.junit.jupiter.api.DynamicContainer
import org.junit.jupiter.api.DynamicNode
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Named
import org.junit.jupiter.params.provider.Arguments

class TestFactoryScope<Column> {
    val tests = mutableListOf<Pair<String, AssertBlockResults<Column>>>()
    var columnSerializer: ((Column) -> String)? = null

    fun test(name: String, block: () -> AssertBlockResults<Column>) {
        tests.add( name to block())
    }

    operator fun String.invoke(block: () -> AssertBlockResults<Column>) {
        test(this, block)
    }
}

fun <Column> dataAssertionTestFactory(init: TestFactoryScope<Column>.() -> Unit) : List<DynamicNode> {
    val scope = TestFactoryScope<Column>()
    scope.init()
    return dataAssertionTestFactory(scope.tests, scope.columnSerializer)
}

fun <Column> dataAssertionTest(result: DataAssertionResult<Column>) {
    result.assertionError?.let { throw it }
}

fun <Column> dataAssertionTestFactory(tests: List<Pair<String, AssertBlockResults<Column>>>, columnSerializer: ((Column) -> String)? = null) : List<DynamicNode> {
    val serializer = columnSerializer ?: { it.toString() }
    return tests.map { (name, map) ->
        DynamicContainer.dynamicContainer(name, map.entries.map { (assertion, results) ->
            val name = assertion.description ?: "${assertion::class.simpleName} ${assertion.metricColumns.map(serializer)}"
            when (results.size) {
                1 -> {
                    DynamicTest.dynamicTest(name) {
                        dataAssertionTest(results[0])
                    }
                }

                else -> {
                    DynamicContainer.dynamicContainer(name, results.map {
                        DynamicTest.dynamicTest(it.computed.groups.mapKeys { (k, _) -> serializer(k) }.toString()) {
                            dataAssertionTest(it)
                        }
                    })
                }
            }
        })
    }
}

fun <Column> AssertBlockResults<Column>.toArguments(columnSerializer: ((Column) -> CharSequence)? = null) : List<Arguments> {
    val serializer = columnSerializer ?: { it.toString() }
    return flatMap { (assertion, results) ->
        results.map {
            val desc = assertion.description ?: "${assertion::class.simpleName} ${assertion.metricColumns.map(serializer)}"
            val name = when (it.computed.groups.isEmpty()) {
                true -> desc
                false -> {
                    "$desc: ${it.computed.groups.mapKeys { (k, _) -> serializer(k) }}"
                }
            }
            Arguments.of(Named.of(name, it))
        }
    }
}