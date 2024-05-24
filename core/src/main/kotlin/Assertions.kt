
package com.attardo.assertainty.core

import kotlin.test.Asserter
import kotlin.test.asserter

internal fun messagePrefix(message: String?) = if (message == null) "" else "$message. "

fun Asserter.assertLessThan(message: String?, threshold: Number, actual: Number) {
    assertTrue({ messagePrefix(message) + "Threshold [<$threshold], actual [$actual]." }, actual < threshold)
}

fun Asserter.assertLessThanOrEqualTo(message: String?, threshold: Number, actual: Number) {
    assertTrue({ messagePrefix(message) + "Threshold [<=$threshold], actual [$actual]." }, actual <= threshold)
}

fun Asserter.assertGreaterThan(message: String?, threshold: Number, actual: Number) {
    assertTrue({ messagePrefix(message) + "Threshold [>$threshold], actual [$actual]." }, actual > threshold)
}

fun Asserter.assertGreaterThanOrEqualTo(message: String?, threshold: Number, actual: Number) {
    assertTrue({ messagePrefix(message) + "Threshold [>=$threshold], actual [$actual]." }, actual >= threshold)
}

fun assertLessThan(threshold: Number, actual: Number, message: String? = null) {
    asserter.assertLessThan(message, threshold, actual)
}

fun assertLessThanOrEqualTo(threshold: Number, actual: Number, message: String? = null) {
    asserter.assertLessThanOrEqualTo(message, threshold, actual)
}

fun assertGreaterThan(threshold: Number, actual: Number, message: String? = null) {
    asserter.assertGreaterThan(message, threshold, actual)
}

fun assertGreaterThanOrEqualTo(threshold: Number, actual: Number, message: String? = null) {
    asserter.assertGreaterThanOrEqualTo(message, threshold, actual)
}

operator fun Number.compareTo(that: Number): Int {
    return when (this) {
        is Int -> when (that) {
            is Int -> this.compareTo(that)
            is Long -> this.compareTo(that)
            is Float -> this.compareTo(that)
            is Double -> this.compareTo(that)
            else -> throw RuntimeException()
        }

        is Long -> when (that) {
            is Int -> this.compareTo(that)
            is Long -> this.compareTo(that)
            is Float -> this.compareTo(that)
            is Double -> this.compareTo(that)
            else -> throw RuntimeException()
        }

        is Float -> when (that) {
            is Int -> this.compareTo(that)
            is Long -> this.compareTo(that)
            is Float -> this.compareTo(that)
            is Double -> this.compareTo(that)
            else -> throw RuntimeException()
        }

        is Double -> when (that) {
            is Int -> this.compareTo(that)
            is Long -> this.compareTo(that)
            is Float -> this.compareTo(that)
            is Double -> this.compareTo(that)
            else -> throw RuntimeException()
        }

        else -> throw RuntimeException()
    }
}

