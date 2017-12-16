package com.github.prologdb.indexing

import io.kotlintest.matchers.Matcher
import io.kotlintest.matchers.Result
import kotlin.math.abs
import kotlin.math.sqrt

fun roughlyEqualTo(targetValue: Double, delta: Double) = object : Matcher<Double> {
    override fun test(value: Double): Result {
        val min = targetValue - delta
        val max = targetValue + delta
        if (value >= min && value <= max) {
            return Result(true, "")
        } else {
            val offBy = if (value < min) abs(min - value) else abs(value - max)
            return Result(false, "Expected $targetValue +-$delta but got $value (off by $offBy)")
        }
    }
}

val Collection<Number>.variance: Double
    get() {
        val doubles = map { it.toDouble() }
        val avg = doubles.average()
        return doubles.map { (it - avg) * (it - avg) }.sum() / size.toDouble()
    }

val Collection<Number>.standardDeviation: Double
    get() = sqrt(variance)