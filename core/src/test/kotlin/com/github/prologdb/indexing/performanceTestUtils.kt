package com.github.prologdb.indexing

import kotlin.math.sqrt

val Collection<Number>.variance: Double
    get() {
        val doubles = map { it.toDouble() }
        val avg = doubles.average()
        return doubles.sumOf { (it - avg) * (it - avg) } / size.toDouble()
    }

val Collection<Number>.standardDeviation: Double
    get() = sqrt(variance)