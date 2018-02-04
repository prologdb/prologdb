package com.github.prologdb.indexing.index

import com.github.prologdb.runtime.term.Integer
import com.github.prologdb.runtime.term.Number
import io.kotlintest.specs.FreeSpec

class BTreePredicateArgumentIndexTest : FreeSpec() {init {
    "test" {
        val index = BTreePredicateArgumentIndex(Number::class, Comparator { o1, o2 -> o1!!.compareTo(o2!!) })
        index.onInserted(Integer(10), 0)
        index.onInserted(Integer(20), 0)
        index.onInserted(Integer(30), 0)
        index.onInserted(Integer(40), 0)
        index.onInserted(Integer(50), 0)
        index.onInserted(Integer(60), 0)
        index.onInserted(Integer(70), 0)
        index.onInserted(Integer(80), 0)
        index.onInserted(Integer(90), 0)
    }
}}