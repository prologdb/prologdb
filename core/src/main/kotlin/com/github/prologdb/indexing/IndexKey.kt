package com.github.prologdb.indexing

import com.github.prologdb.runtime.unification.Unification

class IndexKey(val value: Unification) {
    init {
        require(value.entries.all { (_, value) -> value.isGround })
    }
}