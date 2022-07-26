package com.github.prologdb.util

import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.module.Module
import com.github.prologdb.runtime.proofsearch.PrologCallable

class OverrideModule(copyFrom: Module, overrides: Map<ClauseIndicator, PrologCallable>) : Module by copyFrom {
    override val allDeclaredPredicates = HashMap(copyFrom.allDeclaredPredicates).also { allDeclared ->
        overrides.forEach { (overrideIndicator, overrideImpl) ->
            allDeclared[overrideIndicator] = overrideImpl
        }
    }
    override val exportedPredicates = copyFrom.exportedPredicates.keys.associateWith(allDeclaredPredicates::getValue)
}