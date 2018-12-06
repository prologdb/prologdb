package com.github.prologdb.execplan

import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.term.PrologInteger

internal fun ClauseIndicator.toIdiomatic(): Predicate = object : Predicate("/", arrayOf(Atom(name), PrologInteger.createUsingStringOptimizerCache(arity.toLong()))) {
    override fun toString() = "$name/$arity"
}