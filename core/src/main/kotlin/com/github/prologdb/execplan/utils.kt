package com.github.prologdb.execplan

import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.CompoundTerm
import com.github.prologdb.runtime.term.PrologInteger

internal fun ClauseIndicator.toIdiomatic(): CompoundTerm = object : CompoundTerm("/", arrayOf(Atom(functor), PrologInteger.createUsingStringOptimizerCache(arity.toLong()))) {
    override fun toString() = "${this@toIdiomatic.functor}/${this@toIdiomatic.arity}"
}
