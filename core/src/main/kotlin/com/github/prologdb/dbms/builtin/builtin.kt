package com.github.prologdb.dbms.builtin

import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.PrologException
import com.github.prologdb.runtime.builtin.NativeCodeRule
import com.github.prologdb.runtime.builtin.getInvocationStackFrame
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.unification.Unification

typealias PrologDBBuiltinImplementation = suspend LazySequenceBuilder<Unification>.(Array<out Term>, DBProofSearchContext) -> Unit

/**
 * A library that can be loaded into a [PersistentKnowledgeBase].
 */
interface DBLibrary {
    val name: String
    val exports: Map<ClauseIndicator, NativeCodeRule>
}

internal fun prologdbBuiltin(name: String, arity: Int, code: PrologDBBuiltinImplementation): NativeCodeRule {
    return NativeCodeRule(
        name,
        arity,
        getInvocationStackFrame()
    ) { args, ctxt ->
        if (ctxt !is DBProofSearchContext) {
            throw PrologException("Cannot invoke prologdb builtin with ${ctxt::class.java.simpleName} (requires ${DBProofSearchContext::class.java.simpleName})")
        }

        code(args, ctxt)
    }
}


internal fun builtinLibrary(name: String, builtins: List<NativeCodeRule>): DBLibrary {
    val exportsMap = builtins
        .map { Pair(ClauseIndicator.of(it.head), it) }
        .toMap()

    return object : DBLibrary {
        override val name = name
        override val exports = exportsMap
    }
}