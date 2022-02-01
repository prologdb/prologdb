package com.github.prologdb.dbms.builtin

import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.dbms.DBProofSearchContext
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.builtin.getInvocationStackFrame
import com.github.prologdb.runtime.stdlib.NativeCodeRule
import com.github.prologdb.runtime.stdlib.TypedPredicateArguments
import com.github.prologdb.runtime.stdlib.nativeRule
import com.github.prologdb.runtime.unification.Unification

fun nativeDatabaseRule(name: String, arity: Int, code: suspend LazySequenceBuilder<Unification>.(TypedPredicateArguments, DBProofSearchContext) -> Unification?): NativeCodeRule {
    val definedAt = getInvocationStackFrame()
    return nativeRule(name, arity, definedAt) { args, ctxt ->
        if (ctxt !is DBProofSearchContext) {
            throw PrologRuntimeException("This predicate must be executed within a database. The given context must implement ${DBProofSearchContext::class.qualifiedName}.")
        }

        code(args, ctxt)
    }
}