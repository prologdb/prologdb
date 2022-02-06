package com.github.prologdb.dbms.builtin

import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.dbms.DatabaseProofSearchContext
import com.github.prologdb.dbms.PhysicalDatabaseProofSearchContext
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.builtin.getInvocationStackFrame
import com.github.prologdb.runtime.stdlib.NativeCodeRule
import com.github.prologdb.runtime.stdlib.TypedPredicateArguments
import com.github.prologdb.runtime.stdlib.nativeRule
import com.github.prologdb.runtime.unification.Unification

fun nativeDatabaseRule(name: String, arity: Int, code: suspend LazySequenceBuilder<Unification>.(TypedPredicateArguments, DatabaseProofSearchContext) -> Unification?): NativeCodeRule {
    val definedAt = getInvocationStackFrame()
    return nativeRule(name, arity, definedAt) { args, ctxt ->
        if (ctxt !is DatabaseProofSearchContext) {
            throw PrologRuntimeException("This predicate must be executed within a database. The given context must implement ${DatabaseProofSearchContext::class.qualifiedName}.")
        }

        code(args, ctxt)
    }
}