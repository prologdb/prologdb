package com.github.prologdb.dbms.builtin

import com.github.prologdb.dbms.SystemCatalog
import com.github.prologdb.runtime.HasFunctorAndArity
import com.github.prologdb.runtime.PrologInvocationContractViolationException
import com.github.prologdb.runtime.proofsearch.PrologCallable
import com.github.prologdb.runtime.proofsearch.PrologCallableFulfill

class PhysicalDynamicPredicate(
    val catalog: SystemCatalog.Predicate
) : PrologCallable, HasFunctorAndArity by catalog {

    override val fulfill: PrologCallableFulfill = { _, _ ->
        throw PrologInvocationContractViolationException("This predicate resides on disk. It should only be accessed through execution plan functors.")
    }
}