package com.github.prologdb.dbms

import com.github.prologdb.runtime.ClauseIndicator
import com.github.prologdb.runtime.proofsearch.PrologCallable
import com.github.prologdb.runtime.proofsearch.ProofSearchContext
import com.github.prologdb.storage.fact.FactStore

interface DBProofSearchContext : ProofSearchContext {
    /**
     * Existing storage for dynamic facts. For get-or-create-and-get semantics
     * use [assureFactStore].
     */
    val factStores: Map<String, Map<ClauseIndicator, FactStore>>

    /**
     * Other code that is defined through code rather than data.
     */
    val callables: Map<String, Map<ClauseIndicator, PrologCallable>>

    /**
     * Builtins. Those cannot be modified and calls to these indicators
     * are never handled by the dynamic clauses in [factStores]
     * and [rules].
     */
    val staticBuiltins: Map<ClauseIndicator, PrologCallable>

    /**
     * Assures that a fact store for the given indicator exists (creates
     * one if necessary).
     *
     * @return the store for facts of the given indicator
     */
    fun assureFactStore(moduleName: String, indicator: ClauseIndicator): FactStore
}