package com.github.prologdb.dbms

import com.github.prologdb.runtime.builtin.NativeCodeRule
import com.github.prologdb.runtime.knowledge.ProofSearchContext
import com.github.prologdb.runtime.knowledge.Rule
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.storage.fact.FactStore

interface DBProofSearchContext : ProofSearchContext {
    /**
     * Existing storages for dynamic facts. For get-or-create-and-get semantics
     * use [assureFactStore]
     */
    val factStores: Map<ClauseIndicator, FactStore>

    /**
     * Definitions of dynamic rules.
     */
    val rules: Map<ClauseIndicator, List<Rule>>

    /**
     * Builtins. Those cannot be modified and calls to these indicators
     * are never handled by the dynamic clauses in [factStores]
     * and [rules].
     */
    val staticBuiltins: Map<ClauseIndicator, NativeCodeRule>

    /**
     * Assures that a fact store for the given indicator exists (creates
     * one if necessary).
     *
     * @return the store for facts of the given indicator
     */
    fun assureFactStore(indicator: ClauseIndicator): FactStore
}