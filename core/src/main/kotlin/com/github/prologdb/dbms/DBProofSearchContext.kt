package com.github.prologdb.dbms

import com.github.prologdb.runtime.builtin.NativeCodeRule
import com.github.prologdb.runtime.knowledge.ProofSearchContext
import com.github.prologdb.runtime.knowledge.Rule
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.storage.predicate.PredicateStore

interface DBProofSearchContext : ProofSearchContext {
    /**
     * Storages for dynamic facts
     */
    val predicateStores: Map<ClauseIndicator, PredicateStore>

    /**
     * Definitions of dynamic rules
     */
    val rules: Map<ClauseIndicator, List<Rule>>

    /**
     * Builtins. Those cannot be modified and calls to these indicators
     * are never handled by the dynamic clauses in [predicateStores]
     * and [rules].
     */
    val staticBuiltins: Map<ClauseIndicator, NativeCodeRule>
}