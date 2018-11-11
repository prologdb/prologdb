package com.github.prologdb.dbms

import com.github.prologdb.runtime.knowledge.ProofSearchContext
import com.github.prologdb.runtime.knowledge.Rule
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.storage.predicate.PredicateStore

interface DBProofSearchContext : ProofSearchContext {
    val predicateStores: Map<ClauseIndicator, PredicateStore>
    val rules: Map<ClauseIndicator, List<Rule>>
}