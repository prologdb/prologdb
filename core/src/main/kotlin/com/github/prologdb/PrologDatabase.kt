package com.github.prologdb

import com.github.prologdb.execplan.planner.ExecutionPlanner
import com.github.prologdb.indexing.IndexByArgumentMap
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.knowledge.KnowledgeBase
import com.github.prologdb.runtime.knowledge.Rule
import com.github.prologdb.runtime.knowledge.library.OperatorRegistry
import com.github.prologdb.runtime.knowledge.library.PredicateIndicator
import com.github.prologdb.runtime.lazysequence.LazySequence
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket
import com.github.prologdb.storage.predicate.PredicateStore

/**
 * Groups everything that belongs to one database. One server process can host multiple databases (just like
 * SQL servers can).
 *
 * This is as close to an SQL schema as it gets.
 */
interface PrologDatabase {
    val predicateStores: Map<PredicateIndicator, PredicateStore>
    val rules: Map<PredicateIndicator, Set<Rule>>
    val indexes: Map<PredicateIndicator, IndexByArgumentMap>
    val planner: ExecutionPlanner

    fun execute(command: Query, randomVariableScope: RandomVariableScope = RandomVariableScope()): LazySequence<Unification> {
        val executionPlan = planner.planExecution(command, this, randomVariableScope)
        return executionPlan.execute(this, randomVariableScope, VariableBucket())
    }
}