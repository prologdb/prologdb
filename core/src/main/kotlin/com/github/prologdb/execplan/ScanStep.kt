package com.github.prologdb.execplan

import com.github.prologdb.PrologDatabaseView
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.knowledge.library.PredicateIndicator
import com.github.prologdb.runtime.lazysequence.LazySequence
import com.github.prologdb.runtime.lazysequence.filterRemainingNotNull
import com.github.prologdb.runtime.lazysequence.mapRemaining
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

/**
 * Most simple plan: scan all known predicates of a given [PredicateIndicator]
 */
class ScanStep(
    val goal: Predicate
) : PlanStep {

    private val goalIndicator = PredicateIndicator.of(goal)

    override fun execute(db: PrologDatabaseView, randomVarsScope: RandomVariableScope, variables: VariableBucket): LazySequence<Unification> {
        val predicateStore = db.predicateStores[goalIndicator] ?: return LazySequence.empty()

        return predicateStore.all()
                .mapRemaining { it.second.unify(goal, randomVarsScope) }
                .filterRemainingNotNull()
    }

    override val explanation by lazy { Predicate("prove", arrayOf(goal)) }
}