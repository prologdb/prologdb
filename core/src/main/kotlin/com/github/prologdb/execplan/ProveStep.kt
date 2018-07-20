package com.github.prologdb.execplan

import com.github.prologdb.PrologDatabase
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.lazysequence.LazySequence
import com.github.prologdb.runtime.lazysequence.filterRemainingNotNull
import com.github.prologdb.runtime.lazysequence.mapRemaining
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

/**
 * Most simple plan: scan all known predicates of a given [PredicateIndicator]
 */
class ProveStep(
    val unifyWith: Predicate
) : PlanStep {
    override fun execute(db: PrologDatabase, randomVarsScope: RandomVariableScope, variables: VariableBucket): LazySequence<Unification> {
        val predicateStore = db.predicateStores[unifyWith] ?: return LazySequence.empty()

        return predicateStore.all().toLazySequence(predicateStore)
                .mapRemaining { it.unify(unifyWith, randomVarsScope) }
                .filterRemainingNotNull()
    }

    override val explanation by lazy({ Predicate("prove", arrayOf(unifyWith)) })
}