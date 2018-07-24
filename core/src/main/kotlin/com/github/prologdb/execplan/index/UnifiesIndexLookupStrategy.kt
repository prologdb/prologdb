package com.github.prologdb.execplan.index

import com.github.prologdb.execplan.PlanStepInappropriateException
import com.github.prologdb.indexing.IndexByTypeMap
import com.github.prologdb.indexing.PersistenceIDSet
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.term.Term

/**
 * Implements the `unifies(+Term)` index lookup strategy.
 */
class UnifiesIndexLookupStrategy(
    val unifiesWith: Term
) : IndexLookupStrategy {
    override val explanation: Term = Predicate(
        name = "unifies",
        arguments = arrayOf(unifiesWith)
    )

    override fun execute(randomVariableScope: RandomVariableScope, indexMap: IndexByTypeMap, valueToBeUnifiedWith: Term?): PersistenceIDSet {
        val indexForType = indexMap[unifiesWith::class] ?: indexMap[Term::class]
            ?: throw PlanStepInappropriateException("No index suitable for term type ${unifiesWith.prologTypeName} in argument #${indexMap.argumentIndex} of ${indexMap.indicator}")

        return indexForType.find(unifiesWith)
    }
}