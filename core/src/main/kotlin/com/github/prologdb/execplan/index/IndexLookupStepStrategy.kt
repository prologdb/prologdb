package com.github.prologdb.execplan.index

import com.github.prologdb.indexing.IndexByTypeMap
import com.github.prologdb.indexing.PersistenceIDSet
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.knowledge.library.PredicateIndicator
import com.github.prologdb.runtime.term.Term

/**
 * [IndexLookupStep] uses the strategy pattern to support the different ways of looking things up from an index
 */
interface IndexLookupStepStrategy {
    /**
     * An explanation of this strategy in the official format; is used as the third argument to the lookup/3
     * step explanation (e.g. `range(gt(10))`)
     */
    val explanation: Term

    /**
     * Finds the correct index and queries it for matching data.
     * @param randomVariableScope to be used if temporary variables are needed
     * @param subjectPredicate An indicator describing the predicate to be searched
     * @param indexMap The available index for the predicate argument that is being queried
     * @param valueToBeUnifiedWith If not null, the values returned from this method will
     *                             later be unified with this term. This **can** be used to avoid
     *                             returning values that won't make it into the further query
     *                             process anyways.
     * @return The [PersistenceIDSet] which is affected, according to the consulted index.
     */
    fun execute(
        randomVariableScope: RandomVariableScope,
        subjectPredicate: PredicateIndicator,
        indexMap: IndexByTypeMap,
        valueToBeUnifiedWith: Term?
    ) : PersistenceIDSet
}