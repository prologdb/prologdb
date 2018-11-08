package com.github.prologdb.execplan.index

import com.github.prologdb.async.LazySequenceBuilder
import com.github.prologdb.execplan.PlanStep
import com.github.prologdb.execplan.PlanStepInappropriateException
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.term.Variable
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

class IndexLookupStep private constructor(
    /** The predicate as originally given in the query */
    protected val predicateAsInQuery: Predicate,
    /** The index of the argument in the query whichs index is to be used */
    protected val targetArgumentIndex: Int,

    protected val strategy: IndexLookupStrategy,

    /** Used to create random variables and avoid collisions */
    randomVariableScope: RandomVariableScope
) : PlanStep {
    init {
        if (targetArgumentIndex < 0 || targetArgumentIndex >= predicateAsInQuery.arguments.size) {
            throw IndexOutOfBoundsException("Argument index is out of bounds: arity = ${predicateAsInQuery.arity}, index = $targetArgumentIndex")
        }
    }

    private val targetClauseIndicator = ClauseIndicator.of(predicateAsInQuery)

    protected val replacementVariable = randomVariableScope.createNewRandomVariable()

    /**
     * The predicate to look up with the subject argument replaced
     */
    protected val predicateToLookUp: Predicate = Predicate(
        predicateAsInQuery.name,
        predicateAsInQuery.arguments
            .mapIndexed { argIndex, arg ->
                if (argIndex == targetArgumentIndex) {
                    replacementVariable
                } else {
                    arg
                }
            }
            .toTypedArray()
    )

    override val explanation: Predicate = Predicate(
        "lookup",
        arrayOf(
            predicateToLookUp,
            Predicate("=", arrayOf(replacementVariable, predicateAsInQuery.arguments[targetArgumentIndex])),
            strategy.explanation
        )
    )

    override val execute: suspend LazySequenceBuilder<Unification>.(PrologDatabaseView, RandomVariableScope, VariableBucket) -> Unit = { db, randomVarsScope, variables ->
        val indicesByIndicator = db.indexes[targetClauseIndicator]
            ?: throw PlanStepInappropriateException("Internal error: index lookup step configured for non-existent index")
        val indicesForArgument = indicesByIndicator[targetArgumentIndex]
        if (indicesForArgument.isEmpty())
            throw PlanStepInappropriateException("No indices defined for argument #$targetArgumentIndex of $targetClauseIndicator")

        val originalValueForArgument = predicateAsInQuery.arguments[targetArgumentIndex]

        val predicateStore = db.predicateStores[targetClauseIndicator]
            ?: throw PlanStepInappropriateException("Internal error: index lookup step configured for non-existent predicate-store")

        await(strategy.execute(
            randomVariableScope = randomVarsScope,
            indexMap = indicesForArgument,
            valueToBeUnifiedWith = if (originalValueForArgument is Variable) null else originalValueForArgument,
            principal = principal
        ))
            .toLazySequence(predicateStore, principal)
            .mapRemaining { storedPredicate -> storedPredicate.unify(predicateAsInQuery, randomVarsScope) }
            .filterRemainingNotNull()
    }
}