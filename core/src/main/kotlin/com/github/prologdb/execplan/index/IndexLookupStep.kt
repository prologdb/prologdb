package com.github.prologdb.execplan.index

import com.github.prologdb.PrologDatabase
import com.github.prologdb.execplan.PlanStep
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.lazysequence.LazySequence
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

class IndexLookupStep private constructor(
    /** The predicate as originally given in the query */
    protected val predicateAsInQuery: Predicate,
    /** The index of the argument in the query whichs index is to be used */
    protected val targetArgumentIndex: Int,

    protected val strategy: IndexLookupStepStrategy,

    /** Used to create random variables and avoid collisions */
    randomVariableScope: RandomVariableScope
) : PlanStep {
    init {
        if (targetArgumentIndex < 0 || targetArgumentIndex >= predicateAsInQuery.arguments.size) {
            throw IndexOutOfBoundsException("Argument index is out of bounds: arity = ${predicateAsInQuery.arity}, index = $targetArgumentIndex")
        }
    }

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

    override fun execute(db: PrologDatabase, randomVarsScope: RandomVariableScope, variables: VariableBucket): LazySequence<Unification> {
        TODO()
    }
}