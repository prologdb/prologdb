package com.github.prologdb.orchestration.engine

import com.github.prologdb.async.*
import com.github.prologdb.execplan.planner.PlanningInformation
import com.github.prologdb.orchestration.SessionContext
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import com.github.prologdb.runtime.knowledge.Authorization
import com.github.prologdb.runtime.knowledge.ProofSearchContext
import com.github.prologdb.runtime.knowledge.ReadWriteAuthorization
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.knowledge.library.OperatorRegistry
import com.github.prologdb.runtime.query.AndQuery
import com.github.prologdb.runtime.query.OrQuery
import com.github.prologdb.runtime.query.PredicateQuery
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket

/**
 * A knowledge base within which every clause is backed by kotlin code.
 */
open class ProgrammaticServerKnowledgeBase(
    override val operators: OperatorRegistry = ISOOpsOperatorRegistry,
    initCode: ProgramaticServerKnowledgeBaseBuilder.() -> Any?
) : ServerKnowledgeBase {

    private val directives: Map<ClauseIndicator, (SessionContext, Array<out Term>) -> LazySequence<Unification>>
    private val callables: Map<ClauseIndicator, (SessionContext, Array<out Term>) -> LazySequence<Unification>>

    init {
        val builder = ProgramaticServerKnowledgeBaseBuilder()
        builder.initCode()
        this.directives = builder.directives
        this.callables  = builder.callables
    }

    infix fun supportsDirective(indicator: ClauseIndicator): Boolean = indicator in directives

    override fun startQuery(session: SessionContext, query: Query, totalLimit: Long?): LazySequence<Unification> {
        val ctxt = ProgrammaticProofSearchContext(session, RandomVariableScope())
        return buildLazySequence(IrrelevantPrincipal) {
            ctxt.fulfillAttach(this@buildLazySequence, query, VariableBucket())
        }
    }

    override fun startDirective(session: SessionContext, command: Predicate, totalLimit: Long?): LazySequence<Unification> {
        val indicator = ClauseIndicator.of(command)
        val code = directives[indicator] ?: return lazyError(PrologRuntimeException("Directive $indicator not defined."))

        val results = code(session, command.arguments)
        return if (totalLimit != null) results.limitRemaining(totalLimit) else results
    }

    override fun close() {
        // Nothing to do
    }

    override val planningInformation = object : PlanningInformation {
        override val existingDynamicFacts: Set<ClauseIndicator> = emptySet()
        override val existingRules: Set<ClauseIndicator> = emptySet()
        override val staticBuiltins: Set<ClauseIndicator> = directives.keys
    }
    
    private inner class ProgrammaticProofSearchContext(
        private val session: SessionContext,
        override val randomVariableScope: RandomVariableScope
    ) : ProofSearchContext {

        override val principal: Principal = IrrelevantPrincipal

        // TODO? the callables should do the authorization themselves
        override val authorization: Authorization = ReadWriteAuthorization
        
        override val fulfillAttach: suspend LazySequenceBuilder<Unification>.(Query, VariableBucket) -> Unit = { q, variables ->
            when (q) {
                is AndQuery -> fulfillAndQuery(q, variables)
                is OrQuery -> for (goal in q.goals) fulfillOrQuery(q, variables)
                is PredicateQuery -> fulfillPredicate(q, variables)
            }
        }

        private suspend fun LazySequenceBuilder<Unification>.fulfillAndQuery(query: AndQuery, initialVariables: VariableBucket) {
            val substitutedGoals = query.goals
                .map { it.substituteVariables(initialVariables) }


            fulfillAllGoals(substitutedGoals, this@ProgrammaticProofSearchContext, initialVariables.copy())
        }

        private suspend fun LazySequenceBuilder<Unification>.fulfillAllGoals(goals: List<Query>, context: ProofSearchContext,
                                                                             vars: VariableBucket = VariableBucket()) {
            val goal = goals[0].substituteVariables(vars)

            buildLazySequence<Unification>(context.principal) {
                context.fulfillAttach(this, goal, VariableBucket())
            }
                .forEachRemaining { goalUnification ->
                    val goalVars = vars.copy()
                    for ((variable, value) in goalUnification.variableValues.values) {
                        if (value != null) {
                            // substitute all instantiated variables for simplicity and performance
                            val substitutedValue = value.substituteVariables(goalVars.asSubstitutionMapper())
                            if (goalVars.isInstantiated(variable)) {
                                if (goalVars[variable] != substitutedValue && goalVars[variable] != value) {
                                    // instantiated to different value => no unification
                                    return@forEachRemaining
                                }
                            }
                            else {
                                goalVars.instantiate(variable, substitutedValue)
                            }
                        }
                    }

                    if (goals.size == 1) {
                        // this was the last goal in the list and it is fulfilled
                        // the variable bucket now holds all necessary instantiations
                        yield(Unification(goalVars))
                    }
                    else {
                        fulfillAllGoals(goals.subList(1, goals.size), context, goalVars)
                    }
                }
        }

        private suspend fun LazySequenceBuilder<Unification>.fulfillOrQuery(query: OrQuery, initialVariables: VariableBucket) {
            for (goal in query.goals) {
                fulfillAttach(goal, initialVariables)
            }
        }

        private suspend fun LazySequenceBuilder<Unification>.fulfillPredicate(query: PredicateQuery, initialVariables: VariableBucket) {
            val rawInvocationPredicate = query.predicate
            
            val indicator = ClauseIndicator.of(rawInvocationPredicate)
            val callable = callables[indicator] ?: return
            
            val invocation = if (initialVariables.isEmpty) rawInvocationPredicate else rawInvocationPredicate.substituteVariables(initialVariables.asSubstitutionMapper())
            
            yieldAll(callable(session, invocation.arguments))
        }
    }
}

class ProgramaticServerKnowledgeBaseBuilder internal constructor() {

    internal val directives = mutableMapOf<ClauseIndicator, (SessionContext, Array<out Term>) -> LazySequence<Unification>>()
    internal val callables  = mutableMapOf<ClauseIndicator, (SessionContext, Array<out Term>) -> LazySequence<Unification>>()

    operator fun String.div(arity: Int) = ClauseIndicator.of(this, arity)

    fun directive(indicator: ClauseIndicator, code: (SessionContext, Array<out Term>) -> LazySequence<Unification>) {
        if (directives.putIfAbsent(indicator, code) != null) {
            throw IllegalStateException("Directive $this is already defined")
        }
    }
    
    fun callable(indicator: ClauseIndicator, code: (SessionContext, Array<out Term>) -> LazySequence<Unification>) {
        if (callables.putIfAbsent(indicator, code) != null) {
            if (indicator in callables) {
                throw IllegalArgumentException("Callable $this is already defined")
            }
        }
    }
}