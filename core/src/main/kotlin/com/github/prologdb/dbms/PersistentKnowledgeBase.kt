package com.github.prologdb.dbms

import com.github.prologdb.async.*
import com.github.prologdb.execplan.planner.ExecutionPlanner
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import com.github.prologdb.runtime.knowledge.Authorization
import com.github.prologdb.runtime.knowledge.KnowledgeBase
import com.github.prologdb.runtime.knowledge.Rule
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.knowledge.library.OperatorRegistry
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket
import com.github.prologdb.storage.predicate.PredicateStore
import java.nio.file.Path
import java.util.*

/**
 * The central element of the database management system (compare RDBMS). For every
 * knowledge base a server is managing there is one instance of this class.
 */
class PersistentKnowledgeBase(
    val dataDirectory: Path,
    @Volatile var planner: ExecutionPlanner
) : KnowledgeBase {
    override val operators: OperatorRegistry
        get() = ISOOpsOperatorRegistry

    override fun fulfill(query: Query, authorization: Authorization, randomVariableScope: RandomVariableScope): LazySequence<Unification> {
        val principal = UUID.randomUUID()
        val ctxt = PSContext(principal, authorization, randomVariableScope)

        return buildLazySequence(principal) {
            ctxt.fulfillAttach(this, query, VariableBucket())
        }
    }

    override fun invokeDirective(name: String, authorization: Authorization, arguments: Array<out Term>): LazySequence<Unification> {
        return lazyError(PrologRuntimeException("Directive $name/${arguments.size} is not defined."))
    }

    private inner class PSContext(
        override val principal: Principal,
        override val authorization: Authorization,
        override val randomVariableScope: RandomVariableScope
    ) : DBProofSearchContext {
        override val fulfillAttach: suspend LazySequenceBuilder<Unification>.(Query, VariableBucket) -> Unit = { query, vars ->
            val plan = planner.planExecution(query, this@PersistentKnowledgeBase, randomVariableScope)
            plan.execute(this, this@PSContext, vars)
        }

        override val predicateStores: Map<ClauseIndicator, PredicateStore>
            get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
        override val rules: Map<ClauseIndicator, List<Rule>>
            get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
    }
}

private fun <T> lazyError(error: Throwable) = buildLazySequence<T>(IrrelevantPrincipal) { throw error }