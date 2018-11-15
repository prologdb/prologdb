package com.github.prologdb.dbms

import com.github.prologdb.async.*
import com.github.prologdb.execplan.planner.ExecutionPlanner
import com.github.prologdb.execplan.planner.PlanningInformation
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import com.github.prologdb.runtime.builtin.NativeCodeRule
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
import com.github.prologdb.storage.predicate.PredicateStoreFeature
import com.github.prologdb.storage.predicate.PredicateStoreLoader
import java.util.*
import java.util.concurrent.ConcurrentHashMap

/**
 * The central element of the database management system (compare RDBMS). For every
 * knowledge base a server is managing there is one instance of this class.
 */
class PersistentKnowledgeBase(
    private val directoryManager: DataDirectoryManager,
    private val factStoreLoader: PredicateStoreLoader,
    @Volatile private var planner: ExecutionPlanner
) : KnowledgeBase {
    override val operators: OperatorRegistry
        get() = ISOOpsOperatorRegistry

    private val predicateStores: MutableMap<ClauseIndicator, PredicateStore> = ConcurrentHashMap()

    private val rules: MutableMap<ClauseIndicator, List<Rule>> = ConcurrentHashMap()

    private val builtins: MutableMap<ClauseIndicator, NativeCodeRule> = ConcurrentHashMap()

    init {
        directoryManager.persistedClauses.forEach {
            predicateStores[it] = factStoreLoader.load(directoryManager.scopedForFactsOf(it))
                ?: throw IllegalStateException("Cannot to load fact store for $it: was reported to exist by directoryManager but then not found")
        }

        // TODO: rules

        // TODO: refactor into libraries?
        builtins[ClauseIndicator.of(Builtin_Assert_1)] = Builtin_Assert_1
    }

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
    ) : DBProofSearchContext
    {
        override val fulfillAttach: suspend LazySequenceBuilder<Unification>.(Query, VariableBucket) -> Unit = { query, vars ->
            val plan = planner.planExecution(query, this@PersistentKnowledgeBase.planningInfo, randomVariableScope)
            plan.execute(this, this@PSContext, vars)
        }

        override val predicateStores: Map<ClauseIndicator, PredicateStore> = this@PersistentKnowledgeBase.predicateStores
        override val rules: Map<ClauseIndicator, List<Rule>> = this@PersistentKnowledgeBase.rules
        override val staticBuiltins: Map<ClauseIndicator, NativeCodeRule> = this@PersistentKnowledgeBase.builtins

        override fun assurePredicateStore(indicator: ClauseIndicator): PredicateStore {
            return this@PersistentKnowledgeBase.predicateStores.computeIfAbsent(indicator) {
                val clauseScopedDirMgr = directoryManager.scopedForFactsOf(indicator)
                val existing = factStoreLoader.load(clauseScopedDirMgr)
                return@computeIfAbsent if (existing == null) {
                    // TODO: storage preferences
                    factStoreLoader.create(
                        clauseScopedDirMgr,
                        setOf(PredicateStoreFeature.PERSISTENT),
                        setOf(PredicateStoreFeature.ACCELERATED)
                    )
                } else {
                    existing
                }
            }
        }
    }

    private val planningInfo = object : PlanningInformation {
        override val existingDynamicFacts: Set<ClauseIndicator> = predicateStores.keys
        override val existingRules: Set<ClauseIndicator> = rules.keys
        override val staticBuiltins: Set<ClauseIndicator> = builtins.keys
    }
}

private fun <T> lazyError(error: Throwable) = buildLazySequence<T>(IrrelevantPrincipal) { throw error }