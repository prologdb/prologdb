package com.github.prologdb.dbms

import com.github.prologdb.async.*
import com.github.prologdb.dbms.builtin.DBLibrary
import com.github.prologdb.dbms.builtin.ModifyLibrary
import com.github.prologdb.execplan.planner.ExecutionPlanner
import com.github.prologdb.execplan.planner.PlanningInformation
import com.github.prologdb.runtime.PrologException
import com.github.prologdb.runtime.PrologRuntimeException
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import com.github.prologdb.runtime.knowledge.Authorization
import com.github.prologdb.runtime.knowledge.KnowledgeBase
import com.github.prologdb.runtime.knowledge.Rule
import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.runtime.knowledge.library.DefaultOperatorRegistry
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket
import com.github.prologdb.storage.fact.FactStore
import com.github.prologdb.storage.fact.FactStoreFeature
import com.github.prologdb.storage.fact.FactStoreLoader
import java.util.*
import java.util.concurrent.ConcurrentHashMap

/**
 * The central element of the database management system (compare RDBMS). For every
 * knowledge base a server is managing there is one instance of this class.
 */
class PersistentKnowledgeBase(
    private val directoryManager: DataDirectoryManager,
    private val factStoreLoader: FactStoreLoader,
    @Volatile private var planner: ExecutionPlanner
) : KnowledgeBase {
    override val operators = DefaultOperatorRegistry()
    init {
        operators.include(ISOOpsOperatorRegistry)
    }

    private val factStores: MutableMap<ClauseIndicator, FactStore> = ConcurrentHashMap()

    private val rules: MutableMap<ClauseIndicator, List<Rule>> = ConcurrentHashMap()

    /**
     * To be synchronized on for modifications in either [loadedLibraries] or
     * [builtinImplementations].
     */
    private val libraryLoadingMutex = Any()
    private val loadedLibraries: MutableSet<DBLibrary> = HashSet()
    private val builtinImplementations: MutableMap<ClauseIndicator, Rule> = ConcurrentHashMap()

    init {
        directoryManager.persistedClauses.forEach { indicator ->
            factStoreLoader.load(directoryManager.scopedForFactsOf(indicator))?.let { factStore ->
                factStores[indicator] = factStore
            }
        }

        // TODO: rules

        // Load DB-Specific builtins
        load(ModifyLibrary)
    }

    /**
     * Loads the given library. The loading is not persistent (has to be repeated after
     * every launch).
     *
     * @throws PrologException If a different library with the same name is already loaded.
     * @throws PrologException If there is a conflict between the exports & operators of the library
     *                         and what is already present in this knowledge base.
     */
    fun load(library: DBLibrary) {
        synchronized(libraryLoadingMutex) {
            // prevent double-load
            loadedLibraries.firstOrNull { it.name == library.name }?.let {
                if (it === library) {
                    // double load, just ignore
                    return
                }

                throw PrologException("Library with name ${library.name} is already loaded: already loaded ${System.identityHashCode(it)}, attempted to load ${System.identityHashCode(library)}")
            }

            // detect double declaration
            library.exports.keys.firstOrNull { it in builtinImplementations }?.let {
                throw PrologException("Cannot load library ${library.name}: static predicate $it already declared by another library")
            }
            library.exports.keys.firstOrNull { it in factStores }?.let {
                throw PrologException("Cannot load library ${library.name}: static export $it is already a dynamic predicate in this knowledge base.")
            }
            library.operators.allOperators.firstOrNull { newDef ->
                val existingDefs = operators.getOperatorDefinitionsFor(newDef.name)
                existingDefs.any { existingDef ->
                    newDef.type.isSameArgumentRelationAs(existingDef.type) && newDef != existingDef
                }
            } ?.let {
                throw PrologException("Cannot load library ${library.name}: operator $it defined in the library would change an existing one.")
            }

            // all good => load
            library.exports.forEach { (indicator, code) ->
                builtinImplementations[indicator] = code
            }
            loadedLibraries.add(library)
            operators.include(library.operators)
        }
    }

    fun close() {
        TODO()
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

        override val factStores: Map<ClauseIndicator, FactStore> = this@PersistentKnowledgeBase.factStores
        override val rules: Map<ClauseIndicator, List<Rule>> = this@PersistentKnowledgeBase.rules
        override val staticBuiltins: Map<ClauseIndicator, Rule> = this@PersistentKnowledgeBase.builtinImplementations

        override fun assureFactStore(indicator: ClauseIndicator): FactStore {
            return this@PersistentKnowledgeBase.factStores.computeIfAbsent(indicator) {
                val clauseScopedDirMgr = directoryManager.scopedForFactsOf(indicator)
                val existing = factStoreLoader.load(clauseScopedDirMgr)
                return@computeIfAbsent if (existing == null) {
                    // TODO: storage preferences
                    factStoreLoader.create(
                        clauseScopedDirMgr,
                        setOf(FactStoreFeature.PERSISTENT),
                        setOf(FactStoreFeature.ACCELERATED)
                    )
                } else {
                    existing
                }
            }
        }
    }

    private val planningInfo = object : PlanningInformation {
        override val existingDynamicFacts: Set<ClauseIndicator> = factStores.keys
        override val existingRules: Set<ClauseIndicator> = rules.keys
        override val staticBuiltins: Set<ClauseIndicator> = builtinImplementations.keys
    }
}

private fun <T> lazyError(error: Throwable) = buildLazySequence<T>(IrrelevantPrincipal) { throw error }