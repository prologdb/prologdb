package com.github.prologdb.dbms

import com.github.prologdb.async.LazySequence
import com.github.prologdb.async.Principal
import com.github.prologdb.async.buildLazySequence
import com.github.prologdb.execplan.planner.ExecutionPlanner
import com.github.prologdb.indexing.IndexByArgumentMap
import com.github.prologdb.runtime.RandomVariableScope
import com.github.prologdb.runtime.knowledge.Rule
import com.github.prologdb.runtime.knowledge.library.PredicateIndicator
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.runtime.unification.VariableBucket
import com.github.prologdb.storage.predicate.PredicateStore
import com.github.prologdb.util.concurrency.BroadcastConcurrentQueue
import java.util.concurrent.BlockingQueue

/**
 * Groups everything that belongs to one database. One server process can host multiple databases (just like
 * SQL servers can).
 * For each database instance, there are threads waiting for queries to handle. Each of these threads has one
 * instance of this class. Changes on the DB level are propagated from the thread that does the change to the
 * others using a [BroadcastConcurrentQueue].
 *
 * **This class is intended for single threaded use!**
 */
class PrologDatabaseView(
    val planner: ExecutionPlanner,
    private val changes: BlockingQueue<DatabaseUpdateEvent>,
    initialPredicateStores: Map<PredicateIndicator, PredicateStore>,
    initialRules: Map<PredicateIndicator, Set<Rule>>,
    initialIndices: Map<PredicateIndicator, IndexByArgumentMap>
) {
    internal val _predicateStores: MutableMap<PredicateIndicator, PredicateStore> = HashMap(initialPredicateStores)

    internal val _rules: MutableMap<PredicateIndicator, MutableSet<Rule>> = {
        val _rules = HashMap<PredicateIndicator, MutableSet<Rule>>()
        for ((pi, rules) in initialRules) {
            _rules[pi] = HashSet(rules)
        }
        _rules
    }()

    internal val _indexes: MutableMap<PredicateIndicator, IndexByArgumentMap> = HashMap(initialIndices)

    val predicateStores: Map<PredicateIndicator, PredicateStore>
        get() = _predicateStores

    val rules: Map<PredicateIndicator, Set<Rule>>
        get() = _rules

    val indexes: Map<PredicateIndicator, IndexByArgumentMap>
        get() = _indexes

    fun execute(command: Query, asPrincipal: Principal, randomVariableScope: RandomVariableScope = RandomVariableScope()): LazySequence<Unification> {
        val executionPlan = planner.planExecution(command, this, randomVariableScope)
        return buildLazySequence(asPrincipal) {
            executionPlan.execute(this, this@PrologDatabaseView, randomVariableScope, VariableBucket())
        }
    }

    /**
     * Applies the outstanding schema changes from [changes] to this view.
     */
    fun incorporateLatestChanges() {
        val events = ArrayList<DatabaseUpdateEvent>(changes.size + 3)
        changes.drainTo(events)
        events.forEach { it.applyTo(this) }
    }
}

/**
 * Changes on the schema level of a database
 */
sealed class DatabaseUpdateEvent {
    abstract fun applyTo(dbv: PrologDatabaseView)
}

/**
 * When a new predicate store has been added
 */
data class PredicateStoreAddedEvent(val store: PredicateStore) : DatabaseUpdateEvent() {
    override fun applyTo(dbv: PrologDatabaseView) {
        if (store.indicator in dbv._predicateStores) {
            throw IllegalStateException("A predicate store for ${store.indicator} is already registered with this database view.")
        }

        dbv._predicateStores[store.indicator] = store
    }
}

data class RuleAddedEvent(val rule: Rule) : DatabaseUpdateEvent() {
    override fun applyTo(dbv: PrologDatabaseView) {
        val indicator = PredicateIndicator.of(rule.head)
        val set: MutableSet<Rule> = dbv._rules[indicator] ?: {
            val newSet = HashSet<Rule>()
            dbv._rules[indicator] = newSet
            newSet
        }()

        set += rule
    }
}