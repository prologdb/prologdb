package com.github.prologdb.execplan

import com.github.prologdb.PredicateStore
import com.github.prologdb.PrologDatabase
import com.github.prologdb.indexing.IndexSet
import com.github.prologdb.indexing.PredicateArgumentIndex
import com.github.prologdb.runtime.knowledge.library.PredicatePrototype
import com.github.prologdb.runtime.lazysequence.LazySequence
import com.github.prologdb.runtime.term.Predicate
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.runtime.unification.VariableBucket

/**
 * An element of an execution plan. Instances serve two purposes:
 * * provide the necessary inspection mechanisms/data so that the execution plan can be shown to the user
 * * execute their part of the execution plan
 */
interface PlanStep {
    /**
     * Runs the underlying query element on the given database with the given variable values.
     * @throws PrologQueryException
     */
    fun execute(db: PrologDatabase, variables: VariableBucket): LazySequence<Predicate>

    /** @return an explanation of this step in form of a predicate, e.g. scan(foobar/2) */
    fun explain(): Predicate
}

/**
 * Most simple plan: scan all known predicates of a given [PredicatePrototype]
 */
class ScanStep(
    /** The prototype that will be scanned */
    val prototype: PredicatePrototype

) : PlanStep {
    override fun execute(db: PrologDatabase, variables: VariableBucket): LazySequence<Predicate> {
        val predicateStore = db.predicateStores[prototype] ?: return LazySequence.empty()

        return predicateStore.all().toLazySequence(predicateStore)
    }

    override fun explain() = Predicate("scan", arrayOf(prototype.asIdiomatic()))
}

class SimpleIndexLookupStep(
        /** the target prototype */
        val prototype: PredicatePrototype,
        val
) : PlanStep {

}

data class IndexLookup(
        val prototype: PredicatePrototype,
        val argumentIndex: PredicateArgumentIndex,
        val lookupType: Type,
        val queryTerm: Term
) {
    init {
        if (lookupType == Type.RANGE) {
            var isValid = false

            // attempt to prove queryTerm is valid
            if (queryTerm is Predicate) {
                when (queryTerm.name) {
                    "lt", "lte", "gt", "gte" -> isValid = queryTerm.arity == 1
                    "between" -> isValid = queryTerm.arity == 2
                }
            }

            if (!isValid) throw IllegalArgumentException("When lookup type is RANGE, the query term must be an instance of lt/1, lte/1, gt/1, gte/2 or between/2, $queryTerm given")
        }
    }

    enum class Type {
        /**
         * Query for an exact or likely match with the [queryTerm]
         * @see PredicateArgumentIndex.find
         */
        MATCH,

        /**
         * Do a range query. The [queryTerm] is expected to be an instance of any of these:
         * * `lt/1`
         * * `lte/1`
         * * `gt/1`
         * * `gte/1`
         * * `between/2`
         *
         * @see RangeQueryPredicateArgumentIndex
         */
        RANGE
    }

    fun doLookup(db: PrologDatabase, variables: VariableBucket): Pair<PredicateStore, IndexSet> {
        TODO("implement")
    }

    fun explain() = Predicate("lookup", )
}

private fun IndexSet.toLazySequence(store: PredicateStore): LazySequence<Predicate> {
    val indexIt = iterator()

    return LazySequence.fromGenerator {
        while (indexIt.hasNext()) {
            return@fromGenerator store.getByIndex(indexIt.next()) ?: continue
        }
        null
    }
}