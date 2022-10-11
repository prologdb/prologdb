package com.github.prologdb.indexing

import com.github.prologdb.storage.fact.FactStore
import com.github.prologdb.storage.fact.PersistenceID

/**
 * Indexes facts of one specific indicator, e.g. foo/2. When the fact is asserted, the real fact is unified (isolated)
 * with the [template]. The resulting instantiations (only of the variables in the [template]) then are combined to
 * form the index key. The [PersistenceID] is then associated with that key in the index (e.g. for hash indexes: the
 * persistence ID is the value to the key; for B-like-trees the persistence ID is attached to the key/value).
 */
interface FactIndex {
    
    val definition: IndexDefinition
    
    /**
     * @param key The result of isolatedly unifying the query predicate with the template,
     * retaining only variables from the template.
     * 
     * @return The [PersistenceID]s for which unifying the original fact with the template would result
     * in the same instantiations as [key].
     * @throws InvalidIndexKeyException If not all of the templates variables have a value within
     * the given [key].
     */
    @Throws(InvalidIndexKeyException::class)
    fun find(key: IndexKey): IndexLookupResult

    /**
     * To be called when a fact is [FactStore.store]d into the relating [FactStore]. Updates the index accordingly.
     *
     * Behaviour of duplicate keys is implementation-defined.
     */
    fun onInserted(entry: IndexEntry)

    /**
     * To be called when a fact is removed from the underlying list. Updates the index accordingly.
     *
     * @param persistenceID Index in the source list of predicates from which the term is being removed.
     */
    fun onRemoved(persistenceID: PersistenceID, key: IndexKey)
}

/**
 * A [FactIndex] that supports efficient range-queries (faster than O(n) where n is the number of facts known to
 * the index).
 */
interface RangeQueryFactIndex : FactIndex {
    /**
     * @param lowerBound the lower bound. If null, there is no lower bound (query for greaterThan / greaterThanOrEqualTo)
     * @param lowerInclusive Is ignored if [lowerBound] is null
     * @param upperBound the upper bound. If null, there is no upper bound (query for lessThan / lessThanOrEqualTo)
     * @param upperInclusive Is ignored if [upperBound] is null
     * @return the [PersistenceID]s of the facts whichs keys are within the given bound
     * @throws IllegalArgumentException If both `lowerBound` and `upperBound` are `null`
     */
    fun findBetween(lowerBound: IndexKey?, lowerInclusive: Boolean, upperBound: IndexKey?, upperInclusive: Boolean): IndexLookupResult
}
