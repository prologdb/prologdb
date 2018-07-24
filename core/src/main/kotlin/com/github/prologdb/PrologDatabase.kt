package com.github.prologdb

import com.github.prologdb.indexing.IndexByArgumentMap
import com.github.prologdb.runtime.knowledge.Rule
import com.github.prologdb.runtime.knowledge.library.PredicateIndicator
import com.github.prologdb.storage.predicate.PredicateStore

/**
 * Groups everything that belongs to one database. One server process can host multiple databases (just like
 * SQL servers can).
 *
 * This is as close to an SQL schema as it gets.
 */
interface PrologDatabase {
    val predicateStores: Map<PredicateIndicator, PredicateStore>
    val rules: Map<PredicateIndicator, Set<Rule>>
    val indexes: Map<PredicateIndicator, IndexByArgumentMap>
}