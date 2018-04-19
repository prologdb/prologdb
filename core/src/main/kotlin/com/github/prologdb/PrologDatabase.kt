package com.github.prologdb

import com.github.prologdb.indexing.IndexByArgumentMap
import com.github.prologdb.runtime.knowledge.library.PredicatePrototype

/**
 * Groups everything that belongs to one database. One server process can host multiple databases (just like
 * SQL servers can).
 *
 * This is as close to an SQL schema as it gets.
 */
interface PrologDatabase {
    val predicateStores: Map<PredicatePrototype, PredicateStore>

    val indexes: Map<PredicatePrototype, IndexByArgumentMap>
}