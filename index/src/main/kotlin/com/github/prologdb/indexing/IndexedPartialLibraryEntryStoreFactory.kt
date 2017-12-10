package com.github.prologdb.indexing

import com.github.prologdb.runtime.knowledge.library.PredicatePrototype

interface IndexedPartialLibraryEntryStoreFactory {
    /**
     * Creates an [IndexedPartialLibraryEntryStore]. There are no requirements to this store other than those
     * defined in the interface; use this method to create indexes suited to the data the store is expected
     * to be holding.
     */
    fun createEntryStoreFor(prototype: PredicatePrototype): IndexedPartialLibraryEntryStore
}