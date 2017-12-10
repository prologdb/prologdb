package com.github.prologdb.indexing

import com.github.prologdb.runtime.knowledge.library.LibraryEntry
import com.github.prologdb.runtime.knowledge.library.MutableLibraryEntryStore

/**
 * Stores instances of a particular [PredicatePrototype] (hence partial) as a list and holds references to
 * [PredicateArgumentIndex]es into that list for quicker access.
 */
interface IndexedPartialLibraryEntryStore : MutableLibraryEntryStore {
    override val exports: List<LibraryEntry>

    /**
     * @return The index for the given argument; if no index is maintained for this argument, `null` is returned.
     */
    fun getIndexForArgument(argumentIndex: Int): PredicateArgumentIndex?
}