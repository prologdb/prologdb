package com.github.prologdb.indexing

/**
 * The result of an index lookup. Can be eagerly loaded or a lazy iteration handle, depending on
 * the implementation.
 */
interface IndexLookupResult : Iterable<IndexEntry> {
    /**
     * @return an iterator over all matching entries
     */
    override fun iterator(): Iterator<IndexEntry>

    companion object {
        /**
         * An empty [IndexLookupResult]
         */
        val NONE: IndexLookupResult = object : IndexLookupResult {
            override fun iterator(): Iterator<IndexEntry> = emptyList<IndexEntry>().iterator()
        }
    }
}