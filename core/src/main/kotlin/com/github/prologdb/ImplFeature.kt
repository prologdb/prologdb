package com.github.prologdb

import com.github.prologdb.parser.ParseException
import com.github.prologdb.parser.SyntaxError
import com.github.prologdb.parser.source.SourceLocation
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.indexing.RangeQueryFactIndex

@JvmInline
value class ImplFeature(val spec: Term) {
    init {
        if (spec.variables.isNotEmpty()) {
            throw ParseException.ofSingle(
                SyntaxError("feature specs must be ground.", SourceLocation.EOF)
            )
        }
    }

    override fun toString() = spec.toString()

    companion object {
        /** a fact store or index store persists across restarts of the DB server */
        @JvmStatic
        val PERSISTENT = ImplFeature(Atom("persistent"))

        /**
         * a fact index that has improved performance on range queries compared to scan+filter on the index
         * @see RangeQueryFactIndex
         */
        val EFFICIENT_RANGE_QUERIES = ImplFeature(Atom("efficient_range_queries"))

        /** TODO: what is this used for?? remove? */
        @JvmStatic
        val ACCELERATED = ImplFeature(Atom("accelerated"))
    }
}