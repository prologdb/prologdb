package com.github.prologdb.indexing

import com.github.prologdb.parser.ParseException
import com.github.prologdb.parser.SyntaxError
import com.github.prologdb.parser.source.SourceLocation
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.Term

@JvmInline
value class FactIndexFeature(val spec: Term) {
    init {
        if (spec.variables.isNotEmpty()) {
            throw ParseException.ofSingle(
                SyntaxError("FactStoreFeature specs must be ground.", SourceLocation.EOF)
            )
        }
    }

    companion object {
        @JvmStatic
        val PERSISTENT = FactIndexFeature(Atom("persistent"))

        @JvmStatic
        val VOLATILE = FactIndexFeature(Atom("volatile"))

        @JvmStatic
        val EFFICIENT_RANGE_QUERIES = FactIndexFeature(Atom("efficient_range_queries"))
    }
}