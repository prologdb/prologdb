package com.github.prologdb.storage.fact

import com.github.prologdb.parser.ParseException
import com.github.prologdb.parser.SyntaxError
import com.github.prologdb.parser.source.SourceLocation
import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.Term

@JvmInline
value class FactStoreFeature(val spec: Term) {
    init {
        if (spec.variables.isNotEmpty()) {
            throw ParseException.ofSingle(
                SyntaxError("FactStoreFeature specs must be ground.", SourceLocation.EOF)
            )
        }
    }

    companion object {
        @JvmStatic
        val PERSISTENT = FactStoreFeature(Atom("persistent"))
        @JvmStatic
        val VOLATILE = FactStoreFeature(Atom("volatile"))
        @JvmStatic
        val ACCELERATED = FactStoreFeature(Atom("accelerated"))
    }
}