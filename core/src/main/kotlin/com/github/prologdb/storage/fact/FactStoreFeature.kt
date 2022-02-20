package com.github.prologdb.storage.fact

import com.github.prologdb.runtime.term.Atom
import com.github.prologdb.runtime.term.Term
import com.github.prologdb.storage.AcceleratedStorage
import com.github.prologdb.storage.PersistentStorage
import com.github.prologdb.storage.VolatileStorage
import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation

@JvmInline
value class FactStoreFeature(val spec: Term) {
    companion object {
        @JvmStatic
        val PERSISTENT = FactStoreFeature(Atom("persistent"))
        @JvmStatic
        val VOLATILE = FactStoreFeature(Atom("volatile"))
        @JvmStatic
        val ACCELERATED = FactStoreFeature(Atom("accelerated"))
    }
}