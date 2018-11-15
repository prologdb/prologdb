package com.github.prologdb.storage.fact

import com.github.prologdb.storage.AcceleratedStorage
import com.github.prologdb.storage.PersistentStorage
import com.github.prologdb.storage.VolatileStorage
import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation

enum class FactStoreFeature(
    private val predicate: (KClass<out FactStore>) -> Boolean
) {
    /** Data persists across restarts and reboots */
    PERSISTENT(hasAnnotation<PersistentStorage>() and !hasAnnotation<VolatileStorage>()),

    /** Data does not persist across restarts and reboots */
    VOLATILE(hasAnnotation<VolatileStorage>() and !hasAnnotation<PersistentStorage>()),

    /**
     * The storage has a cache that is considerably faster
     * than the stores main means of storage. This also includes
     * 100% in-memory stores.
     */
    ACCELERATED(hasAnnotation<AcceleratedStorage>());

    infix fun isSupportedBy(storageImplCls: KClass<out FactStore>): Boolean = predicate(storageImplCls)
}

private inline fun <reified T : Annotation> hasAnnotation(): (KClass<*>) -> Boolean
    = { cls -> cls.findAnnotation<T>() != null }

private operator fun <T> ((T) -> Boolean).not(): (T) -> Boolean = { !this(it) }

private infix fun <T> ((T) -> Boolean).and(other: (T) -> Boolean): (T) -> Boolean = { this(it) && other(it)}