package com.github.prologdb.storage.predicate

import com.github.prologdb.storage.AcceleratedStorage
import com.github.prologdb.storage.PersistentStorage
import com.github.prologdb.storage.VolatileStorage
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf

enum class PredicateStorageFeature(
    private val predicate: (KClass<out PredicateStore>) -> Boolean
) {
    /** Data persists across restarts and reboots */
    PERSISTENT(hasAnnotation(PersistentStorage::class) and !hasAnnotation(VolatileStorage::class)),

    /** Data does not persist across restarts and reboots */
    VOLATILE(hasAnnotation(VolatileStorage::class) and !hasAnnotation(PersistentStorage::class)),

    /**
     * The storage has a cache that is considerably faster
     * than the stores main means of storage. This also includes
     * 100% in-memory stores.
     */
    ACCELERATED(hasAnnotation(AcceleratedStorage::class));

    infix fun isSupportedBy(storageImplCls: KClass<out PredicateStore>): Boolean = predicate(storageImplCls)
}

private fun hasAnnotation(annotationCls: KClass<out Annotation>): (KClass<*>) -> Boolean
    = { cls -> cls.annotations.any { it.annotationClass.isSubclassOf(annotationCls) }}

private operator fun <T> ((T) -> Boolean).not(): (T) -> Boolean = { !this(it) }

private infix fun <T> ((T) -> Boolean).and(other: (T) -> Boolean): (T) -> Boolean = { this(it) && other(it)}