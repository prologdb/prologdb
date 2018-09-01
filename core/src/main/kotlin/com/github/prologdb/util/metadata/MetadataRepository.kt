package com.github.prologdb.util.metadata

import kotlin.reflect.KClass

/**
 * A simple key-value store for other objects (database internal ones) to persist metadata.
 * Metadata must be saved as JSON; implementations mus use Jackson to convert to JSON.
 */
interface MetadataRepository {
    /**
     * Persists the given value in association with the given key. If
     * A value already exists for the given key, it will be overwritten.
     * @param key Allowed characters: alphanumeric plus `.` and `_`
     */
    fun save(key: String, value: Any)

    /**
     * Loads the value for the given key.
     * @return The value or `null` if no value is associated with the given key
     * @throws RuntimeException If the persisted JSON cannot be read back as a value of the given class.
     */
    fun <T : Any> load(key: String, valueClass: KClass<T>): T?
}

inline fun <reified T : Any> MetadataRepository.load(key: String): T? = load(key, T::class)

inline operator fun <reified T: Any> MetadataRepository.get(key: String): T? = load(key)

operator fun MetadataRepository.set(key: String, value: Any) = save(key, value)