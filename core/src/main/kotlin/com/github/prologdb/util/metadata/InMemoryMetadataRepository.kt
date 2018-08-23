package com.github.prologdb.util.metadata

import kotlin.reflect.KClass

class InMemoryMetadataRepository : MetadataRepository {

    private val metadata = mutableMapOf<String, Any>()

    override fun save(key: String, value: Any) {
        synchronized(metadata) {
            metadata[key] = value
        }
    }

    override fun <T : Any> load(key: String, valueClass: KClass<T>): T? {
        synchronized(metadata) {
            val obj = metadata[key] ?: return null
            if (obj::class != valueClass) {
                throw RuntimeException("The value for key $key is not of type ${valueClass.qualifiedName} (is ${obj::class.qualifiedName})")
            }

            return obj as T
        }
    }
}
