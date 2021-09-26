package com.github.prologdb.util.metadata

class InMemoryMetadataRepository : MetadataRepository {

    private val metadata = mutableMapOf<String, Any>()

    override fun save(key: String, value: Any) {
        synchronized(metadata) {
            metadata[key] = value
        }
    }

    override fun remove(key: String) {
        metadata.remove(key)
    }

    override fun <T : Any> load(key: String, valueClass: Class<T>): T? {
        synchronized(metadata) {
            val obj = metadata[key] ?: return null
            if (obj.javaClass != valueClass) {
                throw RuntimeException("The value for key $key is not of type ${valueClass.name} (is ${obj::class.qualifiedName})")
            }

            return obj as T
        }
    }
}
