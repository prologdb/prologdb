package com.github.prologdb.storage.fact

class DuplicateFactStoreImplementationException(
    val id: String,
    val existingLoaderClass: Class<out FactStoreImplementationLoader>,
) : RuntimeException("A loader for fact store implementation id $id is already registered: ${existingLoaderClass.canonicalName}")