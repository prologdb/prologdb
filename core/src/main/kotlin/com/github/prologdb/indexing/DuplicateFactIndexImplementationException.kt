package com.github.prologdb.indexing

class DuplicateFactIndexImplementationException(
    val id: String,
    val existingLoaderClass: Class<out FactIndexImplementationLoader>,
) : RuntimeException("A loader for fact index implementation id $id is already registered: ${existingLoaderClass.canonicalName}")