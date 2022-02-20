package com.github.prologdb.storage.fact

class DuplicateFactStoreImplementationException(val id: String) : RuntimeException("A loader for fact store implementation id $id is already registered.")