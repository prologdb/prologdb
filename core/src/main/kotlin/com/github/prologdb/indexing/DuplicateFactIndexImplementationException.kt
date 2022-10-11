package com.github.prologdb.indexing

class DuplicateFactIndexImplementationException(val id: String) : RuntimeException("A loader for fact index implementation id $id is already registered.")