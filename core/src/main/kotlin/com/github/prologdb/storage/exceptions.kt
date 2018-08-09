package com.github.prologdb.storage

/** Thrown for all storage related errors */
open class StorageException(message: String, cause: Throwable? = null) : Exception(message, cause)

class OutOfStorageMemoryException(message: String, cause: Throwable? = null) : StorageException(message, cause)