package com.github.prologdb.storage

/** Thrown for all storage related errors */
open class StorageException(message: String, cause: Throwable? = null) : Exception(message, cause)

class OutOfStorageMemoryException(message: String, cause: Throwable? = null) : StorageException(message, cause)

/**
 * Thrown when a storage implementation is requested that is not known/found or none of the
 * known/found storage implementations fulfills the requirements of the request.
 */
class NoSuchStorageImplementationException(message: String, cause: Throwable? = null) : StorageException(message, cause)