package com.github.prologdb.storage.predicate

import com.github.prologdb.storage.StorageException

/**
 * Thrown when multiple [PredicateStorageFeature]s are to be combined for a
 * [PredicateStore] where at least two of the features are not compatible
 * with each other, e.g. [PredicateStorageFeature.PERSISTENT] and [PredicateStorageFeature.VOLATILE]
 */
class ConflictingFeaturesException(message: String, cause: Throwable? = null) : StorageException(message, cause)