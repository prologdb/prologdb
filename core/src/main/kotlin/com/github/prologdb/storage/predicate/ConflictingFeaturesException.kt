package com.github.prologdb.storage.predicate

import com.github.prologdb.storage.StorageException

/**
 * Thrown when multiple [PredicateStoreFeature]s are to be combined for a
 * [PredicateStore] where at least two of the features are not compatible
 * with each other, e.g. [PredicateStoreFeature.PERSISTENT] and [PredicateStoreFeature.VOLATILE]
 */
class ConflictingFeaturesException(message: String, cause: Throwable? = null) : StorageException(message, cause)