package com.github.prologdb.storage.predicate

import com.github.prologdb.storage.StorageException

/**
 * Thrown when multiple [FactStoreFeature]s are to be combined for a
 * [FactStore] where at least two of the features are not compatible
 * with each other, e.g. [FactStoreFeature.PERSISTENT] and [FactStoreFeature.VOLATILE]
 */
class ConflictingFeaturesException(message: String, cause: Throwable? = null) : StorageException(message, cause)