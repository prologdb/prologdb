package com.github.prologdb.storage.fact

import com.github.prologdb.dbms.DataDirectoryManager
import com.github.prologdb.runtime.FullyQualifiedClauseIndicator
import com.github.prologdb.storage.StorageException

/**
 * Creates new [FactStore]s for given [FullyQualifiedClauseIndicator]s.
 *
 * Implementations need not be thread safe. Code using objects of this
 * type should synchronize on the loader object.
 */
interface FactStoreLoader {
    /**
     * Creates a new predicate in the data directory[FactStore] for facts of the given indicator.
     * @param directoryManager manager for the directory scoped to the knowledge base name and clause indicator
     * @param requiredFeatures The returned fact store is guaranteed to have all these features
     *                         contained in [requiredFeatures].
     * @param desiredFeatures The factory will try to find an implementation that has the desired features
     *                        on a best-effort/best-match basis. The actual algorithm is implementation-defined.
     *                        Implementations may interpret the natural order of this set to be the order of
     *                        descending priority (e.g. first element in the set is the most desired feature).
     *                        Sets created using [setOf] expose that behaviour of natural order.
     * @throws ConflictingFeaturesException If two or more of the features in [requiredFeatures] are in conflict with
     *                                      each other
     * @return first: the newly created system catalog entry (as written to the [directoryManager]),
     *         second: the fact store for the new predicate
     * @throws StorageException If a fact store for the given database and predicate indicator already exists.
     */
    fun create(
        directoryManager: DataDirectoryManager.PredicateScope,
        requiredFeatures: Set<FactStoreFeature>,
        desiredFeatures: Set<FactStoreFeature>
    ) : FactStore

    /**
     * Creates a new predicate in the data directory[FactStore] for facts of the given indicator.
     *
     * @throws FactStoreImplementationUnknownException
     */
    fun create(
        directoryManager: DataDirectoryManager.PredicateScope,
        implementationId: String
    ) : FactStore

    /**
     * Loads the [FactStore] for facts of the given indicator. Though stores can be loaded
     * multiple times, if both loaded stores are used, the behaviour of the stores is undefined.
     * @param directoryManager manager for the directory scoped to the knowledge base name and clause indicator
     * @return the loaded store or `null` if a store for the given database and predicate does not exist yet.
     */
    fun load(directoryManager: DataDirectoryManager.PredicateScope) : FactStore?

    /**
     * Assures any physical files associated with the fact store for the given predicate
     * are removed from the filesystem and the disk space occupied has been made available.
     */
    fun destroy(directoryManager: DataDirectoryManager.PredicateScope)
}
