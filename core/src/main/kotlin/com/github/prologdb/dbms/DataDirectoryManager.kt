package com.github.prologdb.dbms

import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.util.concurrency.locks.PIDLockFile
import com.github.prologdb.util.metadata.FileMetadataRepository
import com.github.prologdb.util.metadata.MetadataRepository
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors.toSet

/**
 * Manages the files in the data directory of a knowledge base.
 */
class DataDirectoryManager private constructor(
    private val dataDirectory: Path
) {
    private val lock = PIDLockFile(dataDirectory.resolve("lock.pid").toFile())
    init {
        if (!lock.tryLock()) {
            throw IOException("Failed to lock data directory $dataDirectory")
        }
    }

    /** Everything synchronizes on this for thread-safety. */
    private val mutex = Any()

    private val storeScopes: MutableMap<ClauseIndicator, ClauseStoreScope>
    init {
        // init store scopes
        storeScopes = discoverClauseStoresWithin(dataDirectory.resolve("clauses"))
            .fold(HashMap()) { m, s -> m[s.indicator] = s; m }
    }

    val metadata: KnowledgeBaseMetadata by lazy {
        val file = dataDirectory.resolve("meta")
        Files.createDirectories(file.parent)
        KnowledgeBaseMetadata(FileMetadataRepository(file))
    }

    /**
     * Clauses for which any persistent state is already present.
     */
    val persistedClauses: Set<ClauseIndicator> = storeScopes.keys

    fun scopedForFactsOf(indicator: ClauseIndicator): ClauseStoreScope {
        synchronized(mutex) {
            return storeScopes.computeIfAbsent(indicator) { ClauseStoreScope(indicator) }
        }
    }

    /**
     * Manages files in the data directory belonging to the predicate
     * store for the [indicator].
     */
    inner class ClauseStoreScope internal constructor(val indicator: ClauseIndicator) {
        /** Everything synchronizes on this for thread-safety. */
        private val mutex = Any()

        private val contextDirectory = dataDirectory
            .resolve("clauses")
            .resolve(indicator.toString().toSaveFileName())

        init {
            if (!Files.exists(contextDirectory)) {
                Files.createDirectories(contextDirectory)
            }
        }

        val metadata: MetadataRepository by lazy {
            val path = contextDirectory.resolve("meta")
            if (!Files.exists(path)) Files.createFile(path)
            FileMetadataRepository(path)
        }

        /**
         * Reserves a filename for a file purposed to storing facts.
         * Then invokes the given code with the path.
         * Forwards exceptions from `init`; if an exception is thrown,
         * the path is freed again.
         * @return forwarded from the `init` function
         */
        fun <T> createStorageFile(init: (Path) -> T): T {
            synchronized(mutex) {
                var counter = 0
                var path: Path
                do {
                    path = contextDirectory.resolve("storage_facts_$counter")
                    counter++
                } while (Files.isRegularFile(path))

                try {
                    return init(path)
                }
                catch (ex: Throwable) {
                    try {
                        Files.deleteIfExists(path)
                    }
                    catch (ex2: Throwable) {
                        ex.addSuppressed(ex2)
                    }

                    throw ex
                }
            }
        }
    }

    private fun discoverClauseStoresWithin(path: Path): Set<ClauseStoreScope> {
        if (!Files.exists(path)) return emptySet()

        return Files.list(path)
            .filter { Files.isDirectory(it) }
            .map {
                try {
                    ClauseIndicator.parse(
                        it.fileName.toString().fromSaveFileName()
                    )
                }
                catch (ex: IllegalArgumentException) {
                    throw IllegalStateException("Failed to discover clause persistent state in ${it.normalize()}", ex)
                }
            }
            .map { ClauseStoreScope(it) }
            .collect(toSet())
    }

    companion object {
        /**
         * Opens the given data directory. If the directory
         * does not yet exist, creates it.
         */
        @JvmStatic
        fun open(dataDirectory: Path): DataDirectoryManager {
            if (!Files.exists(dataDirectory)) {
                Files.createDirectories(dataDirectory)
            }

            return DataDirectoryManager(dataDirectory)
        }
    }
}