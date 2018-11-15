package com.github.prologdb.dbms

import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.util.concurrency.locks.PIDLockFile
import com.github.prologdb.util.metadata.FileMetadataRepository
import com.github.prologdb.util.metadata.MetadataRepository
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path

/**
 * Manages the files in the data directory of a knowledge base.
 */
class DataDirectoryManager private constructor(
    private val dataDirectory: Path
) {
    /** Everything synchronizes on this for thread-safety. */
    private val mutex = Any()

    private val storeScopes: MutableMap<ClauseIndicator, ClauseStoreScope> = HashMap()

    val metadata: KnowledgeBaseMetadata by lazy {
        val file = dataDirectory.resolve("meta")
        Files.createDirectories(file.parent)
        KnowledgeBaseMetadata(FileMetadataRepository(file))
    }

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

            val lock = PIDLockFile(dataDirectory.resolve("lock.pid").toFile())
            if (!lock.tryLock()) {
                throw IOException("Failed to lock data directory $dataDirectory")
            }

            try {
                return DataDirectoryManager(dataDirectory)
            }
            catch (ex: Throwable) {
                try {
                    lock.release()
                }
                catch (ex2: Throwable) {
                    ex.addSuppressed(ex2);
                }

                throw ex
            }
        }
    }
}