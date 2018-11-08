package com.github.prologdb.dbms

import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.util.concurrency.locks.PIDLockFile
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

    private val storeScopes: MutableMap<ClauseIndicator, PredicateStoreScope> = HashMap()

    val metadata: KnowledgeBaseMetadata by lazy { KnowledgeBaseMetadata(TODO()) }

    fun scopedForPredicatesOf(indicator: ClauseIndicator): PredicateStoreScope {
        synchronized(mutex) {
            return storeScopes.computeIfAbsent(indicator) { PredicateStoreScope(indicator) }
        }
    }

    /**
     * Manages files in the data directory belonging to the predicate
     * store for the [indicator].
     */
    inner class PredicateStoreScope internal constructor(val indicator: ClauseIndicator) {
        /** Everything synchronizes on this for thread-safety. */
        private val mutex = Any()
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