package com.github.prologdb.dbms

import com.github.prologdb.runtime.knowledge.library.ClauseIndicator
import com.github.prologdb.util.concurrency.locks.PIDLockFile
import com.github.prologdb.util.metadata.FileMetadataRepository
import com.github.prologdb.util.metadata.MetadataRepository
import java.io.FileNotFoundException
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

        val storageFileManager: StorageFileManager = DirectoryStorageFileManager(contextDirectory)
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

/**
 * Manages arbitrary-purpose storage files inside a scope/directory,
 * e.g. all storage files for a predicate indicator.
 */
interface StorageFileManager {
    /**
     * Reserves a filename. Then invokes the given code with the path.
     * Forwards exceptions from `init`; if an exception is thrown,
     * the path is freed again.
     * @return first: forwarded from the `init` function, second: an ID that
     * can be used with the [initStorageFile] function to open the file again.
     */
    fun <T> initStorageFile(purpose: Purpose, init: (Path) -> T): Pair<T, String>

    /**
     * If the file with the given ID exists, returns its path.
     * @throws FileNotFoundException if a file with the given ID does not exist.
     */
    @Throws(FileNotFoundException::class)
    fun getStorageFilePath(id: String): Path

    /**
     * Denotes the purpose of a storage file created with [createStorageFile].
     */
    enum class Purpose {
        /** Stores entire facts with all data */
        FACT_FULL,
        
        /** Stores index data for a fact */
        FACT_INDEX
    }
}

private class DirectoryStorageFileManager(
    val directory: Path
) : StorageFileManager {
    
    private val mutex = Any()
    
    override fun <T> initStorageFile(purpose: StorageFileManager.Purpose, init: (Path) -> T): Pair<T, String> {
        synchronized(mutex) {
            var counter = 0
            var path: Path
            var id: String
            do {
                id = "storage_${purpose.name.toLowerCase()}_$counter"
                path = directory.resolve(id)
                counter++
            } while (Files.isRegularFile(path))

            try {
                return Pair(init(path), id)
            } catch (ex: Throwable) {
                try {
                    Files.deleteIfExists(path)
                } catch (ex2: Throwable) {
                    ex.addSuppressed(ex2)
                }

                throw ex
            }
        }
    }

    @Throws(FileNotFoundException::class)
    override fun getStorageFilePath(id: String): Path {
        synchronized(mutex) {
            val directoryAbsolute = directory.toAbsolutePath()
            val path = directoryAbsolute.resolve(id).toAbsolutePath()

            if (path.nameCount != directoryAbsolute.nameCount + 1) {
                // ID uses directories -> invalid
                throw IllegalArgumentException("Storage file ID $id is not valid.")
            }

            if (!Files.isRegularFile(path)) {
                throw FileNotFoundException("Storage file with ID $id does not exist")
            }

            return path
        }
    }
}