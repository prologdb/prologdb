package com.github.prologdb.dbms

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.prologdb.dbms.catalog.SystemCatalogJacksonModule
import com.github.prologdb.indexing.IndexDefinition
import com.github.prologdb.indexing.IndexTemplate
import com.github.prologdb.util.concurrency.locks.PIDLockFile
import com.github.prologdb.util.filesystem.setOwnerReadWriteEverybodyElseNoAccess
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.UUID
import java.util.stream.Collectors.toList

private val log = LoggerFactory.getLogger("prologdb.dbmanager")

/**
 * Manages the files in the data directory of the database.
 *
 * TODO: ensure thread safety
 */
class DataDirectoryManager private constructor(
    private val dataDirectory: Path
) {

    init {
        require(dataDirectory.isAbsolute)
        dataDirectory.setOwnerReadWriteEverybodyElseNoAccess()
    }

    private val catalogDirectory = dataDirectory.resolve(CATALOG_SUBDIR)
    private val catalogMapper = jacksonObjectMapper().also {
        it.registerModule(SystemCatalogJacksonModule)
    }
    private val catalogWriter = catalogMapper.writerWithDefaultPrettyPrinter()

    @Volatile
    private var closed: Boolean = false

    private val systemCatalogModificationMutex = Any()
    private val lock = PIDLockFile(dataDirectory.resolve("lock.pid").toFile())
    init {
        if (!lock.tryLock()) {
            throw IOException("Failed to lock data directory $dataDirectory. An instance of prologdb seems to be already running.")
        }
    }

    @Volatile
    var systemCatalog: SystemCatalog = loadSystemCatalog()
        private set

    /**
     * @param revision If not null, attempts to load this revision of the catalog
     * @throws SystemCatalogNotFoundException if the requested revision is not available
     */
    fun loadSystemCatalog(revision: Long? = null): SystemCatalog {
        requireOpen()

        if (!Files.exists(catalogDirectory)) {
            Files.createDirectories(catalogDirectory)
            catalogDirectory.setOwnerReadWriteEverybodyElseNoAccess()
        }

        val actualRevision: Long = revision
            ?: Files.walk(catalogDirectory, 1)
                .filter { Files.isRegularFile(it) && !Files.isSameFile(catalogDirectory, it) }
                .collect(toList())
                .mapNotNull { file ->
                    CATALOG_REVISION_FILENAME_PATTERN.matchEntire(file.fileName.toString())
                        ?.groupValues
                        ?.get(1)
                        ?.toLongOrNull()
                }
                .maxOrNull()
            ?: run {
                systemCatalog = SystemCatalog.INITIAL
                return saveSystemCatalog(systemCatalog)
            }

        val file = catalogDirectory.resolve("system-$actualRevision.json")
        val fileContent = try {
            file.toFile().readText(Charsets.UTF_8)
        } catch (ex: FileNotFoundException) {
            throw SystemCatalogNotFoundException(actualRevision, file, ex)
        }

        return catalogMapper.readValue<SystemCatalog>(fileContent)
            .copy(revision = actualRevision)
    }

    /**
     * Saves the given system catalog with the given revision. When this method returns, the catalog is guaranteed
     * to be saved to disc.
     * @param keepOldRevisions Any revision older than the given number before [asRevision] will be deleted. Pass null
     * to not delete any revisions.
     * @return the given catalog, with [SystemCatalog.revision] set to the given [asRevision] value.
     */
    fun saveSystemCatalog(
        catalog: SystemCatalog,
        asRevision: Long = catalog.nextRevisionNumber(),
        keepOldRevisions: Long? = 5
    ): SystemCatalog {
        require(catalog.revision < asRevision)
        requireOpen()

        if (!Files.exists(catalogDirectory)) {
            Files.createDirectories(catalogDirectory)
            catalogDirectory.setOwnerReadWriteEverybodyElseNoAccess()
        }

        val newCatalog: SystemCatalog
        synchronized(systemCatalogModificationMutex) {
            if (systemCatalog.revision > catalog.revision) {
                throw SystemCatalogOutdatedException("The catalog has changed. Attempting to go from revision ${catalog.revision} to $asRevision, but the catalog has moved on to revision ${systemCatalog.revision}")
            }

            val serialized = catalogWriter.writeValueAsString(catalog).toByteArray(Charsets.UTF_8)
            val file = catalogDirectory.resolve("system-$asRevision.json")
            try {
                Files.write(file, serialized, StandardOpenOption.CREATE_NEW)
            } catch (ex: FileAlreadyExistsException) {
                throw SystemCatalogOutdatedException("A catalog with revision $asRevision is already saved on disc.", ex)
            }
            file.setOwnerReadWriteEverybodyElseNoAccess()
            newCatalog = catalog.copy(revision = asRevision)
        }

        if (keepOldRevisions != null) {
            try {
                Files.walk(catalogDirectory, 1)
                    .filter { file -> file != catalogDirectory }
                    .filter { file ->
                        val revisionNumber = CATALOG_REVISION_FILENAME_PATTERN.matchEntire(file.fileName.toString())
                            ?.groupValues
                            ?.get(1)
                            ?.toLongOrNull()

                        revisionNumber != null && revisionNumber < asRevision - keepOldRevisions
                    }
                    .forEach {
                        log.debug("Deleting obsolete system catalog revision at $it")
                        Files.deleteIfExists(it)
                    }
            } catch (ex: Exception) {
                log.warn("Failed to delete old system catalog revisions", ex)
            }
        }

        return newCatalog
    }

    fun modifySystemCatalog(action: (SystemCatalog) -> SystemCatalog): SystemCatalog {
        synchronized(systemCatalogModificationMutex) {
            var newCatalog = action(systemCatalog)
            newCatalog = saveSystemCatalog(newCatalog)
            systemCatalog = newCatalog
            return newCatalog
        }
    }

    fun scopedForPredicate(uuid: UUID): PredicateScope {
        requireOpen()

        val predicateDirectory = dataDirectory.resolve("predicates").resolve(uuid.toString())
        Files.createDirectories(predicateDirectory)
        predicateDirectory.setOwnerReadWriteEverybodyElseNoAccess()

        return PredicateScope(uuid, predicateDirectory)
    }

    fun scopedForIndex(uuid: UUID): IndexScope {
        requireOpen()

        val indexDirectory = dataDirectory.resolve("indices").resolve(uuid.toString())
        Files.createDirectories(indexDirectory)
        indexDirectory.setOwnerReadWriteEverybodyElseNoAccess()

        return IndexScope(uuid, indexDirectory)
    }

    fun close() {
        closed = true
        lock.release()
    }

    inner class PredicateScope internal constructor(val uuid: UUID, val directory: Path) {

        val catalogEntry: SystemCatalog.Predicate
            get() = systemCatalog.allPredicates.getValue(uuid)

        fun modifyPredicateCatalog(action: (SystemCatalog.Predicate) -> SystemCatalog.Predicate): SystemCatalog.Predicate {
            val newSystemCatalog = modifySystemCatalog { systemCatalog ->
                systemCatalog.withModifiedPredicate(uuid, action)
            }

            return newSystemCatalog.allPredicates.getValue(uuid)
        }
    }

    inner class IndexScope internal constructor(val uuid: UUID, val directory: Path) {
        val catalogEntry: SystemCatalog.Index
            get() = systemCatalog.allIndices.getValue(uuid)

        val indexDefinition: IndexDefinition = catalogEntry.let { IndexDefinition(
            it.name,
            IndexTemplate(it.unscopedTemplateGoal),
            it.key,
            it.storeAdditionally,
        )}

        fun modifyIndexCatalog(action: (SystemCatalog.Index) -> SystemCatalog.Index): SystemCatalog.Index {
            val newSystemCatalog = modifySystemCatalog { systemCatalog ->
                systemCatalog.withModifiedIndex(uuid, action)
            }

            return newSystemCatalog.allIndices.getValue(uuid)
        }
    }

    private fun requireOpen() {
        if (closed) {
            throw IllegalStateException("This manager instance is already closed.")
        }
    }

    companion object {

        /**
         * Opens the given data directory. If the directory
         * does not yet exist, creates it.
         */
        @JvmStatic
        fun open(dataDirectory: Path): DataDirectoryManager {
            return DataDirectoryManager(dataDirectory)
        }

        private const val CATALOG_SUBDIR = "catalog"
        private val CATALOG_REVISION_FILENAME_PATTERN = Regex("system-(\\d+)\\.json")

    }
}

class SystemCatalogNotFoundException(revision: Long, atPath: Path, cause: Throwable? = null) : RuntimeException(
    "Did not find system catalog with revision $revision at $atPath",
    cause
) {

    init {
        require(atPath.isAbsolute)
    }
}

class SystemCatalogOutdatedException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)