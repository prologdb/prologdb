package com.github.prologdb.dbms

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.prologdb.util.concurrency.locks.PIDLockFile
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermissions
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
        Files.createDirectories(dataDirectory, DIRECTORY_PERMISSIONS_OWNER_ONLY)
    }

    private val lock = PIDLockFile(dataDirectory.resolve("lock.pid").toFile())
    init {
        if (!lock.tryLock()) {
            throw IOException("Failed to lock data directory $dataDirectory")
        }
    }

    private val catalogMapper = jacksonObjectMapper()

    /**
     * @param revision If not null, attempts to load this revision of the catalog
     * @throws SystemCatalogNotFoundException if the requested revision is not available
     */
    fun loadSystemCatalog(revision: Long? = null): SystemCatalog {
        val actualRevision: Long = revision
            ?: Files.walk(dataDirectory.resolve(CATALOG_SUBDIR), 0)
                .collect(toList())
                .mapNotNull { file ->
                    CATALOG_REVISION_FILENAME_PATTERN.matchEntire(file.fileName.toString())
                        ?.groupValues
                        ?.get(1)
                        ?.toLongOrNull()
                }
                .max()
            ?: return SystemCatalog.INITIAL

        val file = dataDirectory.resolve(CATALOG_SUBDIR).resolve("system-$actualRevision.json")
        val fileContent = try {
            file.toFile().readText(Charsets.UTF_8)
        }
        catch (ex: FileNotFoundException) {
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
    fun saveSystemCatalog(catalog: SystemCatalog, asRevision: Long = catalog.revision + 1, keepOldRevisions: Long? = 5): SystemCatalog {
        val catalogDir = dataDirectory.resolve(CATALOG_SUBDIR)
        val serialized = catalogMapper.writeValueAsString(catalog).toByteArray(Charsets.UTF_8)
        val file = catalogDir.resolve("system-$asRevision")
        try {
            Files.write(file, serialized, StandardOpenOption.CREATE_NEW)
        }
        catch (ex: FileAlreadyExistsException) {
            throw SystemCatalogOutdatedException("A catalog with revision $asRevision is already saved on disc.", ex)
        }

        if (keepOldRevisions != null) {
            try {
                Files.walk(catalogDir, 0)
                    .filter { file ->
                        val revisionNumber = CATALOG_REVISION_FILENAME_PATTERN.matchEntire(file.fileName.toString())
                            ?.groupValues
                            ?.get(1)
                            ?.toLongOrNull()

                        revisionNumber != null && revisionNumber < asRevision - keepOldRevisions
                    }
                    .forEach { Files.deleteIfExists(it) }
            }
            catch(ex: Exception) {
                log.warn("Failed to delete old system catalog revisions", ex)
            }
        }

        return catalog.copy(revision = asRevision)
    }

    fun scopedForPredicate(uuid: UUID): PredicateScope {
        val predicateDirectory = dataDirectory.resolve("predicates").resolve(uuid.toString())
        Files.createDirectories(predicateDirectory, DIRECTORY_PERMISSIONS_OWNER_ONLY)

        return PredicateScope(uuid, predicateDirectory)
    }

    inner class PredicateScope(val predicateUuid: UUID, val directory: Path) {

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

        private val CATALOG_SUBDIR = "catalog"
        private val CATALOG_REVISION_FILENAME_PATTERN = Regex("catalog-(\\d+)\\.json")
        private val DIRECTORY_PERMISSIONS_OWNER_ONLY = PosixFilePermissions.asFileAttribute(setOf(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE
        ))
        private val FILE_PERMISSIONS_OWNER_ONLY = PosixFilePermissions.asFileAttribute(setOf(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE
        ))
    }
}

class SystemCatalogNotFoundException(revision: Long, atPath: Path, cause: Throwable? = null) : RuntimeException(
    "Did not find system catalog with revision $revision at $atPath"
) {
    init {
        require(atPath.isAbsolute)
    }
}

class SystemCatalogOutdatedException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)