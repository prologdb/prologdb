package com.github.prologdb.orchestration.engine

import com.github.prologdb.dbms.toSaveFileName
import com.github.prologdb.util.concurrency.locks.PIDLockFile
import com.github.prologdb.util.metadata.FileMetadataRepository
import com.github.prologdb.util.metadata.MetadataRepository
import com.github.prologdb.util.metadata.load
import java.nio.file.Files
import java.nio.file.Path
import java.util.*

/**
 * Manages the data-directory of an entire server (that includes multiple
 * knowledge bases in relation to [com.github.prologdb.dbms.DataDirectoryManager]).
 *
 * * Data for every knowledge base
 * * Metadata
 * * User data and permissions
 */
class ServerDataDirectoryManager private constructor(
    val dataDirectory: Path
) {
    val pidLock = PIDLockFile(dataDirectory.resolve("lock.pid").toFile())
    init {
        if (!pidLock.tryLock()) {
            throw ConcurrentModificationException("Failed to acquire PID lock on $pidLock")
        }
    }

    val serverMetadata: ServerMetadata by lazy {
        val path = dataDirectory.resolve("server.meta")

        if (!Files.exists(path)) {
            Files.createDirectories(path.parent)
            Files.createFile(path)
        }

        ServerMetadata(FileMetadataRepository(path))
    }

    fun directoryForKnowledgeBase(name: String): Path {
        return dataDirectory.resolve("knowledge-bases").resolve(name.toSaveFileName())
    }

    fun close() {
        pidLock.release()

        // TODO: close serverMetadata?
    }

    companion object {
        fun open(dataDirectory: Path): ServerDataDirectoryManager {
            if (!Files.exists(dataDirectory)) {
                Files.createDirectories(dataDirectory)
            } else if (!Files.isDirectory(dataDirectory)) {
                throw IllegalArgumentException("Data directory is a file.")
            }

            return ServerDataDirectoryManager(dataDirectory)
        }
    }
}

class ServerMetadata(private val repo: MetadataRepository) {

    private val knowledgeBasesMutex = Any()

    val allKnowledgeBaseNames: Iterable<String>
        get() {
            synchronized(knowledgeBasesMutex) {
                return (repo.load("knowledgeBases", Array<String>::class.java) ?: emptyArray()).toList()
            }
        }

    fun onKnowledgeBaseAdded(name: String) {
        synchronized(knowledgeBasesMutex) {
            val all = repo.load<List<String>>("knowledgeBases") ?: emptyList()
            repo.save("knowledgeBases", (all + listOf(name)).toSet())
        }
    }

    fun onKnowledgeBaseRemoved(name: String) {
        synchronized(knowledgeBasesMutex) {
            val all = repo.load<List<String>>("knowledgeBases") ?: emptyList()
            repo.save("knowledgeBases", all - setOf(name))
        }
    }
}