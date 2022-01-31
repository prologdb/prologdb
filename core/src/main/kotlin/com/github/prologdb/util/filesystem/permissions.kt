@file:JvmName("PermissionUtils")
package com.github.prologdb.util.filesystem

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermissions

private val POSIX_DIRECTORY_PERMISSIONS_OWNER_ONLY = setOf(
    PosixFilePermission.OWNER_READ,
    PosixFilePermission.OWNER_WRITE,
    PosixFilePermission.OWNER_EXECUTE
)

private val POSIX_FILE_PERMISSIONS_OWNER_ONLY = setOf(
    PosixFilePermission.OWNER_READ,
    PosixFilePermission.OWNER_WRITE
)

fun Path.setOwnerReadWriteEverybodyElseNoAccess() {
    try {
        if (Files.isDirectory(this)) {
            Files.setPosixFilePermissions(this, POSIX_DIRECTORY_PERMISSIONS_OWNER_ONLY)
        } else {
            Files.setPosixFilePermissions(this, POSIX_FILE_PERMISSIONS_OWNER_ONLY)
        }
    }
    catch (ex: UnsupportedOperationException) {
        toFile().apply {
            setReadable(false, false)
            setReadable(true, true)
            setWritable(false, false)
            setWritable(true, true)
            setExecutable(false, false)
        }
    }
}