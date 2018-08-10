package com.github.prologdb.storage

import java.io.File
import java.io.InputStreamReader
import java.nio.file.Path

val Path.rootDeviceProperties: StorageDeviceProperties
    get() = STORAGE_DEVICE_INFORMATION_PROVIDER.getRootDeviceProperties(this)

interface StorageDeviceInformationProvider {
    fun getRootDeviceProperties(ofPath: Path): StorageDeviceProperties
}

data class StorageDeviceProperties(
    val physicalStorageStrategy: StorageStrategy
)

enum class StorageStrategy {
    ROTATIONAL_DISKS,
    SOLID_STATE,
    UNKNOWN
}

private val STORAGE_DEVICE_INFORMATION_PROVIDER: StorageDeviceInformationProvider = when(File.separatorChar) {
    '\\' -> WindowsStorageDeviceInformationProvider()
    else -> throw Exception("Cannot initialize storageDeviceInformationProvider: unknown OS")
}

private fun captureOutput(vararg command: String): List<String> {
    val pb = ProcessBuilder(*command).start()
    pb.waitFor()
    if (pb.exitValue() != 0) {
        throw RuntimeException("${command[0]} exited with code ${pb.exitValue()}")
    }

    return InputStreamReader(pb.inputStream).use { it.readLines() }
}

// ------------------

class WindowsStorageDeviceInformationProvider : StorageDeviceInformationProvider {
    override fun getRootDeviceProperties(ofPath: Path): StorageDeviceProperties {
        ofPath.root ?: throw IllegalArgumentException("The given path does not have a root.")
        val root = ofPath.root!!.toString()
        if (!root.matches(Regex("^[A-Z]:\\\\$"))) {
            throw IllegalArgumentException("'$root' is not a valid windows drive letter")
        }

        val pathDriveLetter = root[0]

        val logicalDrive = locigalDrives
            .firstOrNull { it.driveLetter == pathDriveLetter }
            ?: throw IllegalArgumentException("Drive $pathDriveLetter:\\ is not mounted.")

        val physicalDrive = physicalDrives
            .firstOrNull { it.diskNumber == logicalDrive.diskNumber }
            ?: throw RuntimeException("Logical drive $pathDriveLetter:\\ does not belong to a physical drive.")

        return StorageDeviceProperties(
            physicalDrive.mediaType
        )
    }

    private val locigalDrives: Collection<LogicalDrive> by lazy {
        val outLines = captureOutput("powershell", "get-partition | Format-List -Property DiskNumber,DriveLetter")
        val objects = parsePowershellListFormatOutput(outLines)
        return@lazy objects.map { rawObj ->
            LogicalDrive(
                rawObj["DriveLetter"]!!.elementAtOrNull(0),
                Integer.parseInt(rawObj["DiskNumber"]!!)
            )
        }
    }

    /**
     * The physical drives as reported by powershells get-physicaldrive
     */
    private val physicalDrives: Collection<PhysicalDrive> by lazy {
        val outLines = captureOutput("powershell", "get-physicaldisk | Format-List -Property DeviceId,UniqueId,PhysicalSectorSize,MediaType")
        val objects = parsePowershellListFormatOutput(outLines)
        return@lazy objects.map { rawObj ->
            PhysicalDrive(
                Integer.parseInt(rawObj["DeviceId"]!!),
                rawObj["UniqueId"]!!,
                Integer.parseInt(rawObj["PhysicalSectorSize"]!!),
                when (rawObj["MediaType"]!!) {
                    "SSD" -> StorageStrategy.SOLID_STATE
                    "HDD" -> StorageStrategy.ROTATIONAL_DISKS
                    else -> StorageStrategy.UNKNOWN
                }
            )
        }
    }

    private data class PhysicalDrive(
        /** Simple ID for the device, usually 1 digit. Unique only within this OS runtime instance */
        val diskNumber: Int,

        /** Unique ID of the device, usually hex coded */
        val uniqueID: String,

        val physicalSectorSize: Int,
        val mediaType: StorageStrategy
    )

    private data class LogicalDrive(
        /** Is null when the partition is not mounted. */
        val driveLetter: Char?,

        /** The [PhysicalDrive.diskNumber] this partition is stored on. */
        val diskNumber: Int
    )

    companion object {
        private fun parsePowershellListFormatOutput(lines: List<String>): List<Map<String, String>> {
            val allObjects = mutableListOf<Map<String, String>>()
            var currentObject: MutableMap<String, String>? = null

            for (line in lines.map { it.trim() }) {
                if (line.isEmpty()) {
                    if (currentObject != null) {
                        allObjects += currentObject
                        currentObject = null
                    }
                    continue
                }

                if (currentObject == null) {
                    currentObject = mutableMapOf()
                }

                val (propertyName, value) = line.split(delimiters = ":", limit = 2)
                currentObject[propertyName.trim()] = value.trim()
            }

            return allObjects
        }
    }
}