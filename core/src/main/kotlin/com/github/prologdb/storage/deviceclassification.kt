package com.github.prologdb.storage

import java.io.File
import java.io.InputStreamReader
import java.nio.file.Path
import java.nio.file.Paths

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
    '/'  -> UnixStorageDeviceInformationProvider()
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

// ---------------------------
private class UnixStorageDeviceInformationProvider : StorageDeviceInformationProvider {
    override fun getRootDeviceProperties(ofPath: Path): StorageDeviceProperties {
        val rootDevice = ofPath.rootDevice ?: throw IllegalArgumentException("Data in this path will not be stored in a physical device.")
        return StorageDeviceProperties(
            physicalStorageStrategy = when(File("/sys/block/${rootDevice.fileName}/queue/rotational").readText().trim()) {
                "1" -> StorageStrategy.ROTATIONAL_DISKS
                else -> StorageStrategy.UNKNOWN
            }
        )
    }

    /**
     * The device on which data in this path is being stored. That is the actual device,
     * without partitions or encryption layers (e.g. /dev/sda even if the path is mounted
     * for /dev/sda3).
     * Null if this path does not map to a mounted location (the case for non-existing paths).
     */
    private val Path.rootDevice: Path?
        get() {
            val mountRoot = mountRoot ?: return null
            val leafDevice = mounts[mountRoot]!!

            fun BlockDeviceTreeNode.containsDevice(devicePath: Path): Boolean {
                return devicePath == this.devicePath || children.any { it.containsDevice(devicePath) }
            }

            return blockDeviceTree.firstOrNull { it.containsDevice(leafDevice) }?.devicePath
        }

    /**
     * The mount point of this path, e.g. if this path is `/usr/bin/bash` and that directory
     * is stored on device `/dev/sda4` which is mounted at `/usr`, returns `/`. This value
     * can be used as a valid key for [mounts].
     * Null if this path does not map to a mounted location (the case for non-existing paths).
     */
    private val Path.mountRoot: Path?
        get() = mounts.keys
                .sortedBy { it.nameCount }
                .reversed()
                .firstOrNull { mountPath -> this.toAbsolutePath().startsWith(mountPath) }

    /**
     * All mounts in the system; keys: mountpoint, value: device file
     */
    private val mounts: Map<Path, Path> by lazy {
        val mountOut = captureOutput("mount")
        TODO("parse mountOut")
        // emptyMap()
    }

    private val blockDeviceTree: Set<BlockDeviceTreeNode> by lazy {
        /**
         * Reads the next device off the given list.
         * @return The parsed device (including children) and the remaining lines possibly containing more devices.
         */
        fun readNode(lsblkOutLines: List<String>): Pair<BlockDeviceTreeNode, List<String>> {
            val nameLine = lsblkOutLines[0]
            val nameLineTrimmed = nameLine.trim()
            val name = if (nameLineTrimmed.startsWith("├─") || nameLineTrimmed.startsWith("└─")) nameLineTrimmed.substring(2) else nameLine

            val children = mutableSetOf<BlockDeviceTreeNode>()

            var remainingLines = lsblkOutLines.subList(1, lsblkOutLines.size)
            while (remainingLines.isNotEmpty() && remainingLines[0].spaceIndentationAmount > nameLine.spaceIndentationAmount) {
                val childResult = readNode(remainingLines)
                children += childResult.first
                remainingLines = childResult.second
            }

            return Pair(BlockDeviceTreeNode(Paths.get("/dev/${name}"), children), remainingLines)
        }

        val lsblkOut = captureOutput("lsblk", "-o", "name", "-t")
        var currentLines = lsblkOut.subList(1, lsblkOut.size) // 1st line are headings
        val rootNodes = mutableSetOf<BlockDeviceTreeNode>()

        while (currentLines.isNotEmpty()) {
            val rootDeviceNodeResult = readNode(currentLines)

            if (!rootDeviceNodeResult.first.devicePath.fileName.startsWith("loop")) {
                rootNodes += rootDeviceNodeResult.first
            }

            currentLines = rootDeviceNodeResult.second
        }

        return@lazy rootNodes
    }

    /** The number of leading spaces in this string */
    private val String.spaceIndentationAmount: Int
        get() = takeWhile { it == ' ' }.length

    private data class BlockDeviceTreeNode(
        val devicePath: Path,
        val children: Set<BlockDeviceTreeNode>
    )
}