package com.github.prologdb.storage

import java.io.File
import java.io.InputStreamReader
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.collections.Collection
import kotlin.collections.List
import kotlin.collections.Map
import kotlin.collections.MutableMap
import kotlin.collections.Set
import kotlin.collections.any
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.filterNotNull
import kotlin.collections.firstOrNull
import kotlin.collections.lastIndex
import kotlin.collections.map
import kotlin.collections.mutableListOf
import kotlin.collections.mutableMapOf
import kotlin.collections.mutableSetOf
import kotlin.collections.plusAssign
import kotlin.collections.reversed
import kotlin.collections.set
import kotlin.collections.sortedBy
import kotlin.collections.toMap

/**
 * Properties of the device that stores data in this path. Null if this path is not backed by a physical device.
 */
val Path.rootDeviceProperties: StorageDeviceProperties?
    get() = STORAGE_DEVICE_INFORMATION_PROVIDER.getRootDeviceProperties(this)

interface StorageDeviceInformationProvider {
    /**
     * Same contract as [Path.rootDeviceProperties]
     */
    fun getRootDeviceProperties(ofPath: Path): StorageDeviceProperties?
}

data class StorageDeviceProperties(
    val physicalStorageStrategy: StorageStrategy,

    /**
     * The size for read&write buffers that will presumably yield optimal performance.
     * Null for devices that can handle all buffer sizes equally well (e.g. SSDs). In those
     * cases, 8192 and 4069 are sensible defaults.
     */
    val optimalIOSize: Int?
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
            physicalDrive.mediaType,
            physicalDrive.physicalSectorSize
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

        /**
         *  The size of a physical sector on the disk; this is usually the smallest IO size the disk supports
         *  via the connecting protocol (e.g. SATA).
         */
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
    override fun getRootDeviceProperties(ofPath: Path): StorageDeviceProperties? {
        val rootDevice = ofPath.rootDevice ?: return null
        return StorageDeviceProperties(
            physicalStorageStrategy = if(rootDevice.isRotational) StorageStrategy.ROTATIONAL_DISKS else StorageStrategy.UNKNOWN,
            optimalIOSize = rootDevice.optimalIOSize
        )
    }

    /**
     * The device on which data in this path is being stored. That is the actual device,
     * without partitions or encryption layers (e.g. /dev/sda even if the path is mounted
     * for /dev/sda3).
     * Null if this path does not map to a mounted location (the case for non-existing paths).
     */
    private val Path.rootDevice: DeviceRef?
        get() {
            val mountRoot = mountRoot ?: return null
            val leafDevice = mounts[mountRoot]!!

            fun BlockDeviceTreeNode.containsDevice(device: DeviceRef): Boolean {
                return device == this.device || children.any { it.containsDevice(device) }
            }

            return blockDeviceTree.firstOrNull { it.containsDevice(leafDevice) }?.device
        }

    /**
     * The mount point of this path, e.g. if this path is `/usr/bin/bash` and that directory
     * is stored on device `/dev/sda4` which is mounted at `/usr`, returns `/user`. This value
     * can be used as a valid key for [mounts].
     * Null if this path does not map to a mounted location (the case for non-existing paths).
     */
    private val Path.mountRoot: Path?
        get() = mounts.keys
                .sortedBy { it.nameCount }
                .reversed()
                .firstOrNull { mountPath -> this.toAbsolutePath().startsWith(mountPath) }

    /**
     * Mount points of the actual devices in the system (all mounts excluding special devices like sysfs, proc, ...).
     * Key: Mount Point, Value: Device
     */
    private val mounts: Map<Path, DeviceRef> by lazy {
        val mountOut = captureOutput("mount")
        mountOut
            .map { line ->
                // lines have this format: <device file> on <mount point> type <...>
                val indexOn = line.indexOf("on")
                val indexType = line.indexOf("type")

                val devicePathStr = line.substring(0, indexOn - 1).trim()
                val mountPointStr = line.substring(indexOn + 2, indexType - 1).trim()

                if (devicePathStr.startsWith("/dev")) {
                    Pair(Paths.get(mountPointStr), DeviceRef(Paths.get(devicePathStr)))
                } else {
                    null
                }
            }
            .filterNotNull()
            .toMap()
    }

    private val blockDeviceTree: Set<BlockDeviceTreeNode> by lazy {
        fun minusTreeDisplayPrefix(str: String): String {
            val indentAmount = str.spaceIndentationAmount
            var pivot = str

            if (indentAmount > 0) {
                pivot = pivot.substring(indentAmount)
            }

            if (pivot.startsWith("├─") || pivot.startsWith("└─")) {
                pivot = pivot.substring(2)
            }

            return pivot
        }

        /**
         * Reads the next device off the given list.
         * @return The parsed device (including children) and the remaining lines possibly containing more devices.
         */
        fun readNodes(lsblkOutLines: List<String>): Set<BlockDeviceTreeNode> {
            val nodes = mutableSetOf<BlockDeviceTreeNode>()
            var lineIndex = 0
            while (lineIndex <= lsblkOutLines.lastIndex) {
                val name = lsblkOutLines[lineIndex].trim()
                lineIndex++

                val subLines = mutableListOf<String>()
                while (
                    lineIndex < lsblkOutLines.size && (
                        lsblkOutLines[lineIndex].startsWith("├─") ||
                        lsblkOutLines[lineIndex].startsWith("└─") ||
                        lsblkOutLines[lineIndex].startsWith(" ") ||
                        lsblkOutLines[lineIndex].startsWith("\t")
                    )
                ) {
                    subLines += minusTreeDisplayPrefix(lsblkOutLines[lineIndex])
                    lineIndex++
                }

                val subNodes = readNodes(subLines)
                val devicePath = Paths.get("/dev/$name")
                nodes += BlockDeviceTreeNode(DeviceRef(devicePath), subNodes)
            }

            return nodes
        }

        val lsblkOut = captureOutput("lsblk", "-o", "name", "-t")
        val devices = readNodes(lsblkOut.subList(0, lsblkOut.size)) // cut first line with NAME header
        return@lazy devices
    }

    /** The number of leading spaces in this string */
    private val String.spaceIndentationAmount: Int
        get() = takeWhile { it == ' ' }.length

    private data class BlockDeviceTreeNode(
        val device: DeviceRef,
        val children: Set<BlockDeviceTreeNode>
    )

    /**
     * A simple reference to a device/device file.
     */
    private data class DeviceRef(val devicePath: Path) {
        init {
            assert(devicePath.toString().startsWith("/dev/"))
        }

        /**
         * Assumes thah `this` path is a path to a device file, e.g. /dev/sda. Returns whether that device
         * uses rotational disks (see /sys/block/$DEV/queue/rotational).
         */
        val isRotational: Boolean by lazy { File("/sys/block/${devicePath.fileName}/queue/rotational").readText().trim() == "1" }

        /**
         * The size for read&write buffers that will presumably yield optimal performance.
         * Null for devices that can handle all buffer sizes equally well (e.g. SSDs).
         */
        val optimalIOSize: Int? by lazy {
            val reportedByKernel = Integer.parseInt(
                File("/sys/block/${devicePath.fileName}/queue/optimal_io_size").readText().trim()
            )

            if (reportedByKernel > 0) reportedByKernel else null
        }
    }
}