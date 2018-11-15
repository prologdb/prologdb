package com.github.prologdb.util.concurrency.locks

import java.io.File
import java.io.RandomAccessFile
import java.lang.management.ManagementFactory
import java.lang.reflect.InvocationTargetException
import java.nio.channels.FileLock
import java.nio.charset.Charset
import javax.management.ReflectionException
import kotlin.concurrent.thread

/**
 * Implements a PID file that is used to lock access of multiple
 * processes to the same area on a drive.
 *
 * If not released manually, the lock is released when the JVM
 * exits (see [Runtime.addShutdownHook]).
 */
class PIDLockFile(val pidFile: File) {
    init {
        if (pidFile.parent == null || !pidFile.parentFile.isDirectory) {
            throw IllegalArgumentException("Invalid lock file given: must have an existing parent directory")
        }

        if (pidFile.isDirectory) {
            throw IllegalArgumentException("Invalid lock file given: must be a file (is a directory)")
        }
    }

    private val mutex = Any()

    private var currentLocking: RandomAccessFile? = null
    private var currentLock: FileLock? = null

    /**
     * Attempts to acquire the lock for this process (= instance of the JVM).
     * Returns true if this JVM already holds the lock.
     * If the lock is held by another process and that process
     * * is alive: returns false
     * * is dead: acquires the lock and returns true
     */
    fun tryLock(): Boolean {
        synchronized(mutex) {
            if (currentLocking != null) {
                return true
            }

            pidFile.createNewFile()

            val raf = RandomAccessFile(pidFile, "rws")

            try {
                val lock = raf.channel.tryLock()
                if (lock == null) {
                    // locked, cannot acquire
                    return false
                }

                try {
                    val existingPIDString = raf.readLine() ?: ""
                    if (existingPIDString.isNotBlank()) {
                        val existingPID = try {
                            existingPIDString.toLong()
                        } catch (ex: NumberFormatException) {
                            // don't know who is holding the lock
                            lock.release()
                            return false
                        }

                        if (existingPID == JVM_PROCESS_ID) {
                            // this JVM already owns the lock, great :)
                            return true
                        }

                        if (isProcessAlive(existingPID)) {
                            lock.release()
                            return false
                        }
                    }
                }
                catch (ex: Throwable) {
                    lock.release()
                    throw ex
                }

                // put our own PID in there
                raf.channel.truncate(0)
                raf.channel.force(true)
                raf.seek(0)
                raf.write(JVM_PROCESS_ID.toString(10).toByteArray(Charset.defaultCharset()))

                currentLocking = raf
                currentLock = lock

                Runtime.getRuntime().addShutdownHook(releaseHook)

                return true

            }
            catch (ex: Throwable) {
                try {
                    raf.close()
                }
                catch (ex2: Throwable) {
                    ex.addSuppressed(ex2)
                }

                throw ex
            }
        }
    }

    /**
     * Releases this lock
     *
     * @throws IllegalStateException If the lock is currently not acquired.
     */
    fun release() {
        synchronized(mutex) {
            if (currentLock == null || currentLocking == null) {
                throw IllegalStateException("Currently not acquired.")
            }

            currentLock!!.release()
            currentLocking!!.close()

            Runtime.getRuntime().removeShutdownHook(releaseHook)

            currentLock = null
            currentLocking = null

            pidFile.delete()
        }
    }

    /** is registered as a shutdown hook that will release the lock */
    private val releaseHook = thread(start = false) {
        release()
    }

    override fun toString() = pidFile.toString()
}

private fun isProcessAlive(pid: Long): Boolean {
    if (File.separatorChar == '/') {
        // try unix ps
        val process = Runtime.getRuntime().exec("ps -q $pid -o pid=")
        val firstOutLine = process.inputStream.bufferedReader(Charset.defaultCharset()).readLine()
        val exitCode = process.waitFor()
        if (exitCode != 0) return false
        return firstOutLine.trim() == pid.toString(10)
    }
    else
    {
        // try dos tasklist
        val process = Runtime.getRuntime().exec("""tasklist /fi "PID eq $pid" /fo list""")
        val firstOutLine = process.inputStream.bufferedReader(Charset.defaultCharset()).readLine()
        val exitCode = process.waitFor()
        return firstOutLine.trim().isNotEmpty() // TODO: actually test this on a windows machine
    }
}

private val JVM_PROCESS_ID: Long by lazy {
    val runtimeMX = ManagementFactory.getRuntimeMXBean()

    // Since Java 10, RuntimeMXBean has the getPid() method
    // try that first
    try {
        val getPIDMethod = runtimeMX.javaClass.getMethod("getPid")
        val result = getPIDMethod.invoke(runtimeMX)
        if (result is Long) {
            return@lazy result as Long
        } else if (result is java.lang.Long) {
            return@lazy result.toLong()
        }
        // else: go ahead
    }
    catch (ex: ReflectiveOperationException) { /* carry on... */ }
    catch (ex: ReflectionException) { /* carry on... */ }
    catch (ex: SecurityException) { /* carry on... */ }
    catch (ex: InvocationTargetException) { /* carry on... */ }

    // otherwise, the PID might be included in the jvm name which is,
    // at least on the HotSpot VM, likely of the format <PID>@<HOSTNAME>
    val jvmName = runtimeMX.name
    if (jvmName.matches(Regex("^\\d+(@.+)?$"))) {
        try {
            return@lazy jvmName.split('@')[0].toLong()
        }
        catch (ex: NumberFormatException) { /* carry on... */ }
    }

    // for windows, we are out of luck already
    if (File.separatorChar == '\\') {
        throw UnsupportedOperationException("Failed to determine process ID from RuntimeMXBean")
    }

    // for unix, we can try reading the $PPID variable
    val process = Runtime.getRuntime().exec("echo \$PPID")
    val pidAsString = try {
        process.inputStream.bufferedReader().readLine().trim()
    }
    finally {
        try {
            process.waitFor()
        }
        catch (ex: InterruptedException) { /* ignored */ }
        process.destroyForcibly()
    }

    return@lazy try {
        pidAsString.toLong()
    }
    catch (ex: NumberFormatException) {
        throw UnsupportedOperationException("Failed to determine process ID using `echo \$PPID`", ex)
    }
}