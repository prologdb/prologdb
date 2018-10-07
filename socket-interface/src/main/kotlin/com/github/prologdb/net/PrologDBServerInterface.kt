package com.github.prologdb.net

import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.LinkedBlockingQueue

/**
 * Opens a new interface on the given [ServerSocket]
 */
class PrologDBServerInterface(
    private val serverSocket: ServerSocket
) {
    /**
     * When new connections come in, they are placed on this queue for a ??? to pick them up.
     */
    private val newConnections = LinkedBlockingQueue<Socket>()

    /**
     * A reference to the thread that accepts new connections and places them onto the [newConnections] queue.
     */
    private lateinit var accepterThread: Thread

    /**
     * Whether this interface is actively accepting connections
     */
    val isActive: Boolean
        get() = ::accepterThread.isInitialized && accepterThread.isAlive

    /**
     * Set to true once
     */
    private var isClosed = false

    private val startupMutex = Any()
    fun startAcceptingClients(withThisThread: Boolean = false) {
        synchronized(startupMutex) {
            if (isActive) {
                throw IllegalStateException("Already listening for connections.")
            }

            if (withThisThread) {
                accepterThread = Thread.currentThread()
                accepterLoop()
            } else {
                accepterThread = Thread(this::accepterLoop, "prologdb-${serverSocket.localPort}-connection-accept")
                accepterThread.start()
            }
        }
    }

    fun close() {
        if (isActive) {
            if (!serverSocket.isClosed) serverSocket.close()
        }
    }

    /**
     * [ServerSocket.accept]s new connections and puts them onto [newConnections].
     * Runs until the server socket is closed.
     */
    private fun accepterLoop() {
        while (true) {
            newConnections.put(serverSocket.accept())
        }
    }
}