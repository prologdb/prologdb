package com.github.prologdb.orchestration.bootstrap

import com.github.prologdb.orchestration.config.ServerConf

/**
 * Starts the server. Keeps the server open
 */
fun runServer(config: ServerConf): ServerHandle {
    TODO()
}

/**
 * A handle to a running server.
 */
class ServerHandle {
    /**
     * Shuts the server down for the given reason. Blocks
     * until all client connections have been closed and all
     * resources have been released.
     */
    fun shutdown(reason: ShutdownReason) {
        TODO()
    }
}

enum class ShutdownReason {
    /**
     * Server shuts down in response to a command issued by an administrator
     * (e.g. SIGTERM).
     */
    UNKNOWN,

    /**
     * Server shuts down for maintenance
     */
    MAINTENANCE
}