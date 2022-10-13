package com.github.prologdb.orchestration.bootstrap

import com.github.prologdb.dbms.PrologDatabase
import com.github.prologdb.execplan.planner.NaiveExecutionPlanner
import com.github.prologdb.indexing.DefaultFactIndexLoader
import com.github.prologdb.indexing.FactIndexLoader
import com.github.prologdb.net.ServerInterface
import com.github.prologdb.net.negotiation.SemanticVersion
import com.github.prologdb.net.session.SessionInitializer
import com.github.prologdb.net.session.handle.buildProtocolVersion1SessionHandleFactory
import com.github.prologdb.orchestration.config.ServerConf
import com.github.prologdb.orchestration.engine.PrologDatabaseToNetworkAdapter
import com.github.prologdb.orchestration.introspect.SERVER_VERSION
import com.github.prologdb.storage.fact.DefaultFactStoreLoader
import com.github.prologdb.storage.fact.FactStoreLoader
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("prologdb.bootstrap")

/**
 * Starts the server. Keeps the server open
 */
fun runServer(config: ServerConf): ServerHandle {
    log.trace("Starting server with config {}", config)

    val engine = PrologDatabaseToNetworkAdapter(PrologDatabase(
        config.dataDirectory!!,
        FactStoreLoader,
        FactIndexLoader,
        NaiveExecutionPlanner()
    ))
    log.info("database loaded")

    log.info("Starting network interface on port ${config.network.port}")
    val networkIFace = ServerInterface(
        engine,
        SessionInitializer(
            serverVendorName = "prologdb",
            serverVersion = SERVER_VERSION,
            versionHandleFactories = mapOf(
                PROTOCOL_VERSION1_SEMVER to buildProtocolVersion1SessionHandleFactory(parser = engine)
            )
        ),
        config.network.port,
        { 2 }
    )
    log.info("Network interface started, now accepting connections.")

    return ServerHandle(networkIFace, engine)
}

private val PROTOCOL_VERSION1_SEMVER = SemanticVersion.newBuilder()
    .setMajor(1)
    .setMinor(0)
    .setPatch(0)
    .build()

/**
 * A handle to a running server.
 */
class ServerHandle(
    private val networkIFace: ServerInterface<*>,
    private val engine: PrologDatabaseToNetworkAdapter
) {
    /**
     * Shuts the server down for the given reason. Blocks
     * until all client connections have been closed and all
     * resources have been released.
     */
    fun shutdown(reason: ShutdownReason) {
        networkIFace.close()
        engine.close()
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

private val FactStoreLoader: FactStoreLoader = DefaultFactStoreLoader.withServiceLoaderImplementations()
private val FactIndexLoader: FactIndexLoader = DefaultFactIndexLoader.withServiceLoaderImplementations()