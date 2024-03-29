package com.github.prologdb.orchestration.bootstrap

import com.github.prologdb.net.async.format
import com.github.prologdb.orchestration.config.ObjectPathException
import com.github.prologdb.orchestration.config.OverrideException
import com.github.prologdb.orchestration.config.validation.ValidationException
import com.github.prologdb.orchestration.config.validation.logViolationsError
import com.github.prologdb.orchestration.config.validation.refuseInvalid
import com.github.prologdb.orchestration.introspect.SERVER_VERSION
import org.slf4j.LoggerFactory
import java.io.IOException
import kotlin.concurrent.thread

private val log = LoggerFactory.getLogger("prologdb.cli")

private fun printUsage() {
    println("""
        Usage:
        prologdb-server [<path to config file>|--] [<<config parameter name>=<override value>>...]

        Examples:

        prologdb-server - port=42621

        prologdb-server /path/to/alternative/configuration.yml
    """.trimIndent())
}

fun main(args: Array<String>) {
    log.info("PrologDB Server Version ${SERVER_VERSION.format()}")

    val input = try {
        refuseInvalid(parseCLI(args))
    }
    catch (ex: ObjectPathException) {
        log.error("Failed to parse overrides: ${ex.message}", ex)
        System.exit(1)
        return
    }
    catch (ex: OverrideException) {
        log.error("Failed to parse overrides: ${ex.message}", ex)
        System.exit(1)
        return
    }
    catch (ex: ValidationException) {
        log.logViolationsError(ex, "Invalid command line input")

        printUsage()

        System.exit(1)
        return
    }

    val configuration = try {
        refuseInvalid(input.buildConfig())
    }
    catch (ex: IOException) {
        log.error("Failed to read configuration: ${ex.message}", ex)
        System.exit(-1)
        return
    }
    catch (ex: ValidationException) {
        log.logViolationsError(ex, "Configuration (after applying overrides) is not valid")
        System.exit(-1)
        return
    }

    val handle = runServer(configuration)
    Runtime.getRuntime().addShutdownHook(thread(start = false, name = "prologdb-shutdown") {
        log.info("Shutting server down; waiting for all connections to close.")
        handle.shutdown(ShutdownReason.UNKNOWN)
        log.info("Shutdown complete.")
    })
}