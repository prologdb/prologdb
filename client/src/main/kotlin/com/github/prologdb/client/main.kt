package com.github.prologdb.client

import com.github.prologdb.net.async.format
import com.github.prologdb.net.negotiation.SemanticVersion
import com.tmarsteel.jcli.Environment
import com.tmarsteel.jcli.Input
import kotlin.concurrent.thread

fun main(args: Array<String>) {
    val input = Input(Environment.UNIX, args)
    val hostname = input.options()["host"]?.first() ?: "localhost"
    val port = input.options()["port"]?.first()?.toInt() ?: 30001

    println("Connecting to prologdb server on $hostname:$port...")

    val connection = Connection(hostname, port)

    println("Connection established to ${connection.serverVendor} server version ${connection.serverVersion.format()}")

    Runtime.getRuntime().addShutdownHook(thread(start = false) {
        println("\nClosing connection")
        connection.close()
    })

    val frontend = CLIFrontend(connection)
    frontend.run()
}