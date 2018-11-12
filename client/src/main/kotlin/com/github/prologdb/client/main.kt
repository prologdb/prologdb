package com.github.prologdb.client

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

    println("Connection established to ${connection.serverVendor} server version ${connection.serverVersion.toFormattedString()}")

    Runtime.getRuntime().addShutdownHook(thread(start = false) {
        connection.close()
    })

    val frontend = CLIFrontend(connection)
    frontend.run()
}

fun SemanticVersion.toFormattedString(): String {
    val sb = StringBuilder()
    sb.append(major)
    sb.append('.')
    sb.append(minor)
    sb.append('.')
    sb.append(patch)
    sb.append('.')

    for (preReleaseLabel in preReleaseLabelsList) {
        sb.append('-')
        sb.append(preReleaseLabel)
    }

    if (buildNumber != 0L) {
        sb.append('+')
        sb.append(buildNumber)
    }

    return sb.toString()
}