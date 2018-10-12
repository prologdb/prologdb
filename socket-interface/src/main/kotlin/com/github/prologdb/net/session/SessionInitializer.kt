package com.github.prologdb.net.session

import com.github.prologdb.net.HandshakeFailedException
import com.github.prologdb.net.negotiation.*
import com.github.prologdb.net.session.handle.SessionHandle
import com.google.protobuf.InvalidProtocolBufferException
import java.net.Socket

/**
 * Performs the handshake with a connection and returns a [SessionHandle] from that.
 */
class SessionInitializer(
    private val serverVendorName: String?,
    private val serverVersion: SemanticVersion,
    private val versionHandleFactories: Map<SemanticVersion, (Socket, ClientHello) -> SessionHandle>
) {
    private val preferredVersion: SemanticVersion = versionHandleFactories.keys
        .asSequence()
        .sortedWith(SEMANTIC_VERSION_NATURAL_ORDER)
        .lastOrNull() // alias latest
        ?: throw IllegalArgumentException("Need support for at least one protocol version.")

    /**
     * Performs the handshake with the oder side and initializes a suitable
     * [SessionHandle] instance for the negotiated parameters.
     */
    fun init(socket: Socket): SessionHandle {
        val clientHello = socket.onErrorReportAndClose {
            ToServer.parseDelimitedFrom(socket.getInputStream()).hello!!
        }

        val targetVersion: SemanticVersion? = if (clientHello.desiredProtocolVersionList.isEmpty()) {
            preferredVersion
        } else {
            clientHello.desiredProtocolVersionList
                .asSequence()
                .filter { it in versionHandleFactories.keys }
                .sortedWith(SEMANTIC_VERSION_NATURAL_ORDER)
                .lastOrNull()
        }

        return socket.onErrorReportAndClose {
            if (targetVersion == null) {
                throw HandshakeFailedException("Failed to negotiate protocol version; no common version.")
            }

            val shb = ServerHello.newBuilder()
            shb.version = serverVersion
            shb.chosenProtocolVersion = targetVersion
            shb.addAllSupportedProtocolVersions(versionHandleFactories.keys)
            if (serverVendorName != null) {
                shb.vendor = serverVendorName
            }

            val handle = versionHandleFactories[targetVersion]!!(socket, clientHello)

            ToClient.newBuilder()
                .setHello(shb.build())
                .build()
                .writeDelimitedTo(socket.getOutputStream())

            return@onErrorReportAndClose handle
        }
    }

    companion object {
        private val SEMANTIC_VERSION_NATURAL_ORDER: Comparator<SemanticVersion> = compareBy(
            SemanticVersion::getMajor,
            SemanticVersion::getMinor,
            SemanticVersion::getPatch
        )
    }

    private fun <T> Socket.onErrorReportAndClose(action: () -> T): T {
        val error: ServerError
        val originalEx: Throwable
        try {
            return action()
        }
        catch (ex: InvalidProtocolBufferException) {
            originalEx = ex
            error = ServerError.newBuilder()
                .setKind(ServerError.Kind.INVALID_WIRE_FORMAT)
                .setMessage(ex.message)
                .build()
        }
        catch (ex: Throwable) {
            originalEx = ex
            error = ServerError.newBuilder()
                .setKind(ServerError.Kind.GENERIC)
                .setMessage(ex.message)
                .build()
        }

        try {
            ToClient.newBuilder()
                .setError(error)
                .build()
                .writeDelimitedTo(this.getOutputStream())
        }
        catch (errorResponseEx: Throwable) {
            originalEx.addSuppressed(errorResponseEx)
        }

        try {
            this.close()
        }
        catch (closeEx: Throwable) {
            originalEx.addSuppressed(closeEx)
        }

        throw originalEx
    }
}