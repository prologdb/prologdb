package com.github.prologdb.net.session

import com.github.prologdb.net.HandshakeFailedException
import com.github.prologdb.net.async.readSingleDelimited
import com.github.prologdb.net.async.writeDelimitedTo
import com.github.prologdb.net.negotiation.*
import com.github.prologdb.net.negotiation.ToClient
import com.github.prologdb.net.negotiation.ToServer
import com.github.prologdb.net.session.handle.SessionHandle
import com.google.protobuf.InvalidProtocolBufferException
import io.reactivex.Single
import io.reactivex.subjects.SingleSubject
import java.nio.channels.AsynchronousByteChannel

/**
 * Performs the handshake with a connection and returns a [SessionHandle] from that.
 */
class SessionInitializer(
    private val serverVendorName: String?,
    private val serverVersion: SemanticVersion,
    private val versionHandleFactories: Map<SemanticVersion, (AsynchronousByteChannel, ClientHello) -> Single<SessionHandle>>
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
    fun init(channel: AsynchronousByteChannel): Single<SessionHandle> {
        return channel.readSingleDelimited(ToServer::class.java).flatMap { clientHelloEnvelope ->
            val clientHello = clientHelloEnvelope.hello!!

            val targetVersion: SemanticVersion? = if (clientHello.desiredProtocolVersionList.isEmpty()) {
                preferredVersion
            } else {
                clientHello.desiredProtocolVersionList
                    .asSequence()
                    .filter { it in versionHandleFactories.keys }
                    .sortedWith(SEMANTIC_VERSION_NATURAL_ORDER)
                    .lastOrNull()
            }

            targetVersion ?: throw HandshakeFailedException("Failed to negotiate protocol version; no common version.")

            val shb = ServerHello.newBuilder()
            shb.version = serverVersion
            shb.chosenProtocolVersion = targetVersion
            shb.addAllSupportedProtocolVersions(versionHandleFactories.keys)
            if (serverVendorName != null) {
                shb.vendor = serverVendorName
            }

            val onHelloSent = SingleSubject.create<Unit>()
            com.github.prologdb.net.negotiation.ToClient.newBuilder()
                    .setHello(shb.build())
                    .build()
                    .writeDelimitedTo(channel, onHelloSent)

            return@flatMap onHelloSent
                .flatMap { versionHandleFactories[targetVersion]!!(channel, clientHello) }
        }
        .doOnError { ex ->
            val error = when (ex) {
                is InvalidProtocolBufferException ->
                    ServerError.newBuilder()
                        .setKind(ServerError.Kind.INVALID_WIRE_FORMAT)
                        .setMessage(ex.message ?: "Failed to parse protobuf message")
                        .build()
                else ->
                    ServerError.newBuilder()
                        .setKind(ServerError.Kind.GENERIC)
                        .setMessage(ex.message ?: "Unknown error")
                        .build()
            }

            // TODO: log properly
            ex.printStackTrace(System.err)

            ToClient.newBuilder()
                .setError(error)
                .build()
                .writeDelimitedTo(channel)
        }
    }

    companion object {
        private val SEMANTIC_VERSION_NATURAL_ORDER: Comparator<SemanticVersion> = compareBy(
            SemanticVersion::getMajor,
            SemanticVersion::getMinor,
            SemanticVersion::getPatch
            // TODO: prerelease labels lexicographically
        )
    }
}