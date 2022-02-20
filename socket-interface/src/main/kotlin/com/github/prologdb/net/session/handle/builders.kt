package com.github.prologdb.net.session.handle

import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.io.binaryprolog.BinaryPrologWriter
import com.github.prologdb.net.negotiation.ClientHello
import java.nio.channels.AsynchronousByteChannel
import java.nio.channels.AsynchronousSocketChannel
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

fun buildProtocolVersion1SessionHandleFactory(
    parser: ParserDelegate<*> = IsoOpsStatelessParserDelegate,
    binaryReader: BinaryPrologReader = BinaryPrologReader.getDefaultInstance(),
    binaryWriter: BinaryPrologWriter = BinaryPrologWriter.getDefaultInstance()
): (AsynchronousByteChannel, ClientHello) -> CompletionStage<SessionHandle<*>> = { channel, _ ->
    val id = if (channel is AsynchronousSocketChannel) {
        "${channel.remoteAddress}-${channel.localAddress}"
    } else {
        UUID.randomUUID().toString()
    }

    val source = CompletableFuture<SessionHandle<*>>()
    source.complete(ProtocolVersion1SessionHandle(
        id,
        channel,
        parser,
        binaryReader,
        binaryWriter
    ))

    source
}