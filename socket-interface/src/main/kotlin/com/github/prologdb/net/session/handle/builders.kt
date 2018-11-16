package com.github.prologdb.net.session.handle

import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.io.binaryprolog.BinaryPrologWriter
import com.github.prologdb.net.negotiation.ClientHello
import io.reactivex.Single
import io.reactivex.subjects.SingleSubject
import java.nio.channels.AsynchronousByteChannel
import java.nio.channels.AsynchronousSocketChannel
import java.util.*

fun buildProtocolVersion1SessionHandleFactory(
    parser: ParserDelegate<*> = IsoOpsStatelessParserDelegate,
    binaryReader: BinaryPrologReader = BinaryPrologReader.getDefaultInstance(),
    binaryWriter: BinaryPrologWriter = BinaryPrologWriter.getDefaultInstance()
): (AsynchronousByteChannel, ClientHello) -> Single<SessionHandle> = { channel, _ ->
    val id = if (channel is AsynchronousSocketChannel) {
        "${channel.remoteAddress}-${channel.localAddress}"
    } else {
        UUID.randomUUID().toString()
    }

    val source = SingleSubject.create<SessionHandle>()
    source.onSuccess(ProtocolVersion1SessionHandle(
        id,
        channel,
        parser,
        binaryReader,
        binaryWriter
    ))

    source
}