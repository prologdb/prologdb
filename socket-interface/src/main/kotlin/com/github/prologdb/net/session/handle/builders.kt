package com.github.prologdb.net.session.handle

import com.github.prologdb.io.binaryprolog.BinaryPrologReader
import com.github.prologdb.io.binaryprolog.BinaryPrologWriter
import com.github.prologdb.net.negotiation.ClientHello
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import io.reactivex.Single
import io.reactivex.subjects.SingleSubject
import java.nio.channels.AsynchronousByteChannel

fun buildProtocolVersion1SessionHandleFactory(
    parser: PrologParser = PrologParser(),
    binaryReader: BinaryPrologReader = BinaryPrologReader.getDefaultInstance(),
    binaryWriter: BinaryPrologWriter = BinaryPrologWriter.getDefaultInstance()
): (AsynchronousByteChannel, ClientHello) -> Single<SessionHandle> = { channel, _ ->
    val source = SingleSubject.create<SessionHandle>()
    source.onSuccess(ProtocolVersion1SessionHandle(
        channel,
        ProtocolVersion1PrologReader(
            parser,
            binaryReader,
            ISOOpsOperatorRegistry
        ),
        ProtocolVersion1PrologWriter(
            binaryWriter
        )
    ))

    source
}