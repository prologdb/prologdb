package com.github.prologdb.net.session.handle

import com.github.prologdb.parser.lexer.Lexer
import com.github.prologdb.parser.lexer.Operator
import com.github.prologdb.parser.lexer.OperatorToken
import com.github.prologdb.parser.lexer.Token
import com.github.prologdb.parser.parser.ParseResult
import com.github.prologdb.parser.parser.PrologParser
import com.github.prologdb.parser.parser.PrologParser.Companion.STOP_AT_EOF
import com.github.prologdb.parser.sequence.TransactionalSequence
import com.github.prologdb.parser.source.SourceUnit
import com.github.prologdb.runtime.builtin.ISOOpsOperatorRegistry
import com.github.prologdb.runtime.query.Query
import com.github.prologdb.runtime.term.Term

/**
 * Delegates parsing of textual prolog to a parser; gets the current session as
 * extra information for the parsing calls which allows for context-dependent
 * parsing (different knowledge bases can have different sets of operators).
 *
 * @param SessionState The session state to be used with this delegate.
 */
interface ParserDelegate<in SessionState : Any> {
    /**
     * Parses `codeToParse` as a term.
     * @param context The context in which the parsing should happen. This must be the same value as [SessionHandle.sessionState].
     * @param origin Will be used as the origin of the code in parsing errors (and stack traces if the resulting AST is ran).
     */
    fun parseTerm(context: SessionState?, codeToParse: String, origin: SourceUnit): ParseResult<Term>

    /**
     * Parses `codeToParse` as a term.
     * @param context The context in which the parsing should happen. This must be the same value as [SessionHandle.sessionState].
     * @param origin Will be used as the origin of the code in parsing errors (and stack traces if the resulting AST is ran).
     */
    fun parseQuery(context: SessionState?, codeToParse: String, origin: SourceUnit): ParseResult<Query>
}

/**
 * To be used as a parameter to the parse methods of [PrologParser].
 */
val STOP_AT_EOF_OR_FULL_STOP: (TransactionalSequence<Token>) -> Boolean = {
    if (!it.hasNext()) true else {
        it.mark()
        val next = it.next()
        it.rollback()
        next is OperatorToken && next.operator == Operator.FULL_STOP
    }
}

/**
 * A [ParserDelegate] that is **NOT** context-dependent and provides only
 * the ISO operators (see [ISOOpsOperatorRegistry]).
 */
object IsoOpsStatelessParserDelegate : ParserDelegate<Any> {
    private val parser = PrologParser()

    override fun parseTerm(context: Any?, codeToParse: String, origin: SourceUnit): ParseResult<Term> {
        val lexer = Lexer(origin, codeToParse.iterator())
        return parser.parseTerm(lexer, ISOOpsOperatorRegistry, STOP_AT_EOF)
    }

    override fun parseQuery(context: Any?, codeToParse: String, origin: SourceUnit): ParseResult<Query> {
        val lexer = Lexer(origin, codeToParse.iterator())
        return parser.parseQuery(lexer, ISOOpsOperatorRegistry, STOP_AT_EOF_OR_FULL_STOP)
    }
}
