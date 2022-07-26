package com.github.prologdb.orchestration

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.pattern.CompositeConverter

class StripPrefixLogLayoutConverter : CompositeConverter<ILoggingEvent>() {
    override fun convert(event: ILoggingEvent): String {
        val subConverter = childConverter ?: return "<missing argument 1>"
        val prefixConverter = subConverter.next ?: return "<missing argument 2>"

        val prefix = prefixConverter.convert(event).trim()
        val sub = subConverter.convert(event)
        return if (sub.startsWith(prefix)) {
            sub.substring(prefix.length)
        } else {
            sub
        }
    }

    override fun transform(p0: ILoggingEvent?, p1: String?) = error("not implemented. Call convert(ILoggingEvent).")
}