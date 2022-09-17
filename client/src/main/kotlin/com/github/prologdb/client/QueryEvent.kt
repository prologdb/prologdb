package com.github.prologdb.client

import com.github.prologdb.net.v1.messages.QueryClosedEvent
import com.github.prologdb.net.v1.messages.QueryRelatedError
import com.github.prologdb.runtime.unification.Unification

sealed class QueryEvent
data class QuerySolutionEvent(val solution: Unification): QueryEvent()
data class QueryErrorEvent(val error: QueryRelatedError): QueryEvent()
data class QueryClosedEvent(val reason: QueryClosedEvent.Reason): QueryEvent()
