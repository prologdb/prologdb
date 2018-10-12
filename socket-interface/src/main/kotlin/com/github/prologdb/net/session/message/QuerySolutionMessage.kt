package com.github.prologdb.net.session.message

import com.github.prologdb.runtime.unification.Unification

data class QuerySolutionMessage(
    val queryId: Int,

    val solution: Unification
)