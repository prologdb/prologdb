package com.github.prologdb.indexing

import com.github.prologdb.runtime.unification.Unification
import com.github.prologdb.storage.fact.PersistenceID

data class IndexEntry(val persistenceId: PersistenceID, val data: Unification)