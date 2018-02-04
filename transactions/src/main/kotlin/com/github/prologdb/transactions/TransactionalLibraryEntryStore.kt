package com.github.prologdb.transactions

import com.github.prologdb.runtime.knowledge.library.MutableLibraryEntryStore

interface TransactionalLibraryEntryStore : Transactional<MutableLibraryEntryStore>, MutableLibraryEntryStore {

}