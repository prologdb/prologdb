package com.github.prologdb.transactions

/**
 * Anything that supports transactions - intended to be implemented by subtypes of
 * [MutableLibraryEntryStore] and [MutableKnowledgeBases].
 *
 * @param Self the extending/implementing subtype to make the [transact] method meaningful.
 */
interface Transactional<Self> {
    /**
     * Starts a new transaction. If a transaction is already active, starts a nested transaction.
     * @throws TransactionException
     */
    fun beginTransaction()

    /**
     * Commits the current transaction. If the current transaction is a nested transaction, commits the nested
     * transaction. The "outer" transaction stays active; changes applied in the nested transactions become part
     * of the changeset of the outer transactions.
     * @throws TransactionException
     */
    fun commit()

    /**
     * Reverts all changes done in the current transaction and ends the transaction.
     * @throws TransactionException
     */
    fun rollback()

    /**
     * Executes the given action on this knowledge base within a transaction. If the action executes, commits the
     * transaction and passes the return value to the caller. If the action throws any exception,
     * rolls the transaction back and rethrows the exception.
     * @throws TransactionException
     */
    fun <R> transact(action: (Self) -> R): R {
        beginTransaction()

        val returnValue = try {
            action(this as Self)
        } catch (ex: Throwable) {
            try {
                rollback()
                throw ex
            }
            catch (nestedEx: TransactionException) {
                throw TransactionException("Exception during transaction; another exception occured during rollback: $nestedEx. The former is set as the cause of this exception.", ex)
            }
        }

        commit()
        return returnValue
    }
}