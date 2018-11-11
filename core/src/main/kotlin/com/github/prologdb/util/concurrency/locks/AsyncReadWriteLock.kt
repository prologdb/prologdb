package com.github.prologdb.util.concurrency.locks

interface AsyncReadWriteLock {
    val readLock: AsyncLock
    val writeLock: AsyncLock
}