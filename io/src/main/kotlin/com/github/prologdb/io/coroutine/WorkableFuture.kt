package com.github.prologdb.io.coroutine

import java.util.concurrent.CancellationException
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import kotlin.coroutines.experimental.*

/**
 * Resembles the work needed to compute a result which is split into multiple
 * steps each separated by a coroutine suspension. The [step] function can be
 * called repeatedly until a result is present or a error condition arises.
 */
interface WorkableFuture<T> : Future<T> {
    /**
     * Performs CPU&memory bound work on this task returns when waiting for
     * I/O bound tasks (disk, memory, ...).
     *
     * If the result is already present, does nothing and returns `true`.
     *
     * @return whether the result is available after this call
     */
    fun step(): Boolean
}

@RestrictsSuspension
interface WorkableFutureBuilder {
    /**
     * Suspends this coroutine until the given future is present.
     * @return the futures value
     * @throws Exception Forwarded from the future, including [CancellationException]
     */
    suspend fun <E> waitFor(future: Future<E>): E
}

internal class WorkableFutureImpl<T>(code: suspend WorkableFutureBuilder.() -> T) : WorkableFutureBuilder, WorkableFuture<T> {

    private val onComplete = object : Continuation<T> {
        override val context: CoroutineContext = EmptyCoroutineContext

        override fun resume(value: T) {
            synchronized(mutex) {
                if (state == State.RUNNING) {
                    result = value!!
                    state = State.COMPLETED
                }
                else {
                    throw IllegalStateException("Future is in state $state, cannot complete")
                }
            }
        }

        override fun resumeWithException(exception: Throwable) {
            synchronized(mutex) {
                if (state == State.RUNNING) {
                    error = exception
                    state = State.COMPLETED
                }
                else {
                    throw IllegalStateException("Future is in state $state, cannot complete exceptionally")
                }
            }
        }
    }

    private val mutex = Any()

    @Volatile
    private var continuation: Continuation<Unit> = code.createCoroutine(this, onComplete)

    @Volatile
    private var result: T? = null
    @Volatile
    private var error: Throwable? = null

    @Volatile
    private var state: State = State.RUNNING

    @Volatile
    private var currentWaitingFuture: Future<*>? = null

    override fun isDone(): Boolean = state == State.COMPLETED

    override fun isCancelled(): Boolean = state == State.CANCELLED

    override suspend fun <E> waitFor(future: Future<E>): E {
        synchronized(mutex) {
            if (state == State.CANCELLED) {
                // this has been cancelled, shut it down right here
                suspendCoroutine<Unit> { /* not picking up the continuation aborts the coroutine. */ }
                throw Exception("This should never have been thrown")
            }

            if (state != State.RUNNING) {
                throw IllegalStateException("Future is in state $state, cannot wait for a future.")
            }

            currentWaitingFuture = future
            state = State.WAITING_ON_FUTURE
        }

        suspendCoroutine<Any> { continuation = it }
        return try {
            future.get()
        }
        catch (ex: ExecutionException) {
            throw ex.cause ?: ex
        }
    }

    override fun step(): Boolean {
        synchronized(mutex) {
            when (state) {
                State.RUNNING -> continuation.resume(Unit)
                State.WAITING_ON_FUTURE -> {
                    val future = currentWaitingFuture!!
                    if (future.isDone || future.isCancelled) {
                        state = State.RUNNING
                        currentWaitingFuture = null
                        continuation.resume(Unit)
                    }
                }
                State.COMPLETED, State.CANCELLED -> {}
            }
        }

        return isDone
    }

    override fun get(): T {
        return when(state) {
            State.COMPLETED -> result ?: throw error!!
            State.CANCELLED -> throw error as CancellationException
            else -> throw IllegalStateException("Future not completed yet.")
        }
    }

    override fun get(timeout: Long, unit: TimeUnit?): T {
        TODO("Implement only when needed.")
    }

    override fun cancel(mayInterruptIfRunning: Boolean): Boolean {
        synchronized(state) {
            when (state) {
                State.RUNNING, State.WAITING_ON_FUTURE -> {
                    state = State.CANCELLED
                    error = CancellationException()
                    currentWaitingFuture = null
                    return true
                }
                State.COMPLETED, State.CANCELLED -> {
                    return false
                }
            }
        }
    }

    private enum class State {
        RUNNING,
        COMPLETED,
        CANCELLED,
        WAITING_ON_FUTURE
    }
}

fun <T> buildWorkableFuture(code: suspend WorkableFutureBuilder.() -> T): WorkableFuture<T> = WorkableFutureImpl(code)