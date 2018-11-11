package com.github.prologdb.storage

/**
 * Marks a storage engine as being persistent (non-valiatile,
 * on disk, across restarts & reboots)
 */
annotation class PersistentStorage

/**
 * Marks a storage engine as volatile (does not persist across
 * restarts of the database program or the hosting machine)
 */
annotation class VolatileStorage

/**
 * Marks a storage engine as having a caching/speedup mechanism
 * that is considerably faster than the engines main means of
 * storage. This also includes 100% in-memory stores.
 */
annotation class AcceleratedStorage