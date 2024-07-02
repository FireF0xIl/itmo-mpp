package mpp.counter

import kotlinx.atomicfu.AtomicIntArray
import kotlin.random.Random

class ShardedCounter {
    private val counters = AtomicIntArray(ARRAY_SIZE)

    /**
     * Atomically increments by one the current value of the counter.
     */
    fun inc() {
        counters[Random.nextInt(ARRAY_SIZE)].getAndIncrement()
    }

    /**
     * Returns the current counter value.
     */
    fun get(): Int {
        var count = 0
        for (i in 0 until ARRAY_SIZE) {
            count += counters[i].value
        }
        return count
    }
}

private const val ARRAY_SIZE = 2 // DO NOT CHANGE ME