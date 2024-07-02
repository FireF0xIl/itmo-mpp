import kotlinx.atomicfu.AtomicIntArray
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.loop

/**
 * Int-to-Int hash map with open addressing and linear probes.
 *
 */
class IntIntHashMap {
    private val core = atomic(Core(INITIAL_CAPACITY))

    /**
     * Returns value for the corresponding key or zero if this key is not present.
     *
     * @param key a positive key.
     * @return value for the corresponding or zero if this key is not present.
     * @throws IllegalArgumentException if key is not positive.
     */
    operator fun get(key: Int): Int {
        require(key > 0) { "Key must be positive: $key" }
        return toValue(core.value.getInternal(key))
    }

    /**
     * Changes value for the corresponding key and returns old value or zero if key was not present.
     *
     * @param key   a positive key.
     * @param value a positive value.
     * @return old value or zero if this key was not present.
     * @throws IllegalArgumentException if key or value are not positive, or value is equal to
     * [Integer.MAX_VALUE] which is reserved.
     */
    fun put(key: Int, value: Int): Int {
        require(key > 0) { "Key must be positive: $key" }
        require(isValue(value)) { "Invalid value: $value" }
        return toValue(putAndRehashWhileNeeded(key, value))
    }

    /**
     * Removes value for the corresponding key and returns old value or zero if key was not present.
     *
     * @param key a positive key.
     * @return old value or zero if this key was not present.
     * @throws IllegalArgumentException if key is not positive.
     */
    fun remove(key: Int): Int {
        require(key > 0) { "Key must be positive: $key" }
        return toValue(putAndRehashWhileNeeded(key, DEL_VALUE))
    }

    private fun putAndRehashWhileNeeded(key: Int, value: Int): Int {
        while (true) {
            val curCore = core.value
            val oldValue = curCore.putInternal(key, value)
            if (oldValue != NEEDS_REHASH) return oldValue
            core.compareAndSet(curCore, curCore.rehash())
        }
    }

    private class Core(capacity: Int) {
        // Pairs of <key, value> here, the actual
        // size of the map is twice as big.
        val map = AtomicIntArray(2 * capacity)
        val shift: Int
        val next = atomic(this)

        init {
            val mask = capacity - 1
            assert(mask > 0 && mask and capacity == 0) { "Capacity must be power of 2: $capacity" }
            shift = 32 - Integer.bitCount(mask)
        }

        fun getInternal(key: Int): Int {
            var index = index(key)
            var probes = 0
            while (map[index].value != key) { // optimize for successful lookup
                if (map[index].value == NULL_KEY) return NULL_VALUE // not found -- no value
                if (++probes >= MAX_PROBES) return NULL_VALUE
                if (index == 0) index = map.size
                index -= 2
            }
            // found key -- return value
            val curValue = map[index + 1].value
            return when {
                curValue == MOVE_VALUE -> next.value.getInternal(key)
                isMoved(curValue) -> -curValue
                else -> curValue
            }
        }

        fun putInternal(key: Int, value: Int): Int {
            var index = index(key)
            var probes = 0
            while (map[index].value != key) { // optimize for successful lookup
                if ((map[index].value == NULL_KEY && map[index].compareAndSet(NULL_KEY, key)) || map[index].value == key) {
                    break
                }
                if (++probes >= MAX_PROBES) return NEEDS_REHASH
                if (index == 0) index = map.size
                index -= 2
            }
            map[index + 1].loop { oldValue ->
                when {
                    oldValue == MOVE_VALUE -> return next.value.putInternal(key, value)
                    isMoved(oldValue) -> next.value.moveValue(key, -oldValue)
                                             .also { map[index + 1].compareAndSet(oldValue, MOVE_VALUE) }
                    // found key -- update value
                    map[index + 1].compareAndSet(oldValue, value) -> return oldValue
                }
            }
        }

        fun rehash(): Core {
            if (next.value === this) {
                next.compareAndSet(this, Core(map.size)) // map.length is twice the current capacity
            }
            var index = 0
            while (index < map.size) {
                while (true) {
                    val oldValue = map[index + 1].value
                    when {
                        oldValue == MOVE_VALUE -> break
                        oldValue == NULL_VALUE -> if (map[index + 1].compareAndSet(NULL_VALUE, MOVE_VALUE)) break
                        isMoved(oldValue) -> next.value.moveValue(map[index].value, -oldValue)
                                                 .also { map[index + 1].compareAndSet(oldValue, MOVE_VALUE) }
                        else -> map[index + 1].compareAndSet(oldValue, -oldValue)
                    }
                }
                index += 2
            }
            return next.value
        }

        /**
         * Returns an initial index in map to look for a given key.
         */
        fun index(key: Int): Int = (key * MAGIC ushr shift) * 2

        private fun moveValue(key: Int, value: Int) {
            var mapIndex = index(key)
            while (true) {
                map[mapIndex].compareAndSet(NULL_KEY, key)
                if (map[mapIndex].value == key) {
                    map[mapIndex + 1].compareAndSet(NULL_VALUE, value)
                    break
                } else {
                    mapIndex = (if (mapIndex == 0) map.size else mapIndex) - 2
                }
            }
        }
    }
}

private const val MAGIC = -0x61c88647 // golden ratio
private const val INITIAL_CAPACITY = 2 // !!! DO NOT CHANGE INITIAL CAPACITY !!!
private const val MAX_PROBES = 8 // max number of probes to find an item
private const val NULL_KEY = 0 // missing key (initial value)
private const val NULL_VALUE = 0 // missing value (initial value)
private const val DEL_VALUE = Int.MAX_VALUE // mark for removed value
private const val NEEDS_REHASH = -1 // returned by `putInternal` to indicate that rehash is needed
private const val MOVE_VALUE = Int.MIN_VALUE // mark for moved value

// Checks is the value is in the range of allowed values
private fun isValue(value: Int): Boolean = value in (1 until DEL_VALUE)

// Converts internal value to the public results of the methods
private fun toValue(value: Int): Int = if (isValue(value)) value else 0

private fun isMoved(value: Int): Boolean = value < 0 && value != MOVE_VALUE
