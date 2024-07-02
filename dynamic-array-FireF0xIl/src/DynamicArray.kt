package mpp.dynamicarray

import kotlinx.atomicfu.*

interface DynamicArray<E> {
    /**
     * Returns the element located in the cell [index],
     * or throws [IllegalArgumentException] if [index]
     * exceeds the [size] of this array.
     */
    fun get(index: Int): E

    /**
     * Puts the specified [element] into the cell [index],
     * or throws [IllegalArgumentException] if [index]
     * exceeds the [size] of this array.
     */
    fun put(index: Int, element: E)

    /**
     * Adds the specified [element] to this array
     * increasing its [size].
     */
    fun pushBack(element: E)

    /**
     * Returns the current size of this array,
     * it increases with [pushBack] invocations.
     */
    val size: Int
}

class DynamicArrayImpl<E> : DynamicArray<E> {
    private val core = atomic(Core<E>(INITIAL_CAPACITY, 0))

    override fun get(index: Int): E = core.value.get(index)

    override fun put(index: Int, element: E) {
        var curCore = core.value
        curCore.set(index, element)
        var nextNode = curCore.nextCore.value
        while (nextNode != null) {
            val curElement = curCore.get(index)
            if (curElement != null) {
                nextNode.set(index, curElement)
            }
            curCore = nextNode
            nextNode = curCore.nextCore.value
        }
    }

    override fun pushBack(element: E) {
        while (true) {
            val curCore = core.value
            val curSize = curCore.size
            val curCapacity = curCore.capacity
            if (curSize < curCore.capacity) {
                try {
                    if (curCore.casSet(curSize, null, element, 0)) {
                        return
                    }
                } finally {
                    curCore.casSetSize(curSize, curSize + 1)
                }
            } else {
                var nextCore = Core<E>(curCapacity * 2, curCapacity)
                try {
                    if (!curCore.nextCore.compareAndSet(null, nextCore)) {
                        nextCore = curCore.nextCore.value ?: continue
                    }
                } finally {
                    for (i in 0 until curCore.capacity) {
                        curCore.get(i)?.run { nextCore.casSet(i, null, this) }
                    }
                    core.compareAndSet(curCore, nextCore)
                }
            }
        }
    }

    override val size: Int get() = core.value.size
}

private class Core<E>(
    capacity: Int,
    initialSize: Int
) {
    private val array = atomicArrayOfNulls<E>(capacity)
    private val _size = atomic(initialSize)
    val nextCore: AtomicRef<Core<E>?> = atomic(null)

    val size: Int
        get() = _size.value

    val capacity: Int
        get() = array.size

    @Suppress("UNCHECKED_CAST")
    fun get(index: Int): E {
        require(index < size)
        return array[index].value as E
    }

    fun casSet(index: Int, expected: E?, update: E, compareTo: Int = -1): Boolean =
        require(index.compareTo(size) <= compareTo).let { array[index].compareAndSet(expected, update) }

    fun set(index: Int, element: E): E? = require(index < size).let { array[index].getAndSet(element) }

    fun casSetSize(expected: Int, update: Int): Boolean = _size.compareAndSet(expected, update)

}

private const val INITIAL_CAPACITY = 1 // DO NOT CHANGE ME