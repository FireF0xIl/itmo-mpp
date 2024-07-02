package mpp.stackWithElimination

import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.atomicArrayOfNulls
import java.util.*

class TreiberStackWithElimination<E> {
    private val top = atomic<Node<E>?>(null)
    private val eliminationArray = atomicArrayOfNulls<Pair<Boolean, E?>>(ELIMINATION_ARRAY_SIZE)
    private val COMPLETED: Pair<Boolean, E?> = false to null
    private val TRY_COUNT = 5
    private val ELIMINATION_WIDTH = 4
    private val random = Random()

    /**
     * Adds the specified element [x] to the stack.
     */
    fun push(x: E) {
        val current = true to x
        var i = random.nextInt(ELIMINATION_ARRAY_SIZE)
        for (k in 0 until ELIMINATION_WIDTH) {
            if (eliminationArray[i].compareAndSet(null, current)) {
                for (j in 0 until TRY_COUNT) {
                    if (eliminationArray[i].compareAndSet(COMPLETED, null)) {
                        return
                    }
                }
                when {
                    eliminationArray[i].compareAndSet(current, null) -> break
                    eliminationArray[i].compareAndSet(COMPLETED, null) -> return
                    else -> break
                }
            }
            i = (++i) % ELIMINATION_ARRAY_SIZE
        }

        while (true) {
            val curTop = top.value
            if (top.compareAndSet(curTop, Node(x, curTop))) {
                return
            }
        }
    }

    /**
     * Retrieves the first element from the stack
     * and returns it; returns `null` if the stack
     * is empty.
     */
    fun pop(): E? {
        var i = random.nextInt(ELIMINATION_ARRAY_SIZE)
        for (k in 0 until ELIMINATION_WIDTH) {
            for (j in 0 until TRY_COUNT) {
                val current = eliminationArray[i].value ?: continue
                if (current.first && eliminationArray[i].compareAndSet(current, COMPLETED)) {
                    return current.second
                }
            }
            i = (++i) % ELIMINATION_ARRAY_SIZE
        }

        while (true) {
            val curTop = top.value ?: return null
            if (top.compareAndSet(curTop, curTop.next)) {
                return curTop.x
            }
        }
    }
}

private class Node<E>(val x: E, val next: Node<E>?)

private const val ELIMINATION_ARRAY_SIZE = 2 // DO NOT CHANGE IT