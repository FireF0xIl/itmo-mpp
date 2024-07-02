package mpp.skiplist

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicMarkableReference

private const val MAX_LEVEL = 30
private val random = ThreadLocalRandom.current()

class SkipList<E : Comparable<E>> {
    private val first = Node<E>(element = null, MAX_LEVEL + 1)
    private val last  = Node<E>(element = null, MAX_LEVEL + 1)

    init {
        for (i in 0 .. MAX_LEVEL) {
            first.setNext(i, last)
        }
    }

    /**
     * Adds the specified element to this set
     * if it is not already present.
     *
     * Returns `true` if this set did not
     * already contain the specified element.
     */
    fun add(element: E): Boolean {
        val topLevel = getLevel()
        val newNode = Node(element, topLevel)

        while (true) {
            var window = findWindow(element)

            if (window.found()) {
                return false
            }
            var cur = window.cur[0]!!
            var next = window.next[0]!!
            for (level in 0..topLevel) {
                newNode.setNext(level, window.next[level])
            }
            if (!cur.casNext(0, next, newNode)) {
                continue
            }
            for (level in 1..topLevel) {
                while (true) {
                    cur = window.cur[level]!!
                    next = window.next[level]!!
                    if (cur.casNext(level, next, newNode)) {
                        break
                    }
                    window = findWindow(element)
                }
            }
            return true
        }
    }

    /**
     * Removes the specified element from this set
     * if it is present.
     *
     * Returns `true` if this set contained
     * the specified element.
     */
    fun remove(element: E): Boolean {
        while (true) {
            val window = findWindow(element)
            if (!window.found()) {
                return false
            }

            val nodeToRemove = window.next[0]!!
            val marked = BooleanArray(1)
            for (level in nodeToRemove.level downTo 1) {
                var next = nodeToRemove.getNextWithMark(level, marked)
                while (!marked[0]) {
                    nodeToRemove.casNextMark(level, next, true)
                    next = nodeToRemove.getNextWithMark(level, marked)
                }
            }

            marked[0] = false
            var next = nodeToRemove.getNextWithMark(0, marked)
            while (true) {
                val isLocalMark = nodeToRemove.casNext(0, next, next, expectedMark = false, updateMark = true)
                next = window.next[0]!!.getNextWithMark(0, marked)
                if (isLocalMark) {
                    findWindow(element)
                    return true
                } else if (marked[0]) {
                    return false
                }
            }
        }
    }

    /**
     * Returns `true` if this set contains
     * the specified element.
     */
    fun contains(element: E): Boolean {
        val window = findWindow(element)
        return window.found() && window.next[window.foundLevel]!!.element == element
    }

    private class Window<E : Comparable<E>> {
        val cur = arrayOfNulls<Node<E>>(MAX_LEVEL + 1)
        val next = arrayOfNulls<Node<E>>(MAX_LEVEL + 1)
        var foundLevel = Int.MIN_VALUE

        fun found(): Boolean = foundLevel != Int.MIN_VALUE
    }

    private fun findWindow(element: E) : Window<E> {
        val window = Window<E>()
        val marked = BooleanArray(1)

        retry@ while (true) {
            var cur = first
            for (level in MAX_LEVEL downTo 0) {
                var next = cur.getNext(level)
                while (next !== last) {
                    var newNext = next.getNextWithMark(level, marked)
                    while (marked[0]) {
                        if (!cur.casNext(level, next, newNext)) {
                            continue@retry
                        }
                        next = cur.getNext(level)
                        newNext = next.getNextWithMark(level, marked)
                    }
                    if (next !== last && next.element < element) {
                        cur = next
                        next = newNext
                    } else {
                        break
                    }
                }
                if (next != last && !window.found() && element == next.element) {
                    window.foundLevel = level
                }
                window.cur[level] = cur
                window.next[level] = next
            }
            return window
        }
    }

    private fun getLevel(): Int {
        for (i in 0..MAX_LEVEL) {
            if (random.nextInt(2) < 1) {
                return i
            }
        }
        return MAX_LEVEL
    }
}

private class Node<E : Comparable<E>>(element: E?, level: Int) {
    private val _element = element // `null` for the first and the last nodes
    val element get() = _element!!

    private val _next = Array<AtomicMarkableReference<Node<E>>>(level + 1) { AtomicMarkableReference(null, false) }
    fun getNext(index: Int): Node<E> = _next[index].reference
    fun getNextWithMark(index: Int, marked: BooleanArray): Node<E> = _next[index][marked]

    fun setNext(index: Int, value: Node<E>?) {
        _next[index].set(value, false)
    }
    fun casNext(index: Int, expectedReference: Node<E>?, updateReference: Node<E>?,
                expectedMark: Boolean = false, updateMark: Boolean = false) =
        _next[index].compareAndSet(expectedReference, updateReference, expectedMark, updateMark)
    fun casNextMark(index: Int, expectedReference: Node<E>, updateMark: Boolean) =
        _next[index].attemptMark(expectedReference, updateMark)

    private val _level = level
    val level get() = _level
}
