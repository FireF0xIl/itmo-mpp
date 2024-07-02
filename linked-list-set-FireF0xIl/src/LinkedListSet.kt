package mpp.linkedlistset

import kotlinx.atomicfu.*

class LinkedListSet<E : Comparable<E>> {
    private val first = Node<E>(prev = null, element = null, next = null)
    private val last = Node<E>(prev = first, element = null, next = null)
    init {
        first.setNext(last)
        last.setPrev(first)
    }

    /**
     * Adds the specified element to this set
     * if it is not already present.
     *
     * Returns `true` if this set did not
     * already contain the specified element.
     */
    fun add(element: E): Boolean {
        while (true) {
            val window = findWindow(element)
            if (window.next?.next == null || window.next?.element != element) {
                if (window.cur.casNext(window.next, Node(window.cur, element, window.next))) {
                    return true
                }
            } else {
                return false
            }
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
        val window = findWindow(element)
        return if (window.next?.next != null && window.next?.element == element) {
            window.next!!.casIsDeleted(expected = false, update = true)
        } else {
            false
        }
    }

    /**
     * Returns `true` if this set contains
     * the specified element.
     */
    fun contains(element: E): Boolean {
        val window = findWindow(element)
        return window.next?.next != null && window.next?.element == element
    }

    private data class Window<E : Comparable<E>>(var cur: Node<E>, var next: Node<E>?)

    private fun findWindow(element: E) : Window<E> {
        while (true) {
            val window = Window(first, first.next)
            while (window.next?.next != null && window.next?.element!! < element || window.next?.isDeleted == true) {
                when {
                    window.cur.isDeleted -> break
                    window.next?.isDeleted == true && !window.cur.next?.casNext(window.next, window.next!!.next)!! -> {
                            window.next = window.cur.next
                            continue
                    }
                    else -> {
                        window.cur = window.next!!
                        window.next = window.cur.next
                    }
                }
            }
            if (window.cur.isDeleted) {
                continue
            }
            return window
        }
    }
}

@Suppress("UNUSED")
private class Node<E : Comparable<E>>(prev: Node<E>?, element: E?, next: Node<E>?) {
    private val _element = element // `null` for the first and the last nodes
    val element get() = _element!!

    private val _prev = atomic(prev)
    val prev get() = _prev.value
    fun setPrev(value: Node<E>?) {
        _prev.value = value
    }
    fun casPrev(expected: Node<E>?, update: Node<E>?) =
        _prev.compareAndSet(expected, update)

    private val _next = atomic(next)
    val next get() = _next.value
    fun setNext(value: Node<E>?) {
        _next.value = value
    }
    fun casNext(expected: Node<E>?, update: Node<E>?) =
        _next.compareAndSet(expected, update)

    private val _isDeleted = atomic(false)
    val isDeleted get() = _isDeleted.value
    fun casIsDeleted(expected: Boolean, update: Boolean) =
        _isDeleted.compareAndSet(expected, update)
}