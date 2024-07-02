package mpp.faaqueue

import kotlinx.atomicfu.*

class FAAQueue<E> {
    private val head: AtomicRef<Segment> // Head pointer, similarly to the Michael-Scott queue (but the first node is _not_ sentinel)
    private val tail: AtomicRef<Segment> // Tail pointer, similarly to the Michael-Scott queue
    private val enqIndex = atomic(0L)
    private val deqIndex = atomic(0L)

    init {
        val firstNode = Segment(0L)
        head = atomic(firstNode)
        tail = atomic(firstNode)
    }

    /**
     * Adds the specified element [element] to the queue.
     */
    fun enqueue(element: E) {
        while (true) {
            val curTail = tail.value
            val curIndex = enqIndex.getAndIncrement()
            val newSegment = findSegment(curTail, curIndex / SEGMENT_SIZE)
            moveTailForward(newSegment)
            if (newSegment.cas((curIndex % SEGMENT_SIZE).toInt(), null, element)) {
                return
            }
        }
    }

    /**
     * Retrieves the first element from the queue and returns it;
     * returns `null` if the queue is empty.
     */
    @Suppress("UNCHECKED_CAST")
    fun dequeue(): E? {
        while (true) {
            if (isEmpty) {
                return null
            }
            val curHead = head.value
            val curIndex = deqIndex.getAndIncrement()
            val newSegment = findSegment(curHead, curIndex / SEGMENT_SIZE)
            moveHeadForward(newSegment)
            if (newSegment.cas((curIndex % SEGMENT_SIZE).toInt(), null, Any())) {
                continue
            }
            return newSegment.get((curIndex % SEGMENT_SIZE).toInt()) as E
        }
    }

    /**
     * Returns `true` if this queue is empty, or `false` otherwise.
     */
    val isEmpty: Boolean
        get() = deqIndex.value >= enqIndex.value

    private fun findSegment(start: Segment, endId: Long): Segment {
        var curId = start.id
        var curSegment = start
        var nextSegment: Segment?
        while (curId < endId) {
            nextSegment = curSegment.next.value
            curId++
            if (nextSegment == null) {
                curSegment.next.compareAndSet(null, Segment(curId))
            }
            curSegment = curSegment.next.value!!
        }
        return curSegment
    }

    private fun moveTailForward(newSegment: Segment) {
        while (newSegment.id > tail.value.id) {
            if (tail.compareAndSet(tail.value, newSegment)) {
                return
            }
        }
    }

    private fun moveHeadForward(newSegment: Segment) {
        while (newSegment.id > head.value.id) {
            if (head.compareAndSet(head.value, newSegment)) {
                return
            }
        }
    }
}

class Segment(val id: Long) {
    val next: AtomicRef<Segment?> = atomic(null)
    private val elements = atomicArrayOfNulls<Any>(SEGMENT_SIZE)

    fun get(i: Int) = elements[i].value

    fun cas(i: Int, expect: Any?, update: Any?) = elements[i].compareAndSet(expect, update)
}

const val SEGMENT_SIZE = 2 // DO NOT CHANGE, IMPORTANT FOR TESTS

