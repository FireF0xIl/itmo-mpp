package dijkstra

import kotlinx.atomicfu.locks.ReentrantLock
import java.util.*
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

private val NODE_DISTANCE_COMPARATOR = Comparator<Node> { o1, o2 -> Integer.compare(o1!!.distance, o2!!.distance) }

// Returns `Integer.MAX_VALUE` if a path has not been found.
fun shortestPathParallel(start: Node) {
    val workers = Runtime.getRuntime().availableProcessors()
    // The distance to the start node is `0`
    start.distance = 0
    // Create a priority (by distance) queue and add the start node into it
    val q = MultiPriorityQueue(workers, NODE_DISTANCE_COMPARATOR)
    q.add(start)
    // Run worker threads and wait until the total work is done
    val onFinish = Phaser(workers + 1) // `arrive()` should be invoked at the end by each worker
    val activeWorkerCount = AtomicInteger(1)
    repeat(workers) {
        thread {
            while (activeWorkerCount.get() > 0) {
                val cur = q.poll() ?: continue
                for (e in cur.outgoingEdges) {
                    while (true) {
                        val oldDist = e.to.distance
                        val newDist = cur.distance + e.weight
                        if (oldDist <= newDist) {
                            break
                        }
                        if (e.to.casDistance(oldDist, newDist)) {
                            q.add(e.to)
                            activeWorkerCount.incrementAndGet()
                            break
                        }
                    }
                }
                activeWorkerCount.decrementAndGet()
            }
            onFinish.arrive()
        }
    }
    onFinish.arriveAndAwaitAdvance()
}

private class MultiPriorityQueue<T>(initialCapacity: Int, comparator: Comparator<T>) {
    private val queueCount = initialCapacity * 2
    private val queueList = Collections.nCopies(queueCount, PriorityQueue(initialCapacity, comparator))
    private val lockList = Collections.nCopies(queueCount, ReentrantLock())
    private val random = Random()

    fun add(value: T) {
        while (true) {
            val curQueue = random.nextInt(queueCount)
            if (lockList[curQueue].tryLock()) {
                try {
                    queueList[curQueue].add(value)
                    return
                } finally {
                    lockList[curQueue].unlock()
                }
            }
        }
    }

    fun poll(): T? {
        while (true) {
            val q1 = random.nextInt(queueCount)
            val q2 = random.nextInt(queueCount)

            if (q1 != q2 && lockList[q1].tryLock()) {
                try {
                    if (lockList[q2].tryLock()) {
                        try {
                            val q1Value = queueList[q1].peek()
                            val q2Value = queueList[q2].peek()
                            return when {
                                q1Value == null && q2Value == null -> null
                                q1Value == null -> q2Value
                                q2Value == null -> q1Value
                                queueList[q1].comparator().compare(q1Value, q2Value) < 0 -> queueList[q1].poll()
                                else -> queueList[q2].poll()
                            }
                        } finally {
                            lockList[q2].unlock()
                        }
                    }
                } finally {
                    lockList[q1].unlock()
                }
            }
        }
    }
}
