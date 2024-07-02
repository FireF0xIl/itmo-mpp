import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.atomicArrayOfNulls
import java.util.*
import java.util.concurrent.ThreadLocalRandom

class FCPriorityQueue<E : Comparable<E>> {
    private val FC_ARRAY_SIZE = Runtime.getRuntime().availableProcessors()
    private val q = PriorityQueue<E>()
    private val fcArray = atomicArrayOfNulls<Request<E>>(FC_ARRAY_SIZE)
    private val fcLock = atomic(false)

    /**
     * Retrieves the element with the highest priority
     * and returns it as the result of this function;
     * returns `null` if the queue is empty.
     */
    fun poll(): E? = doOperation(Request { q.poll() })

    /**
     * Returns the element with the highest priority
     * or `null` if the queue is empty.
     */
    fun peek(): E? = doOperation(Request { q.peek() })

    /**
     * Adds the specified element to the queue.
     */
    fun add(element: E) {
        doOperation(Request {
            q.add(element)
            null
        })
    }

    private fun tryLock(): Boolean = fcLock.compareAndSet(expect = false, update = true)

    private fun unlock() {
        fcLock.value = false
    }

    private fun doOperation(request: Request<E>): E? {
        var curIndex = ThreadLocalRandom.current().nextInt(FC_ARRAY_SIZE)
        while (!fcArray[curIndex].compareAndSet(null, request)) {
            curIndex = (++curIndex) % FC_ARRAY_SIZE
        }
        do {
            if (tryLock()) {
                try {
                    for (i in 0 until FC_ARRAY_SIZE) {
                        val curRequest = fcArray[i].value ?: continue
                        curRequest.element = curRequest.operation()
                        curRequest.isCompleted = true
                        fcArray[i].value = null
                    }
                } finally {
                    unlock()
                    break
                }
            }
        } while (!request.isCompleted)
        return request.element
    }

    private inner class Request<E>(val operation: () -> E?) {
        var element: E? = null
        var isCompleted = false
    }
}
