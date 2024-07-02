import java.util.concurrent.atomic.*

class Solution(private val env: Environment) : Lock<Solution.Node> {
    private val tail = AtomicReference<Node>()

    override fun lock(): Node {
        val my = Node()
        val previous = tail.getAndSet(my)
        if (previous != null) {
            previous.next.value = my
            while (my.locked.value) {
                env.park()
            }
        }
        return my
    }

    override fun unlock(node: Node) {
        if (node.next.value == null) {
            if (tail.compareAndSet(node, null)) {
                return
            }
            while (node.next.value == null) {
                // wait
            }
        }
        node.next.value!!.locked.value = false
        env.unpark(node.next.value!!.thread)
    }

    class Node {
        val thread: Thread = Thread.currentThread()
        val next = AtomicReference<Node?>()
        val locked = AtomicReference(true)
    }
}