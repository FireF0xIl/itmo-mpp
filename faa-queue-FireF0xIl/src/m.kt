package mpp.faaqueue

fun main() {
    val q = FAAQueue<Int>()
    q.enqueue(1)
    q.enqueue(2)
    q.enqueue(3)
    println(q.dequeue())
    println(q.dequeue())
    println(q.dequeue())
}