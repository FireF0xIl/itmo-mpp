import kotlinx.atomicfu.*

class AtomicArray<E>(size: Int, initialValue: E) {
    private val a = atomicArrayOfNulls<Any>(size)

    init {
        for (i in 0 until size) a[i].value = initialValue
    }

    fun get(index: Int): E = complete(index)

    fun set(index: Int, value: E) {
        while (!a[index].compareAndSet(complete(index), value)) {
            // repeat
        }
    }

    fun cas(index: Int, expected: E, update: E): Boolean {
        while (true) {
            val element = get(index)
            if (element !is Descriptor) {
                if (element == expected) {
                    if (a[index].compareAndSet(expected, update)) {
                        return true
                    }
                } else {
                    return false
                }
            }
        }
    }

    fun cas2(index1: Int, expected1: E, update1: E,
             index2: Int, expected2: E, update2: E): Boolean {
        if (index1 == index2) {
            return if (expected1 == expected2) {
                cas(index1, expected1, update2)
            } else {
                false
            }
        } else {
            val casnDescriptor = if (index1 < index2) {
                CASNDescriptor(index1, expected1, update1, index2, expected2, update2)
            } else {
                CASNDescriptor(index2, expected2, update2, index1, expected1, update1)
            }

            while (true) {
                val element = a[casnDescriptor.index1].value
                if (element !is Descriptor) {
                    if (casnDescriptor.expected1 == element) {
                        if (a[casnDescriptor.index1].compareAndSet(casnDescriptor.expected1, casnDescriptor)) {
                            casnDescriptor.complete()
                            return casnDescriptor.outcome.value == Outcome.SUCCESS
                        }
                    } else {
                        return false
                    }
                } else {
                    complete(casnDescriptor.index1)
                }
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun complete(index: Int): E =
        a[index].loop { element ->
            if (element is Descriptor) {
                element.complete()
            } else {
                return element as E
            }
        }

    private enum class Outcome {
        UNDECIDED, SUCCESS, FAIL
    }

    private abstract class Descriptor {
        abstract fun complete()
    }

    private inner class CASNDescriptor (val index1: Int, val expected1: E, val update1: E,
                                        val index2: Int, val expected2: E, val update2: E
    ): Descriptor() {
        val outcome = atomic(Outcome.UNDECIDED)

        override fun complete() {
            while (true) {
                when (outcome.value) {
                    Outcome.SUCCESS -> {
                        push(index1, update1, index2, update2)
                        break
                    }
                    Outcome.FAIL -> {
                        push(index1, expected1, index2, expected2)
                        break
                    }
                    else -> {
                        val element = a[index2].value
                        if (element is Descriptor) {
                            if (element === this) {
                                outcome.compareAndSet(Outcome.UNDECIDED, Outcome.SUCCESS)
                                push(index1, update1, index2, update2)
                                break
                            } else {
                                element.complete()
                            }
                        } else {
                            if (element == expected2) {
                                val dcssDescriptor = DCSSDescriptor(index2, expected2, this)
                                if (a[index2].compareAndSet(expected2, dcssDescriptor)) {
                                    dcssDescriptor.complete()
                                }
                            } else {
                                outcome.compareAndSet(Outcome.UNDECIDED, Outcome.FAIL)
                                push(index1, expected1, index2, expected2)
                                break
                            }
                        }
                    }
                }
            }
        }

        private fun push(index1: Int, value1: E, index2: Int, value2: E) {
            a[index1].compareAndSet(this, value1)
            a[index2].compareAndSet(this, value2)
        }
    }

    private inner class DCSSDescriptor(val index: Int, val expected: E, val update: CASNDescriptor): Descriptor() {
        override fun complete() {
            val element = if (update.outcome.value == Outcome.UNDECIDED) update else expected as Any
            a[index].compareAndSet(this, element)
        }
    }
}