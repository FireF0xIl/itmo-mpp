/**
 * В теле класса решения разрешено использовать только переменные делегированные в класс RegularInt.
 * Нельзя volatile, нельзя другие типы, нельзя блокировки, нельзя лазить в глобальные переменные.
 *
 * @author : FireF0xIl
 */

class Solution : MonotonicClock {
    private var c1 by RegularInt(0)
    private var c2 by RegularInt(0)
    private var c3 by RegularInt(0)
    private var c4 by RegularInt(0)
    private var c5 by RegularInt(0)

    override fun write(time: Time) {
        c1 = time.d1
        c2 = time.d2
        c3 = time.d3
        c5 = c2
        c4 = c1
    }

    override fun read(): Time {
        val time1 = c4
        val time2 = c5
        val time3 = c3
        val time4 = c2
        val time5 = c1

        return if (time5 == time1) {
            if (time2 == time4) Time(time5, time4, time3)
            else                Time(time5, time4, 0)
        } else {
            Time(time5, 0, 0)
        }
    }
}
