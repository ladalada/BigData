import java.math.BigInteger

/*
Task 2.2
Посчитать, какое суммарное количество простых множителей
присутствует при факторизации всех чисел.
Например, пусть всего два числа: 6 и 8.
6 = 2 * 3, 8 = 2 * 2 * 2. Ответ 5.
 */

// Вычисляется количество простых множителей при факторизации одного числа
fun getNumFactorsAmount(number: BigInteger): Int {
    var num = number
    var count: Int = 0
    val two = BigInteger.valueOf(2)

    while (num.mod(two) == BigInteger.ZERO) {
        count++
        num = num.divide(two)
    }

    if (num.compareTo(BigInteger.ONE) > 0) {
        var factor = BigInteger.valueOf(3)

        while (factor.multiply(factor).compareTo(num) <= 0) {
            if (num.mod(factor) == BigInteger.ZERO) {
                count++
                num = num.divide(factor)
            } else {
                factor = factor.add(two)
            }
        }
        count++

    }
    return count
}