import java.io.File
import java.math.BigInteger
import java.util.*

/*
Task 2.1
Нужно сгенерировать файл,
содержащий 2000 48-битных случайных целых чисел,
каждое число на отдельной строке.
Результат: data48.txt
 */

fun getNumbers(size: Int, bitsNum: Int): ArrayList<BigInteger> {
    val numbers = ArrayList<BigInteger>()
    for (i in 1..size) {
        numbers.add(BigInteger(bitsNum, Random()))
    }
    return numbers
}

fun generateFile(fileName: String, size: Int, bitsNum: Int) {
    val numbers = getNumbers(size, bitsNum)
    File(fileName).writeText(numbers.joinToString("\n"))
}

fun getDataList(fileName: String): List<BigInteger> {
    val dataList = mutableListOf<BigInteger>()
    val lines: List<String> = File(fileName).readLines()
    lines.forEach {
        line -> dataList.add(BigInteger(line))
    }
    return dataList
}

fun main() {
    generateFile("data48.txt", 2000, 48)
}

