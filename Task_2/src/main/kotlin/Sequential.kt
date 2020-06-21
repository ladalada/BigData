/*
Task 2.3
Способ 1
Реализовать подсчет
простым последовательным алгоритмом.
Измерить время выполнения.
 */

fun sequentialCounting(fileName: String): Int {
    var totalCount: Int = 0
    val dataList = getDataList(fileName)
    dataList.forEach {
        num -> totalCount += getNumFactorsAmount(num)
    }
    return totalCount
}

fun main() {
    val startTime = System.currentTimeMillis()
    val result = sequentialCounting("data48.txt")
    val finishTime = System.currentTimeMillis()

    println("Подсчет последовательным алгоритмом")
    println("Общее кол-во множителей: $result")
    println("Время выполнения (мс): ${finishTime - startTime}")
}

/*
Подсчет последовательным алгоритмом
Общее кол-во множителей: 8855
Время выполнения (мс): 246785
 */