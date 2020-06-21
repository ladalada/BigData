import java.math.BigInteger
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

/*
Task 2.3
Способ 2
Реализовать подсчет многопоточно,
с использованием примитивов синхронизации (semaphore).
Измерить время выполнения.
 */

var totalCount = 0

@Synchronized fun addToTotalCount(count: Int) {
    totalCount += count
}

// старт потоков и ожидание завершения потоков
fun startAndJoin(vararg blocks: () -> Unit) {
    val threads = blocks.map { thread(block = it, isDaemon = true, start = true) }
    threads.forEach { it.join() }
}

fun semaphore(fileName: String) {
    val sem = Semaphore(1) // задание кол-ва разрешений на доступ к ресурсу
    val dataList = getDataList(fileName).toMutableList()

    val r = {
        var count: Int = 0
        var stopFlag = false
        var number = mutableListOf<BigInteger>()

        while (!stopFlag) {
            try {
                sem.acquire() // получение разрешения у семафора
                if (dataList.isNotEmpty()) {
                    number.add(dataList[0])
                    dataList.removeAt(0)
                }
                TimeUnit.MICROSECONDS.sleep(100)
            } finally {
                if (number.isNotEmpty()) {
                    sem.release() // освобождение разрешения
                    count = getNumFactorsAmount(number[0])
                    number = mutableListOf<BigInteger>()
                    addToTotalCount(count)
                } else {
                    stopFlag = true
                    sem.release()
                }
            }
        }
    }

    startAndJoin(r, r, r, r, r, r) // задание кол-ва потоков, их запуск
}

fun main() {
    val startTime = System.currentTimeMillis()
    semaphore("data48.txt")
    val finishTime = System.currentTimeMillis()

    println("Подсчет с помощью 6 потоков и семафора")
    println("Общее кол-во множителей: $totalCount")
    println("Время выполнения (мс): ${finishTime - startTime}")
}

/*
Подсчет с помощью 4 потоков и семафора
Общее кол-во множителей: 8855
Время выполнения (мс): 135883
 */

/*
Подсчет с помощью 6 потоков и семафора
Общее кол-во множителей: 8855
Время выполнения (мс): 129853
 */

/*
Подсчет с помощью 8 потоков и семафора
Общее кол-во множителей: 8855
Время выполнения (мс): 139942
 */