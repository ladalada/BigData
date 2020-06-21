import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.UntypedAbstractActor
import akka.routing.RoundRobinPool
import java.math.BigInteger

/*
Task 2.3
Способ 3
Реализовать подсчет с помощью Akka.
Измерить время выполнения.
 */

data class Numbers(val numbersList: MutableList<BigInteger>)

class Start

var totalAkkaCount = 0

@Synchronized fun addToTotalAkkaCount(count: Int) {
    totalAkkaCount += count
}

fun main() {
    val startTime = System.currentTimeMillis()
    val system = ActorSystem.create("MySystem")

    val masterActor = system.actorOf(Props.create(MasterActor::class.java), "master")
    masterActor.tell(Start(), null)

    system.whenTerminated.thenRun {
        -> val finishTime = System.currentTimeMillis()
        println("Подсчет с помощью Akka")
        println("Общее кол-во множителей: $totalAkkaCount")
        println("Время выполнения (мс): ${finishTime - startTime}")
    }
}

class ComputingActor: UntypedAbstractActor() {
    override fun onReceive(message: Any?) {
        when(message) {
            is Numbers -> {
                for (num in message.numbersList) {
                    var count = getNumFactorsAmount(num)
                    addToTotalAkkaCount(count)
                }
                println("Actor finished")
                context.system.terminate()
            }
        }
    }
}

class MasterActor: UntypedAbstractActor() {
    private val fileName = "data48.txt"
    private val actorsCount = 6
    val worker = context.actorOf(RoundRobinPool(actorsCount).props(Props.create(ComputingActor::class.java)), "calculator")

    override fun onReceive(message: Any?) {
        when(message) {
            is Start -> {
                val numbers = getDataList(fileName).toMutableList()
                val listsForActors = getListOfLists(numbers, actorsCount)

                for (list in listsForActors) {
                    worker.tell(Numbers(list), self)
                }
            }
        }
    }
}

fun getListOfLists(dataList: MutableList<BigInteger>, count: Int): MutableList<MutableList<BigInteger>> {
    var lists: MutableList<MutableList<BigInteger>> = mutableListOf()
    val size = dataList.size

    var start = 0
    val amountForAll: Int = size / count
    var rest: Int = size - amountForAll * count

    for (actorIndex in 0 until count) {
        var list: List<BigInteger>
        var finish = start + amountForAll
        if (rest > 0) {
            finish++
            rest--
        }
        list = dataList.subList(start, finish)
        lists.add(list)
        start = finish
    }
    return lists
}

/*
4 actors
Подсчет с помощью Akka
Общее кол-во множителей: 8855
Время выполнения (мс): 124493
 */

/*
6 actors
Подсчет с помощью Akka
Общее кол-во множителей: 8855
Время выполнения (мс): 121701
 */

/*
8 actors
Подсчет с помощью Akka
Общее кол-во множителей: 8855
Время выполнения (мс): 141436
 */