import org.apache.commons.lang3.time.StopWatch
import org.apache.commons.math3.primes.Primes
import java.math.BigInteger
import java.util.*
import kotlin.collections.HashMap

var max = BigInteger.TEN.pow(16)

val primes =
        generateSequence(2) { Primes.nextPrime(it+1) }

typealias ExponentTable = IntArray

fun ExponentTable.generateLongerBy1Tables(): Sequence<ExponentTable> {
    return generateSequence(1) { it + 1 }
            .takeWhile { it <= this.lastOrNull() ?: Int.MAX_VALUE }
            .map { this + it }
            .takeWhile { it.smallestPossibleProduct() <= max }
}

fun ExponentTable.smallestPossibleProduct() =
        this.mapIndexed { idx, exponent-> primes.elementAt(idx).toBigInteger().pow(exponent)}
                .fold(BigInteger.ONE){a,b -> a * b}

fun generateNewExponentTable(): Sequence<ExponentTable> {

    return sequence {
        val exponentTables: Queue<ExponentTable> = LinkedList();
        exponentTables.offer(intArrayOf())
        yield(exponentTables.peek())

        while (exponentTables.isNotEmpty()) {
            val exponentTable = exponentTables.remove()

            yieldAll(
                exponentTable.generateLongerBy1Tables().onEach { exponentTables.offer(it) }
            )
        }
    }
}

val cache = HashMap<List<Int>, BigInteger>()

fun ExponentTable.numberOfGozintaChains() : BigInteger {

    val cached = cache[this.toList()]
    if (cached != null) return cached

    val numberOfChains = when(this.size) {
        0 -> BigInteger.ONE
        1 -> BigInteger.valueOf(2).pow(this[0]-1)
        else -> {
            val smallerExponentTable = IntArray(this.size){ 0 }
            var sum = BigInteger.ZERO;
            while (!smallerExponentTable.removeZeros().contentEquals(this)) {
                sum += smallerExponentTable.removeZeros().numberOfGozintaChains()
                smallerExponentTable.incrementLimitedBy(this)
            }
            sum;
        }
    }

    val list = this.toList()
    cache[list] = numberOfChains;
    return numberOfChains;
}

fun ExponentTable.removeZeros(): ExponentTable {

    var zeroCount = 0;
    for (i in this) {
        if (i == 0) zeroCount++
    }

    val copy = IntArray(this.size-zeroCount){0}
    var idx = 0
    for (i in 0..this.size-1) {
        if (this[i] != 0) {
            copy[idx++] = this[i]
        }
    }
    return copy
}

fun ExponentTable.incrementLimitedBy(upperLimit: ExponentTable) {

    for ((idx, value) in upperLimit.withIndex()) {
        if (this[idx] < upperLimit[idx]) {
            this[idx]++
            break
        } else {
            this[idx] = 0
        }
    }
}

fun BigInteger.factorizationExponents(): IntArray {

    var toFactor = this.toLong()
    val exponentTable = arrayListOf<Int>()

    var divisor = 2L
    while (divisor * divisor <= toFactor) {
        var exponent = 0;
        while (toFactor.rem(divisor) == 0L) {
            toFactor /= divisor
            exponent++
        }
        if (exponent != 0) exponentTable.add(exponent)

        divisor++
    }

    if (toFactor != 1L) exponentTable.add(1)

    return exponentTable.toIntArray().sortedArrayDescending()
}

fun main(args: Array<String>) {
    val stopWatch = StopWatch.createStarted()
    solve()
    stopWatch.split()
    println(stopWatch.toSplitString());
}

fun solve() {

//    var c = 0;

    return generateNewExponentTable()
            .map { it to it.numberOfGozintaChains() }
            .filter { it.second <= max }
//            .onEach { if ((++c).rem(100) == 0) println("*** $c") }
//            .onEach { println("${Arrays.toString(it.first)}, chains: ${it.second}, fact: ${Arrays.toString(it.second.factorizationExponents())}") }
            .filter { Arrays.equals(it.first, it.second.factorizationExponents())  }
            .map { it.second }
            .onEach { println(it) }
            .reduce {a, b -> a+b}
            .run { println("answer: $this") }
}
