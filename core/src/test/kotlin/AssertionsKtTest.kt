import com.attardo.datavalidation.core.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import kotlin.random.Random

val ones : List<Number> = listOf(1, 1L, 1.0, 1.0f)
val zeroes : List<Number> = listOf(0, 0L, 0.0, 0.0f)

class AssertionsKtTest {
    @Test
    fun testAssertLessThan() {
        ones.cartesian(zeroes).forEach { (left, right) ->
            assertDoesNotThrow { assertLessThan(left, right) }
        }

        zeroes.cartesian(zeroes + ones).forEach { (left, right) ->
            assertThrows<AssertionError> { assertLessThan(left, right) }
        }
    }

    @Test
    fun testAssertLessThanOrEqualTo() {
        ones.cartesian(zeroes + ones).forEach { (left, right) ->
            assertDoesNotThrow { assertLessThanOrEqualTo(left, right) }
        }

        zeroes.cartesian(ones).forEach { (left, right) ->
            assertThrows<AssertionError> { assertLessThanOrEqualTo(left, right) }
        }
    }

    @Test
    fun testAssertGreaterThan() {
        zeroes.cartesian(ones).forEach { (left, right) ->
            assertDoesNotThrow { assertGreaterThan(left, right) }
        }

        ones.cartesian(zeroes + ones).forEach { (left, right) ->
            assertThrows<AssertionError> { assertGreaterThan(left, right) }
        }
    }

    @Test
    fun testAssertGreaterThanOrEqualTo() {
        zeroes.cartesian(zeroes + ones).forEach { (left, right) ->
            assertDoesNotThrow { assertGreaterThanOrEqualTo(left, right) }
        }

        ones.cartesian(zeroes).forEach { (left, right) ->
            assertThrows<AssertionError> { assertGreaterThanOrEqualTo(left, right) }
        }
    }

    @Test
    fun compareTo() {
        val random = Random(123456789)
        val ints = (1..20).map { random.nextInt() }
        val longs = (1..20).map { random.nextLong() }
        val floats = (1..20).map { random.nextFloat() }
        val doubles = (1..20).map { random.nextDouble() }
        ints.forEach { a->
            ints.forEach { b->
                assertEquals(a.compareTo(b), (a as Number).compareTo((b as Number)))
            }
            longs.forEach { b->
                assertEquals(a.compareTo(b), (a as Number).compareTo((b as Number)))
            }
            floats.forEach { b->
                assertEquals(a.compareTo(b), (a as Number).compareTo((b as Number)))
            }
            doubles.forEach { b->
                assertEquals(a.compareTo(b), (a as Number).compareTo((b as Number)))
            }
        }
        longs.forEach { a->
            ints.forEach { b->
                assertEquals(a.compareTo(b), (a as Number).compareTo((b as Number)))
            }
            longs.forEach { b->
                assertEquals(a.compareTo(b), (a as Number).compareTo((b as Number)))
            }
            floats.forEach { b->
                assertEquals(a.compareTo(b), (a as Number).compareTo((b as Number)))
            }
            doubles.forEach { b->
                assertEquals(a.compareTo(b), (a as Number).compareTo((b as Number)))
            }
        }
        floats.forEach { a->
            ints.forEach { b->
                assertEquals(a.compareTo(b), (a as Number).compareTo((b as Number)))
            }
            longs.forEach { b->
                assertEquals(a.compareTo(b), (a as Number).compareTo((b as Number)))
            }
            floats.forEach { b->
                assertEquals(a.compareTo(b), (a as Number).compareTo((b as Number)))
            }
            doubles.forEach { b->
                assertEquals(a.compareTo(b), (a as Number).compareTo((b as Number)))
            }
        }
        doubles.forEach { a->
            ints.forEach { b->
                assertEquals(a.compareTo(b), (a as Number).compareTo((b as Number)))
            }
            longs.forEach { b->
                assertEquals(a.compareTo(b), (a as Number).compareTo((b as Number)))
            }
            floats.forEach { b->
                assertEquals(a.compareTo(b), (a as Number).compareTo((b as Number)))
            }
            doubles.forEach { b->
                assertEquals(a.compareTo(b), (a as Number).compareTo((b as Number)))
            }
        }
    }
}

fun <T, U> List<T>.cartesian(other: List<U>): List<Pair<T, U>> =
    flatMap {first -> other.map { second -> first to second  } }