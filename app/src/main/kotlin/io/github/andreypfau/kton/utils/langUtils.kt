package io.github.andreypfau.kton.utils

suspend fun <T> suspendRetry(attempts: Int, block: suspend ()->T): T {
    var i = 0
    while (true) {
        try {
            return block()
        } catch (t: Throwable) {
            if (i++ > attempts) throw t
        }
    }
}
