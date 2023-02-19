package io.github.andreypfau.kton

import io.github.andreypfau.kton.utils.suspendRetry
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import io.ktor.client.request.*
import org.ton.bigint.BigInt
import org.ton.bitstring.BitString
import org.ton.bitstring.Bits256
import org.ton.block.Coins
import org.ton.block.VarUInteger

object ClickHouse {
    val client = HttpClient(CIO) {
        install(HttpTimeout)
    }

    suspend operator fun invoke(query: String): String = suspendRetry(10) {
        client.post("http://65.108.207.51:8123/") {
            timeout {
                requestTimeoutMillis = 5000
            }
            setBody(query)
        }.body<String>()
    }
}

val UInt?.ch get() = this ?: 0u
val ULong?.ch get() = this ?: 0uL
val Int?.ch get() = this ?: 0
val Long?.ch get() = this ?: 0L
val Boolean?.ch get() = this == true

val Coins?.ch get() = this?.amount.ch
val VarUInteger?.ch get() = this?.value.ch
val BigInt?.ch get() = this?.toString() ?: "0"
val Bits256?.ch get() = "'${this ?: Bits256()}'"
val String?.ch get() = "'$this'"
val BitString?.ch get() = "'${this ?: BitString.empty()}'"
