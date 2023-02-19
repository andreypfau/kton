package io.github.andreypfau.kton

import kotlinx.atomicfu.atomic
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.ton.bitstring.Bits256
import org.ton.block.*
import org.ton.cell.Cell
import org.ton.tlb.CellRef

object TonMessagesTable {
    const val FLUSH_SIZE = 1000
    private val queue = Channel<CellRef<Message<Cell>>>(capacity = FLUSH_SIZE)
    private val queueSize = atomic(0)

    init {
        runBlocking {
            createTable()
        }
    }

    private val job = GlobalScope.launch {
        while (true) {
            delay(1000)
            val flushed = flush()
            val inQuery = queueSize.addAndGet(-flushed)
//            println("flushed messages: $flushed (in query: $inQuery")
        }
    }

    suspend fun saveMessage(message: CellRef<Message<Cell>>) {
        queue.send(message)
        queueSize.incrementAndGet()
    }

    private suspend fun flush(): Int {
        val messages = queue.receive(FLUSH_SIZE)
        val query = buildString {
            append("INSERT INTO kton.ton_messages VALUES ")
            messages.joinTo(this) { messageCell ->
                val message = messageCell.value
                val messageInfo = message.info
                val intMsgInfo = messageInfo as? IntMsgInfo
                val extInMsgInfo = messageInfo as? ExtInMsgInfo
                val src = messageInfo.src
                val dest = messageInfo.dest
                val srcAnycastRewritePfx = src.anycastRewritePfx
                val destAnycastRewritePfx = dest.anycastRewritePfx
                val commonMessageType = when(message.info) {
                    is IntMsgInfo -> 0
                    is ExtInMsgInfo -> 1
                    is ExtOutMsgInfo -> 2
                }
                buildString {
                    append("(")
                    appendComma(Bits256(messageCell.toCell().hash()).ch)
                    appendComma(commonMessageType)
                    appendComma(intMsgInfo?.ihrDisabled.ch)
                    appendComma(intMsgInfo?.bounce.ch)
                    appendComma(intMsgInfo?.bounced.ch)
                    appendComma(src.type)
                    appendComma(srcAnycastRewritePfx != null)
                    appendComma(srcAnycastRewritePfx.ch)
                    appendComma(src.workchain)
                    appendComma(src.address.ch)
                    appendComma(dest.type)
                    appendComma(destAnycastRewritePfx != null)
                    appendComma(destAnycastRewritePfx.ch)
                    appendComma(dest.workchain)
                    appendComma(dest.address.ch)
                    appendComma(intMsgInfo?.value?.coins.ch)
                    appendComma(intMsgInfo?.ihr_fee.ch)
                    appendComma(intMsgInfo?.fwd_fee.ch)
                    appendComma(extInMsgInfo?.importFee.ch)
                    appendComma(messageInfo.createdLt.ch)
                    append(messageInfo.createdAt.ch)
                    append(")")
                }
            }
        }
        ClickHouse(query)
        return messages.size
    }

    private suspend fun createTable() {
        val query = """
        CREATE TABLE IF NOT EXISTS kton.ton_messages
            (
                hash FixedString(64),
                info Enum('int_msg_info' = 0, 'ext_in_msg_info' = 1, 'ext_out_msg_info' = 2),
                ihr_disabled Bool,
                bounce Bool,
                bounced Bool,
                src_type Enum('addr_none' = 0, 'addr_extern' = 1, 'addr_std' = 2, 'addr_var' = 3),
                src_anycast Bool,
                src_anycast_rewrite_pfx UInt32,
                src_workchain Int32,
                src_address String,
                dest_type Enum('addr_none' = 0, 'addr_extern' = 1, 'addr_std' = 2, 'addr_var' = 3),
                dest_anycast Bool,
                dest_anycast_rewrite_pfx UInt32,
                dest_workchain Int32,
                dest_address String,
                value_nanotons UInt128,
                ihr_fee UInt128,
                fwd_fee UInt128,
                import_fee UInt128,
                created_lt UInt64,
                created_at DateTime
            )
            ENGINE = MergeTree
            ORDER BY (created_at, src_address, dest_address);
        """.trimIndent()
        ClickHouse(query)
    }
}
