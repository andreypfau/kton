package io.github.andreypfau.kton

import kotlinx.atomicfu.atomic
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.ton.api.tonnode.TonNodeBlockIdExt
import org.ton.block.Block
import org.ton.block.ExtBlkRef
import org.ton.block.PrevBlkInfo
import org.ton.block.PrevBlksInfo

private typealias BlockEntry = Pair<TonNodeBlockIdExt, Block>

const val FLUSH_SIZE = 100

object TonBlocksTable {
    private val queue = Channel<BlockEntry>(capacity = 1000)
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
//            println("flushed blocks: $flushed (in query: $inQuery")
        }
    }

    suspend fun saveBlock(entry: BlockEntry) {
        queue.send(entry)
        queueSize.incrementAndGet()
    }

    private suspend fun flush(): Int {
        val entries = queue.receive(FLUSH_SIZE)
        val query = buildString {
            append("INSERT INTO kton.ton_blocks VALUES ")
            entries.joinTo(this) { (id, block) ->
                val valueFlow = block.valueFlow.value
                val blockExtra = block.extra.value
                val blockInfo = block.info.value
                val prevRef1: ExtBlkRef
                val prevRef2: ExtBlkRef?
                when (val prevRef = blockInfo.prevRef.value) {
                    is PrevBlkInfo -> {
                        prevRef1 = prevRef.prev
                        prevRef2 = null
                    }

                    is PrevBlksInfo -> {
                        prevRef1 = prevRef.prev1.value
                        prevRef2 = prevRef.prev2.value
                    }
                }
                val prevVertRef = (blockInfo.prevVertRef?.value as? PrevBlkInfo)?.prev
                buildString {
                    append("(")
                    append(id.workchain).append(",")
                    append(id.shard).append(",")
                    append(id.seqno).append(",")
                    append(id.rootHash.ch).append(",")
                    append(id.fileHash.ch).append(",")
                    append(block.globalId).append(",")
                    append(blockInfo.version).append(",")
                    appendComma(blockInfo.notMaster)
                    append(blockInfo.afterMerge).append(",")
                    append(blockInfo.beforeSplit).append(",")
                    append(blockInfo.afterSplit).append(",")
                    append(blockInfo.wantSplit).append(",")
                    append(blockInfo.wantMerge).append(",")
                    append(blockInfo.keyBlock).append(",")
                    append(blockInfo.vertSeqnoIncr).append(",")
                    append(blockInfo.vertSeqNo).append(",")
                    append(blockInfo.genUtime).append(",")
                    append(blockInfo.startLt).append(",")
                    append(blockInfo.endLt).append(",")
                    append(blockInfo.genValidatorListHashShort).append(",")
                    append(blockInfo.genCatchainSeqno).append(",")
                    append(blockInfo.minRefMcSeqno).append(",")
                    append(blockInfo.prevKeyBlockSeqno).append(",")
                    append(blockInfo.genSoftware?.version.ch).append(",")
                    append(blockInfo.genSoftware?.capabilities.ch).append(",")
                    append(blockInfo.masterRef?.value?.master?.endLt.ch).append(",")
                    append(blockInfo.masterRef?.value?.master?.seqNo.ch).append(",")
                    append(blockInfo.masterRef?.value?.master?.rootHash.ch).append(",")
                    append(blockInfo.masterRef?.value?.master?.fileHash.ch).append(",")
                    append(prevRef1.endLt).append(",")
                    append(prevRef1.seqNo).append(",")
                    append(prevRef1.rootHash.ch).append(",")
                    append(prevRef1.fileHash.ch).append(",")
                    append(prevRef2?.endLt.ch).append(",")
                    append(prevRef2?.seqNo.ch).append(",")
                    append(prevRef2?.rootHash.ch).append(",")
                    append(prevRef2?.fileHash.ch).append(",")
                    append(prevVertRef?.endLt.ch).append(",")
                    append(prevVertRef?.seqNo.ch).append(",")
                    append(prevVertRef?.rootHash.ch).append(",")
                    append(prevVertRef?.fileHash.ch).append(",")
                    appendComma(valueFlow.fromPrevBlk.coins.ch)
                    appendComma(valueFlow.toNextBlk.coins.ch)
                    appendComma(valueFlow.imported.coins.ch)
                    appendComma(valueFlow.exported.coins.ch)
                    appendComma(valueFlow.feesCollected.coins.ch)
                    appendComma(valueFlow.feesImported.coins.ch)
                    appendComma(valueFlow.recovered.coins.ch)
                    appendComma(valueFlow.created.coins.ch)
                    appendComma(valueFlow.minted.coins.ch)
                    appendComma(block.stateUpdate.value.oldHash.ch)
                    appendComma(block.stateUpdate.value.newHash.ch)
                    appendComma(blockExtra.randSeed.ch)
                    append(blockExtra.createdBy.ch)
                    append(")")
                }
            }
        }
        ClickHouse(query)
        return entries.size
    }

    private suspend fun createTable() {
        val query = """
            CREATE TABLE IF NOT EXISTS kton.ton_blocks
            (
                workchain                     Int32,
                shard                         UInt64,
                seqno                         UInt32,
                root_hash                     FixedString(64),
                file_hash                     FixedString(64),
                global_id                     Int32,
                version                       UInt32,
                not_master                    Bool,
                after_merge                   Bool,
                before_split                  Bool,
                after_split                   Bool,
                want_split                    Bool,
                want_merge                    Bool,
                key_block                     Bool,
                vert_seqno_incr               Bool,
                vert_seqno                    UInt32,
                gen_utime                     DateTime,
                start_lt                      UInt64,
                end_lt                        UInt64,
                gen_validator_list_hash_short UInt32,
                gen_catchain_seqno            UInt32,
                min_ref_mc_seqno              UInt32,
                prev_key_block_seqno          UInt32,
                gen_software_version UInt32,
                gen_software_capabilities UInt32,
                master_ref_end_lt UInt64,
                master_ref_seqno UInt32,
                master_ref_root_hash FixedString(64),
                master_ref_file_hash FixedString(64),
                prev_ref1_end_lt UInt64,
                prev_ref1_seqno UInt32,
                prev_ref1_root_hash FixedString(64),
                prev_ref1_file_hash FixedString(64),
                prev_ref2_end_lt UInt64,
                prev_ref2_seqno UInt32,
                prev_ref2_root_hash FixedString(64),
                prev_ref2_file_hash FixedString(64),
                prev_vert_ref_end_lt UInt64,
                prev_vert_ref_seqno UInt32,
                prev_vert_ref_root_hash FixedString(64),
                prev_vert_ref_file_hash FixedString(64),
                value_flow_from_prev_blk_nanotons UInt128,
                value_flow_to_next_blk_nanotons UInt128,
                value_flow_imported_nanotons UInt128,
                value_flow_exported_nanotons UInt128,
                value_flow_fees_collected_nanotons UInt128,
                value_flow_fees_imported_nanotons UInt128,
                value_flow_recovered_nanotons UInt128,
                value_flow_created_nanotons UInt128,
                value_flow_minted_nanotons UInt128,
                state_update_old_hash FixedString(64),
                state_update_new_hash FixedString(64),
                rand_seed FixedString(64),
                created_by FixedString(64)
            )
            ENGINE = MergeTree
            ORDER BY (gen_utime, workchain, shard, seqno);
        """.trimIndent()
        ClickHouse(query)
    }
}
