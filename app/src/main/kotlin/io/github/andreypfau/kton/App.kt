package io.github.andreypfau.kton

import io.github.andreypfau.kton.utils.suspendRetry
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import org.ton.api.tonnode.Shard
import org.ton.api.tonnode.TonNodeBlockId
import org.ton.api.tonnode.TonNodeBlockIdExt
import org.ton.bigint.BigInt
import org.ton.block.*
import org.ton.boc.BagOfCells
import org.ton.lite.api.LiteApi
import org.ton.lite.api.liteserver.LiteServerBlockHeader
import org.ton.lite.api.liteserver.functions.LiteServerGetBlock
import org.ton.lite.api.liteserver.functions.LiteServerLookupBlock
import java.io.File
import kotlin.time.Duration.Companion.seconds

val KTON_DATA_DIR = System.getenv("KTON_DATA_DIR") ?: "kton/data"
val KTON_USE_BLOCK_CACHE = (System.getenv("KTON_USE_BLOCK_CACHE") ?: "false").toBoolean()
val KTON_START_FROM_SEQNO = (System.getenv("KTON_START_FROM_SEQNO") ?: "26200000").let {
    requireNotNull(it.toIntOrNull()) { "Invalid KTON_START_FROM_SEQNO: '$it'" }
}

suspend fun main() {
    collectBlocks()
}

private suspend fun collectBlocks() {
    TonLiteApiService.init()
    val liteClient = TonLiteApiService.liteClient
    val initBlock = liteClient.lookupBlock(TonNodeBlockId(-1, 0x8000000000000000u.toLong(), KTON_START_FROM_SEQNO))!!

    var initBlockTime = Instant.DISTANT_PAST
    var progressText = atomic("")
    val blocks = atomic(0)
    GlobalScope.launch {
        while (true) {
            delay(1000)
            blocks.value = 0
            println(progressText.value)
        }
    }
    var lastSeqno = -1
    var check = -1
    mcBlockFlow(initBlock).buffer().map { it.await() }.toBlockFlow().buffer().map { it.await() }.onEach { (id, block) ->
        val blockTime = Instant.fromEpochSeconds(block.info.value.genUtime.toLong())
        if (initBlockTime == Instant.DISTANT_PAST) {
            initBlockTime = blockTime
        }
        val now = Instant.fromEpochSeconds(Clock.System.now().epochSeconds)
        val delay = now - blockTime
        val startDelay = now - initBlockTime

        val round = 1_000000.0
        var progress = -100.0 + (((startDelay.inWholeSeconds.toDouble() / delay.inWholeSeconds.toDouble())) * 100.0)
        progress = (progress * round).toLong() / round

        progressText.value = buildString {
            if (progress in 0.001..99.999999) {
                append("(").append(progress).append("%) ")
            }
            append("[").append(blockTime).append("] ")
            append("masterchain block: ").append(id.seqno).append(" ")
            if (delay > 10.seconds) {
                append("(out of sync: ").append(delay).append(") ")
            }
            val newBlocksPerSecond = blocks.incrementAndGet()
            if (newBlocksPerSecond > 1) {
                append(newBlocksPerSecond).append(" MC Blocks/sec")
            }
        }
    }.map { it.second }.toShardBlockIdFlow().toBlockFlow().buffer().map { it.await() }
        .transform { (id, block) ->
            if (id.seqno == lastSeqno) return@transform
            val prev = block.prevBlocks(lastSeqno).toList().asReversed()
            prev.forEach { b ->
                emit(b)
            }
            lastSeqno = id.seqno
        }
        .collectLatest { (id, block) ->
            if (check > 9) {
                check(check == id.seqno - 1) {
                    "lost block! ${id.seqno - 1} (current ${id.seqno})"
                }
                check++
            }
//            saveFull(id, block)
        }
}

private suspend fun saveFull(id: TonNodeBlockIdExt, block: Block) = coroutineScope {
    launch {
        TonBlocksTable.saveBlock(id to block) // save Basechain block in db
    }
    launch {
        block.extra.value.accountBlocks.value.nodes().forEach { (accountBlock, _) ->
            accountBlock.transactions.nodes().forEach { (transaction) ->
                launch {
                    TonTransactionsTable.saveTransaction(transaction)
                }
                val aux = transaction.value.r1.value
                aux.inMsg.value?.let {
                    launch {
                        TonMessagesTable.saveMessage(it)
                    }
                }
                aux.outMsgs.nodes().forEach { (_, message) ->
                    launch {
                        TonMessagesTable.saveMessage(message)
                    }
                }
            }
        }
    }
}

private fun mcBlockFlow(startFrom: TonNodeBlockIdExt) = flow {
    coroutineScope {
        val workchain = startFrom.workchain
        val shard = startFrom.shard
        var seqno = startFrom.seqno
        var blockId: Deferred<TonNodeBlockIdExt> = CompletableDeferred(startFrom)
        while (true) {
            emit(blockId)
            seqno++
            val finalSeqno = seqno
            blockId = async {
//                println("start lookup seqno: $finalSeqno")
                TonLiteApiService.liteApi.lookupBlockId(workchain, shard, finalSeqno).also {
//                    println("end lookup seqno: $finalSeqno")
                }
            }
        }
    }
}

private fun Block.prevBlocks(untilSeqno: Int): Flow<Pair<TonNodeBlockIdExt, Block>> = flow {
    var lastBlock = this@prevBlocks
    while (lastBlock.info.value.seqNo > untilSeqno && untilSeqno != -1) {
        val f = lastBlock.prevBlocksIds().asFlow().toBlockFlow().buffer().map { b ->
            lastBlock = b.await().second
            b.await()
        }
        emitAll(f)
    }
}

private fun Flow<Block>.toShardBlockIdFlow() = transform { mcBlock ->
    mcBlock.extra.value.custom.value?.value?.shardHashes?.nodes()?.forEach {
        val workchainId = BigInt(it.first.toByteArray()).toInt()
        val shards = getShards(it.second).toList()
        shards.forEach { (shardId, shardDesc) ->
            val shardBlockId = when (shardDesc) {
                is ShardDescrNew -> TonNodeBlockIdExt(
                    workchainId,
                    shardId,
                    shardDesc.seqNo.toInt(),
                    shardDesc.rootHash,
                    shardDesc.fileHash
                )

                is ShardDescrOld -> TonNodeBlockIdExt(
                    workchainId,
                    shardId,
                    shardDesc.seqNo.toInt(),
                    shardDesc.rootHash,
                    shardDesc.fileHash
                )
            }
            emit(shardBlockId)
        }
    }
}

private fun Flow<TonNodeBlockIdExt>.toBlockFlow() = map { blockId ->
    coroutineScope {
        async {
            blockId to downloadBlock(blockId)
        }
    }
}

private val ShardIdent.shardId: Long
    get() =
        shardPrefix.toLong() or (1L shl (63 - shardPfxBits))

private fun Block.prevBlocksIds() = sequence {
    val block = this@prevBlocksIds
    val shardId = block.info.value.shard.shardId
    val workchainId = block.info.value.shard.workchainId
    when (val prevRef = block.info.value.prevRef.value) {
        is PrevBlkInfo -> {
            val prev = prevRef.prev
            yield(TonNodeBlockIdExt(workchainId, shardId, prev.seqNo.toInt(), prev.rootHash, prev.fileHash))
        }

        is PrevBlksInfo -> {
            val prev1 = prevRef.prev1.value
            val prev2 = prevRef.prev2.value
            yield(TonNodeBlockIdExt(workchainId, shardId, prev1.seqNo.toInt(), prev1.rootHash, prev1.fileHash))
            yield(TonNodeBlockIdExt(workchainId, shardId, prev2.seqNo.toInt(), prev2.rootHash, prev2.fileHash))
        }
    }
}

private fun getShards(
    binTree: BinTree<ShardDescr>,
    shard: Long = Shard.ID_ALL
): Sequence<Pair<Long, ShardDescr>> =
    when (binTree) {
        is BinTreeFork -> {
            val x = ((shard and (shard.inv() + 1)) ushr 1)
            sequence {
                yieldAll(getShards(binTree.left.value, shard - x))
                yieldAll(getShards(binTree.right.value, shard + x))
            }
        }

        is BinTreeLeaf -> sequenceOf(shard to (binTree.leaf))
    }

private suspend fun downloadBlock(blockIdExt: TonNodeBlockIdExt): Block {
    val fileName = blockIdExt.fileHash.toString()
    val file = File(KTON_DATA_DIR, "block_data/${fileName.substring(0, 2)}/$fileName.boc")
    val fileExists = file.exists()
    val blockData = if (KTON_USE_BLOCK_CACHE && fileExists) {
        file.readBytes()
    } else {
        suspendRetry(10) {
            TonLiteApiService.liteApi(LiteServerGetBlock(blockIdExt)).data
        }
    }
    if (KTON_USE_BLOCK_CACHE && !fileExists) {
        file.parentFile.mkdirs()
        file.writeBytes(blockData)
    }
    return Block.loadTlb(BagOfCells(blockData).first())
}

private suspend fun LiteApi.lookupBlockId(workchain: Int, shard: Long, seqno: Int): TonNodeBlockIdExt {
    val blockId = TonNodeBlockId(workchain, shard, seqno)
    val fileName = blockId.toString().let {
        it.substring(1, it.lastIndex)
    }.replace(":", "/")
    val file = File(KTON_DATA_DIR, "block_header/$fileName")
    val fileExists = file.exists()
    val blockHeader = if (KTON_USE_BLOCK_CACHE && fileExists) {
        LiteServerBlockHeader.decodeBoxed(file.readBytes())
    } else {
        suspendRetry(10) {
            invoke(
                LiteServerLookupBlock(
                    LiteServerLookupBlock.ID_MASK,
                    blockId,
                    null,
                    null
                ), seqno
            )
        }
    }
    if (KTON_USE_BLOCK_CACHE && !fileExists) {
        file.parentFile.mkdirs()
        file.writeBytes(LiteServerBlockHeader.encodeToByteArray(blockHeader))
    }
    return blockHeader.id
}
