package io.github.andreypfau.kton

import kotlinx.atomicfu.atomic
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.ton.bitstring.Bits256
import org.ton.block.*
import org.ton.tlb.CellRef

object TonTransactionsTable {
    private val queue = Channel<CellRef<Transaction>>(capacity = 1000)
    private val queueSize = atomic(0)
    const val FLUSH_SIZE = 100

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
//            println("flushed transactions: $flushed (in query: $inQuery")
        }
    }

    suspend fun saveTransaction(transaction: CellRef<Transaction>) {
        var nextTransaction: CellRef<Transaction>? = transaction
        while (nextTransaction != null) {
            queue.send(nextTransaction)
            queueSize.incrementAndGet()
            nextTransaction = transaction.value.description.value.prepareTransaction
        }
    }

    private suspend fun flush(): Int {
        val entries = queue.receive(FLUSH_SIZE).distinctBy { Bits256(it.toCell().hash()) }
        val query = buildString {
            append("INSERT INTO kton.ton_transactions VALUES ")
            entries.joinTo(this) { transactionCell ->
                val transaction = transactionCell.value
                val desc = transaction.description.value
                val descEnum = when (desc) {
                    is TransOrd -> "trans_ord"
                    is TransStorage -> "trans_storage"
                    is TransTickTock -> "trans_tick_tock"
                    is TransSplitPrepare -> "trans_split_prepare"
                    is TransSplitInstall -> "trans_split_install"
                    is TransMergePrepare -> "trans_merge_prepare"
                    is TransMergeInstall -> "trans_merge_install"
                }
                val prepareTransaction = desc.prepareTransaction?.let {
                    Bits256(it.toCell().hash())
                }
                val splitInfo = desc.splitInfo
                val storagePh = desc.storagePh
                val creditPh = desc.creditPh
                val computePh = desc.computePh
                val actionPh = desc.actionPh
                val bouncePh = desc.bouncePh

                val computePhSkipped = computePh as? TrPhaseComputeSkipped
                val computePhVm = computePh as? TrPhaseComputeVm
                val bouncePhNegFunds = bouncePh as? TrPhaseBounceNegFunds
                val bouncePhNoFunds = bouncePh as? TrPhaseBounceNoFunds
                val bouncePhOk = bouncePh as? TrPhaseBounceOk

                buildString {
                    append("(")
                    appendComma(Bits256(transactionCell.toCell().hash()).ch)
                    appendComma(transaction.accountAddr.ch)
                    appendComma(transaction.lt)
                    appendComma(transaction.prevTransHash.ch)
                    appendComma(transaction.prevTransLt.ch)
                    appendComma(transaction.now)
                    appendComma(transaction.outmsgCnt)
                    appendComma(transaction.origStatus.ordinal)
                    appendComma(transaction.endStatus.ordinal)
                    appendComma(transaction.r1.value.inMsg.value?.toCell()?.hash()?.let {
                        Bits256(it)
                    }.ch)
                    appendComma(
                        transaction.r1.value.outMsgs.nodes().joinToString(prefix = "[", postfix = "]") { (_, messageCell) ->
                            Bits256(messageCell.toCell().hash()).ch
                        }
                    )
                    appendComma(transaction.totalFees.coins.ch)
                    appendComma(transaction.stateUpdate.value.oldHash.ch)
                    appendComma(transaction.stateUpdate.value.newHash.ch)
                    appendComma(descEnum.ch)
                    appendComma((desc as? TransOrd)?.creditFirst.ch)
                    appendComma((desc as? TransTickTock)?.isTock.ch)
                    appendComma(splitInfo?.cur_shard_pfx_len.ch) // TODO: fix naming
                    appendComma(splitInfo?.acc_split_depth.ch)
                    appendComma(splitInfo?.this_addr?.toBits256().ch) // TODO: use bits256 in split info
                    appendComma(splitInfo?.sibling_addr?.toBits256().ch)
                    appendComma(prepareTransaction.ch)
                    appendComma((desc as? TransSplitInstall)?.installed.ch)
                    appendComma(desc.aborted)
                    appendComma(desc.destroyed)
                    appendComma(storagePh != null)
                    appendComma(storagePh?.storageFeesCollected.ch)
                    appendComma(storagePh?.storageFeesDue?.value.ch)
                    appendComma(storagePh?.statusChange?.ordinal.ch)
                    appendComma(creditPh != null)
                    appendComma(creditPh?.dueFeesCollected?.value.ch)
                    appendComma(creditPh?.credit?.coins.ch)
                    appendComma(computePh != null)
                    appendComma(computePhSkipped != null)
                    appendComma(computePhSkipped?.reason?.ordinal.ch)
                    appendComma(computePhVm != null)
                    appendComma(computePhVm?.success.ch)
                    appendComma(computePhVm?.msgStateUsed.ch)
                    appendComma(computePhVm?.accountActivated.ch)
                    appendComma(computePhVm?.gasFees.ch)
                    appendComma(computePhVm?.r1?.value?.gasUsed.ch)
                    appendComma(computePhVm?.r1?.value?.gasLimit.ch)
                    appendComma(computePhVm?.r1?.value?.gasCredit?.value != null)
                    appendComma(computePhVm?.r1?.value?.gasCredit?.value.ch)
                    appendComma(computePhVm?.r1?.value?.mode.ch)
                    appendComma(computePhVm?.r1?.value?.exitCode.ch)
                    appendComma(computePhVm?.r1?.value?.exitArg?.value != null)
                    appendComma(computePhVm?.r1?.value?.exitArg?.value.ch)
                    appendComma(computePhVm?.r1?.value?.vmSteps.ch)
                    appendComma(computePhVm?.r1?.value?.vmInitStateHash.ch)
                    appendComma(computePhVm?.r1?.value?.vmFinalStateHash.ch)
                    appendComma(actionPh != null)
                    appendComma(actionPh?.success.ch)
                    appendComma(actionPh?.valid.ch)
                    appendComma(actionPh?.noFunds.ch)
                    appendComma(actionPh?.statusChange?.ordinal.ch)
                    appendComma(actionPh?.totalFwdFees?.value != null)
                    appendComma(actionPh?.totalFwdFees?.value.ch)
                    appendComma(actionPh?.totalActionFees?.value.ch)
                    appendComma(actionPh?.resultCode.ch)
                    appendComma(actionPh?.resultArg?.value != null)
                    appendComma(actionPh?.resultArg?.value.ch)
                    appendComma(actionPh?.totActions.ch)
                    appendComma(actionPh?.specActions.ch)
                    appendComma(actionPh?.skippedActions.ch)
                    appendComma(actionPh?.msgsCreated.ch)
                    appendComma(actionPh?.actionListHash.ch)
                    appendComma(actionPh?.totMsgSize?.cells.ch)
                    appendComma(actionPh?.totMsgSize?.bits.ch)
                    appendComma(bouncePh != null)
                    appendComma(bouncePhNegFunds != null)
                    appendComma(bouncePhNoFunds != null)
                    appendComma(bouncePhNoFunds?.msg_size?.cells.ch) // TODO: fix naming
                    appendComma(bouncePhNoFunds?.msg_size?.bits.ch)
                    appendComma(bouncePhNoFunds?.req_fwd_fees.ch)
                    appendComma(bouncePhOk != null)
                    appendComma(bouncePhOk?.msg_size?.cells.ch) // TODO: fix naming
                    appendComma(bouncePhOk?.msg_size?.bits.ch)
                    appendComma(bouncePhOk?.msg_fees.ch)
                    append(bouncePhOk?.fwd_fees.ch)
                    append(")")
                }
            }
        }
        ClickHouse(query)
        return entries.size
    }

    private suspend fun createTable() {
//        @Language("clickhouse")
        val query = """
            CREATE TABLE IF NOT EXISTS kton.ton_transactions
            (
                hash FixedString(64),
                account FixedString(64),
                lt UInt64,
                prev_trans_hash FixedString(64),
                prev_trans_lt UInt64,
                time DateTime,
                outmsg_cnt UInt16,
                orig_status Enum('acc_state_uninit' = 0, 'acc_state_frozen' = 1, 'acc_state_active' = 2, 'acc_state_nonexist' = 3),
                end_status Enum('acc_state_uninit' = 0, 'acc_state_frozen' = 1, 'acc_state_active' = 2, 'acc_state_nonexist' = 3),
                in_msg FixedString(64),
                out_msgs Array(FixedString(64)),
                total_fees_nanotons UInt128,
                state_update_old_hash FixedString(64),
                state_update_new_hash FixedString(64),
                description Enum('trans_ord' = 0, 'trans_storage' = 1, 'trans_tick_tock' = 2, 'trans_split_prepare' = 3, 'trans_split_install' = 4, 'trans_merge_prepare' = 5, 'trans_merge_install' = 6),
                credit_first Bool,
                is_tock Bool,
                split_info_cur_shard_pfx_len UInt8,
                split_info_acc_split_depth UInt8,
                split_info_this_addr FixedString(64),
                split_info_sibling_addr FixedString(64),
                prepare_transaction FixedString(64),
                installed Bool,
                aborted Bool,
                destroyed Bool,
                storage_ph Bool,
                storage_ph_storage_fees_collected UInt128,
                storage_ph_storage_fees_due UInt128,
                storage_ph_status_change Enum('acst_unchanged' = 0, 'acst_frozen' = 1, 'acst_deleted' = 2),
                credit_ph Bool,
                credit_ph_due_fees_collected UInt128,
                credit_ph_credit_nanotons UInt128,
                compute_ph Bool,
                compute_ph_skipped Bool,
                compute_ph_skipped_reason Enum('cskip_no_state' = 0, 'cskip_bad_state' = 1, 'cskip_no_gas' = 2, 'cskip_suspended' = 3),
                compute_ph_vm Bool,
                compute_ph_vm_success Bool,
                compute_ph_vm_msg_state_used Bool,
                compute_ph_vm_account_activated Bool,
                compute_ph_vm_gas_fees UInt128,
                compute_ph_vm_gas_used UInt64,
                compute_ph_vm_gas_limit UInt64,
                compute_ph_vm_gas_credit Bool,
                compute_ph_vm_gas_credit_nanotons UInt32,
                compute_ph_vm_mode Int8,
                compute_ph_vm_exit_code Int32,
                compute_ph_vm_exit_arg Bool,
                compute_ph_vm_exit_arg_value Int32,
                compute_ph_vm_steps UInt32,
                compute_ph_vm_init_state_hash FixedString(64),
                compute_ph_vm_final_state_hash FixedString(64),
                action_ph Bool,
                action_ph_success Bool,
                action_ph_valid Bool,
                action_ph_no_funds Bool,
                action_ph_status_change Enum('acst_unchanged' = 0, 'acst_frozen' = 1, 'acst_deleted' = 2),
                action_ph_total_fwd_fees Bool,
                action_ph_total_fwd_fees_nanotons UInt128,
                action_ph_total_action_fees UInt128,
                action_ph_result_code Int32,
                action_ph_result_arg Bool,
                action_ph_result_arg_value Int32,
                action_ph_tot_actions UInt16,
                action_ph_spec_actions UInt16,
                action_ph_skipped_actions UInt16,
                action_ph_msgs_created UInt16,
                action_ph_action_list_hash FixedString(64),
                action_ph_action_tot_msg_size_cells UInt64,
                action_ph_action_tot_msg_size_bits UInt64,
                bounce_ph Bool,
                bounce_ph_negfunds Bool,
                bounce_ph_nofunds Bool,
                bounce_ph_nofunds_msg_size_cells UInt64,
                bounce_ph_nofunds_msg_size_bits UInt64,
                bounce_ph_nofunds_req_fwd_fees UInt128,
                bounce_ph_ok Bool,
                bounce_ph_ok_msg_size_cells UInt64,
                bounce_ph_ok_msg_size_bits UInt64,
                bounce_ph_ok_msg_fees UInt128,
                bounce_ph_ok_fwd_fees UInt128
            )
            ENGINE = MergeTree
            ORDER BY (time, lt);
        """.trimIndent()
        ClickHouse(query)
    }
}
