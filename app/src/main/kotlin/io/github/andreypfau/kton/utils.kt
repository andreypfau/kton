package io.github.andreypfau.kton

import kotlinx.coroutines.channels.Channel
import org.ton.bitstring.BitString
import org.ton.bitstring.Bits256
import org.ton.block.*
import org.ton.cell.CellSlice

suspend fun <T> Channel<T>.receive(maxSize: Int): List<T> {
    val list = ArrayList<T>()
    list.add(receive())
    while (list.size < maxSize) {
        val value = tryReceive().getOrNull() ?: break
        list.add(value)
    }
    return list
}

fun StringBuilder.appendComma(value: CharSequence) =
    append(value).append(',')

fun StringBuilder.appendComma(value: Boolean) =
    append(value).append(',')

fun StringBuilder.appendComma(value: Int) =
    append(value).append(',')

fun StringBuilder.appendComma(value: Long) =
    append(value).append(',')

fun StringBuilder.appendComma(value: UInt) =
    append(value).append(',')

fun StringBuilder.appendComma(value: ULong) =
    append(value).append(',')

fun BitString.toBits256() = Bits256(this)

val TransactionDescr.aborted get() = when(this) {
    is TransOrd -> aborted
    is TransTickTock -> aborted
    is TransSplitPrepare -> aborted
    is TransMergePrepare -> aborted
    is TransMergeInstall -> aborted
    else -> false
}

val TransactionDescr.destroyed get() = when(this) {
    is TransOrd -> destroyed
    is TransTickTock -> destroyed
    is TransSplitPrepare -> destroyed
    is TransMergeInstall -> destroyed
    else -> false
}

val TransactionDescr.splitInfo get() = when(this) {
    is TransSplitPrepare -> splitInfo
    is TransSplitInstall -> splitInfo
    is TransMergePrepare -> splitInfo
    is TransMergeInstall -> splitInfo
    else -> null
}

val TransactionDescr.prepareTransaction get() = when(this) {
    is TransSplitInstall -> prepareTransaction
    is TransMergeInstall -> prepareTransaction
    else -> null
}

val TransactionDescr.storagePh get() = when(this) {
    is TransOrd -> storagePh.value
    is TransStorage -> storagePh
    is TransTickTock -> storagePh
    is TransSplitPrepare -> storagePh.value
    is TransMergePrepare -> storagePh
    is TransMergeInstall -> storagePh.value
    else -> null
}

val TransactionDescr.creditPh get() = when(this) {
    is TransOrd -> creditPh.value
    is TransMergeInstall -> creditPh.value
    else -> null
}

val TransactionDescr.computePh get() = when(this) {
    is TransOrd -> computePh
    is TransTickTock -> computePh
    is TransSplitPrepare -> computePh
    is TransMergeInstall -> computePh
    else -> null
}

val TransactionDescr.actionPh get() = when(this) {
    is TransOrd -> action.value?.value
    is TransTickTock -> action.value?.value
    is TransSplitPrepare -> action.value?.value
    is TransMergeInstall -> action.value // TODO: fix cell ref
    else -> null
}

val TransactionDescr.bouncePh get() = (this as? TransOrd)?.bounce?.value

val CommonMsgInfo.src get() = when(this) {
    is ExtInMsgInfo -> src
    is ExtOutMsgInfo -> src
    is IntMsgInfo -> src
}

val CommonMsgInfo.dest get() = when(this) {
    is ExtInMsgInfo -> dest
    is ExtOutMsgInfo -> dest
    is IntMsgInfo -> dest
}

val CommonMsgInfo.createdLt get() = when(this) {
    is ExtInMsgInfo -> 0uL
    is ExtOutMsgInfo -> createdLt
    is IntMsgInfo -> created_lt.toULong()
}

val CommonMsgInfo.createdAt get() = when(this) {
    is ExtInMsgInfo -> 0u
    is ExtOutMsgInfo -> createdAt
    is IntMsgInfo -> created_at.toUInt()
}

val MsgAddress.anycastRewritePfx get() =
    (this as? AddrStd)?.anycast?.value?.rewritePfx?.let {
        CellSlice(it).loadInt(it.size).toInt()
    }

val MsgAddress.address get() = when(this) {
    is AddrExtern -> externalAddress
    AddrNone -> BitString.empty()
    is AddrStd -> address
    is AddrVar -> address
}

val MsgAddress.workchain get() = when(this) {
    is AddrExtern -> 0
    AddrNone -> 0
    is AddrStd -> workchainId
    is AddrVar -> workchainId
}

val MsgAddress.type get() = when(this) {
    AddrNone -> 0
    is AddrExtern -> 1
    is AddrStd -> 2
    is AddrVar -> 3
}
