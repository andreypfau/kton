# kton
High-performance analytical TON blockchain indexer based on [ClickHouse](https://clickhouse.com/) and [TON Kotlin](https://github.com/andreypfau/ton-kotlin)

## Environment variables

- `KTON_START_FROM_SEQNO` - seqno of masterchain block where indexing starts from
  - value: UInt32
  - default: [26200000](https://explorer.toncoin.org/search?workchain=-1&shard=8000000000000000&seqno=26200000&lt=&utime=&roothash=&filehash=) (Sat Dec 31 2022 00:47:46 UTC+0)
- `KTON_DATA_DIR` - Directory for storage internal kton's data (cache, etc.)
    - value: File-path
    - default: `kton/data`
- `KTON_USE_BLOCK_CACHE` - Use preloaded blocks from `$KTON_DATA_DIR/block_data` stored in BOC files
    - value: `true`/`false`
    - default: `false`
