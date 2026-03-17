# clickhouse

Pure Nim ClickHouse native binary protocol client. Supports queries, inserts, prepared data blocks, LZ4 compression, and server-side exceptions.

## Install

```
nimble install
```

## Usage

```nim
import clickhouse

var c = open(Host("localhost"), 9000)
let res = c.query(QueryText("SELECT 1 AS n"))
c.close()
```

## License

Proprietary
