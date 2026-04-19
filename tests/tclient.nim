{.experimental: "strictFuncs".}
## Integration tests -- requires a running ClickHouse server on localhost:19000.

import std/unittest

import clickhouse/client
import clickhouse/column
import clickhouse/datablock
import clickhouse/protocol
import clickhouse/error

const TestPort = 19000'u16

suite "client integration":
  test "connect and ping":
    var c = open(Host("127.0.0.1"), TestPort)
    defer: c.close()
    check c.ping()

  test "execute DDL":
    var c = open(Host("127.0.0.1"), TestPort)
    defer: c.close()
    c.execute(QueryText("DROP TABLE IF EXISTS test_nim"))
    c.execute(QueryText("""
      CREATE TABLE test_nim (
        id UInt32,
        name String,
        value Float64
      ) ENGINE = MergeTree() ORDER BY id
    """))
    c.execute(QueryText("DROP TABLE test_nim"))

  test "query select 1":
    var c = open(Host("127.0.0.1"), TestPort)
    defer: c.close()
    let res = c.query(QueryText("SELECT 1 AS n"))
    check res.num_rows == 1
    check res.columns.len == 1
    check res.columns[0].name == "n"
    check res.columns[0].data[0].u8 == 1

  test "query multiple rows":
    var c = open(Host("127.0.0.1"), TestPort)
    defer: c.close()
    let res = c.query(QueryText("SELECT number FROM system.numbers LIMIT 10"))
    check res.num_rows == 10
    check res.columns[0].data[0].u64 == 0
    check res.columns[0].data[9].u64 == 9

  test "insert and select round-trip":
    var c = open(Host("127.0.0.1"), TestPort)
    defer: c.close()
    c.execute(QueryText("DROP TABLE IF EXISTS test_rt"))
    c.execute(QueryText("""
      CREATE TABLE test_rt (
        id UInt32,
        name String,
        val Float64
      ) ENGINE = MergeTree() ORDER BY id
    """))
    # Build data block
    let col_id = CHColumn(
      name: "id",
      col_type: CHKind(kind: CHTypeKind.UInt32),
      data: @[
        CHValue(kind: CHTypeKind.UInt32, u32: 1),
        CHValue(kind: CHTypeKind.UInt32, u32: 2),
        CHValue(kind: CHTypeKind.UInt32, u32: 3),
      ]
    )
    let col_name = CHColumn(
      name: "name",
      col_type: CHKind(kind: CHTypeKind.String),
      data: @[
        CHValue(kind: CHTypeKind.String, str: "alpha"),
        CHValue(kind: CHTypeKind.String, str: "beta"),
        CHValue(kind: CHTypeKind.String, str: "gamma"),
      ]
    )
    let col_val = CHColumn(
      name: "val",
      col_type: CHKind(kind: CHTypeKind.Float64),
      data: @[
        CHValue(kind: CHTypeKind.Float64, f64: 1.1),
        CHValue(kind: CHTypeKind.Float64, f64: 2.2),
        CHValue(kind: CHTypeKind.Float64, f64: 3.3),
      ]
    )
    let blk = CHBlock(
      info: BlockInfo(is_overflows: 0, bucket_num: -1),
      num_columns: 3,
      num_rows: 3,
      columns: @[col_id, col_name, col_val],
    )
    c.insert(QueryText("INSERT INTO test_rt (id, name, val) VALUES"), blk)
    let res = c.query(QueryText("SELECT id, name, val FROM test_rt ORDER BY id"))
    check res.num_rows == 3
    check res.columns[0].data[0].u32 == 1
    check res.columns[0].data[2].u32 == 3
    check res.columns[1].data[1].str == "beta"
    check res.columns[2].data[2].f64 == 3.3
    c.execute(QueryText("DROP TABLE test_rt"))

  test "server exception handling":
    var c = open(Host("127.0.0.1"), TestPort)
    defer: c.close()
    var caught = false
    try:
      c.execute(QueryText("SELECT * FROM nonexistent_table_xyz"))
    except CHError:
      caught = true
    check caught

  test "integer types round-trip":
    var c = open(Host("127.0.0.1"), TestPort)
    defer: c.close()
    let res = c.query(QueryText("SELECT toUInt8(255), toInt32(-42), toUInt64(18446744073709551615), toFloat32(3.14)"))
    check res.num_rows == 1
    check res.columns[0].data[0].u8 == 255
    check res.columns[1].data[0].i32 == -42
    check res.columns[2].data[0].u64 == uint64.high
