{.experimental: "strictFuncs".}
## Unit tests for data block types.

import std/unittest

import clickhouse/column
import clickhouse/datablock

suite "block":
  test "empty block":
    let blk = empty_block()
    check blk.is_empty()
    check blk.num_columns == 0
    check blk.num_rows == 0

  test "type_string round-trip":
    let types = [
      "UInt8", "Int64", "Float32", "Float64", "String", "Bool",
      "Date", "Date32", "DateTime", "UUID", "IPv4", "IPv6", "Nothing",
      "FixedString(16)",
      "DateTime('UTC')",
      "DateTime64(3)",
      "Decimal32(2)",
      "Nullable(UInt32)",
      "Array(String)",
      "Array(Array(Int32))",
      "Tuple(UInt32, String)",
      "Map(String, UInt64)",
      "LowCardinality(String)",
    ]
    for ts in types:
      let t = parse_ch_type(ts)
      let reconstructed = t.type_string()
      let t2 = parse_ch_type(reconstructed)
      check t.kind == t2.kind
