{.experimental: "strictFuncs".}
## Unit tests for column type parsing.

import std/unittest

import clickhouse/column

suite "type parser":
  test "simple types":
    check parse_ch_type("UInt8").kind == CHTypeKind.UInt8
    check parse_ch_type("UInt16").kind == CHTypeKind.UInt16
    check parse_ch_type("UInt32").kind == CHTypeKind.UInt32
    check parse_ch_type("UInt64").kind == CHTypeKind.UInt64
    check parse_ch_type("Int8").kind == CHTypeKind.Int8
    check parse_ch_type("Int16").kind == CHTypeKind.Int16
    check parse_ch_type("Int32").kind == CHTypeKind.Int32
    check parse_ch_type("Int64").kind == CHTypeKind.Int64
    check parse_ch_type("Float32").kind == CHTypeKind.Float32
    check parse_ch_type("Float64").kind == CHTypeKind.Float64
    check parse_ch_type("Bool").kind == CHTypeKind.Bool
    check parse_ch_type("String").kind == CHTypeKind.String
    check parse_ch_type("Date").kind == CHTypeKind.Date
    check parse_ch_type("Date32").kind == CHTypeKind.Date32
    check parse_ch_type("UUID").kind == CHTypeKind.UUID
    check parse_ch_type("IPv4").kind == CHTypeKind.IPv4
    check parse_ch_type("IPv6").kind == CHTypeKind.IPv6
    check parse_ch_type("Nothing").kind == CHTypeKind.Nothing

  test "FixedString":
    let t = parse_ch_type("FixedString(16)")
    check t.kind == CHTypeKind.FixedString
    check t.fixed_len == 16

  test "DateTime":
    let t1 = parse_ch_type("DateTime")
    check t1.kind == CHTypeKind.DateTime
    check t1.tz == ""
    let t2 = parse_ch_type("DateTime('UTC')")
    check t2.kind == CHTypeKind.DateTime
    check t2.tz == "UTC"

  test "DateTime64":
    let t = parse_ch_type("DateTime64(3, 'UTC')")
    check t.kind == CHTypeKind.DateTime64
    check t.precision == 3
    check t.tz64 == "UTC"

  test "Decimal":
    let t1 = parse_ch_type("Decimal32(2)")
    check t1.kind == CHTypeKind.Decimal32
    check t1.scale == 2
    let t2 = parse_ch_type("Decimal(18, 4)")
    check t2.kind == CHTypeKind.Decimal64
    check t2.scale == 4

  test "Nullable":
    let t = parse_ch_type("Nullable(UInt32)")
    check t.kind == CHTypeKind.Nullable
    check t.inner_type.kind == CHTypeKind.UInt32

  test "Array":
    let t = parse_ch_type("Array(String)")
    check t.kind == CHTypeKind.Array
    check t.elem_type.kind == CHTypeKind.String

  test "nested Array":
    let t = parse_ch_type("Array(Array(Int32))")
    check t.kind == CHTypeKind.Array
    check t.elem_type.kind == CHTypeKind.Array
    check t.elem_type.elem_type.kind == CHTypeKind.Int32

  test "Tuple":
    let t = parse_ch_type("Tuple(UInt32, String, Float64)")
    check t.kind == CHTypeKind.Tuple
    check t.elem_types.len == 3
    check t.elem_types[0].kind == CHTypeKind.UInt32
    check t.elem_types[1].kind == CHTypeKind.String
    check t.elem_types[2].kind == CHTypeKind.Float64

  test "Map":
    let t = parse_ch_type("Map(String, UInt64)")
    check t.kind == CHTypeKind.Map
    check t.key_type.kind == CHTypeKind.String
    check t.val_type.kind == CHTypeKind.UInt64

  test "LowCardinality":
    let t = parse_ch_type("LowCardinality(String)")
    check t.kind == CHTypeKind.LowCardinality
    check t.dict_type.kind == CHTypeKind.String

  test "complex nested":
    let t = parse_ch_type("Nullable(Array(Tuple(String, UInt32)))")
    check t.kind == CHTypeKind.Nullable
    check t.inner_type.kind == CHTypeKind.Array
    check t.inner_type.elem_type.kind == CHTypeKind.Tuple
    check t.inner_type.elem_type.elem_types.len == 2

  test "unknown type raises":
    var caught = false
    try:
      discard parse_ch_type("FooBar")
    except ValueError:
      caught = true
    check caught
