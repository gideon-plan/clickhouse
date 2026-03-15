## Unit tests for column type parsing.

import std/unittest

import clickhouse/column

suite "type parser":
  test "simple types":
    check parse_ch_type("UInt8").kind == chkUInt8
    check parse_ch_type("UInt16").kind == chkUInt16
    check parse_ch_type("UInt32").kind == chkUInt32
    check parse_ch_type("UInt64").kind == chkUInt64
    check parse_ch_type("Int8").kind == chkInt8
    check parse_ch_type("Int16").kind == chkInt16
    check parse_ch_type("Int32").kind == chkInt32
    check parse_ch_type("Int64").kind == chkInt64
    check parse_ch_type("Float32").kind == chkFloat32
    check parse_ch_type("Float64").kind == chkFloat64
    check parse_ch_type("Bool").kind == chkBool
    check parse_ch_type("String").kind == chkString
    check parse_ch_type("Date").kind == chkDate
    check parse_ch_type("Date32").kind == chkDate32
    check parse_ch_type("UUID").kind == chkUUID
    check parse_ch_type("IPv4").kind == chkIPv4
    check parse_ch_type("IPv6").kind == chkIPv6
    check parse_ch_type("Nothing").kind == chkNothing

  test "FixedString":
    let t = parse_ch_type("FixedString(16)")
    check t.kind == chkFixedString
    check t.fixed_len == 16

  test "DateTime":
    let t1 = parse_ch_type("DateTime")
    check t1.kind == chkDateTime
    check t1.tz == ""
    let t2 = parse_ch_type("DateTime('UTC')")
    check t2.kind == chkDateTime
    check t2.tz == "UTC"

  test "DateTime64":
    let t = parse_ch_type("DateTime64(3, 'UTC')")
    check t.kind == chkDateTime64
    check t.precision == 3
    check t.tz64 == "UTC"

  test "Decimal":
    let t1 = parse_ch_type("Decimal32(2)")
    check t1.kind == chkDecimal32
    check t1.scale == 2
    let t2 = parse_ch_type("Decimal(18, 4)")
    check t2.kind == chkDecimal64
    check t2.scale == 4

  test "Nullable":
    let t = parse_ch_type("Nullable(UInt32)")
    check t.kind == chkNullable
    check t.inner_type.kind == chkUInt32

  test "Array":
    let t = parse_ch_type("Array(String)")
    check t.kind == chkArray
    check t.elem_type.kind == chkString

  test "nested Array":
    let t = parse_ch_type("Array(Array(Int32))")
    check t.kind == chkArray
    check t.elem_type.kind == chkArray
    check t.elem_type.elem_type.kind == chkInt32

  test "Tuple":
    let t = parse_ch_type("Tuple(UInt32, String, Float64)")
    check t.kind == chkTuple
    check t.elem_types.len == 3
    check t.elem_types[0].kind == chkUInt32
    check t.elem_types[1].kind == chkString
    check t.elem_types[2].kind == chkFloat64

  test "Map":
    let t = parse_ch_type("Map(String, UInt64)")
    check t.kind == chkMap
    check t.key_type.kind == chkString
    check t.val_type.kind == chkUInt64

  test "LowCardinality":
    let t = parse_ch_type("LowCardinality(String)")
    check t.kind == chkLowCardinality
    check t.dict_type.kind == chkString

  test "complex nested":
    let t = parse_ch_type("Nullable(Array(Tuple(String, UInt32)))")
    check t.kind == chkNullable
    check t.inner_type.kind == chkArray
    check t.inner_type.elem_type.kind == chkTuple
    check t.inner_type.elem_type.elem_types.len == 2

  test "unknown type raises":
    var caught = false
    try:
      discard parse_ch_type("FooBar")
    except ValueError:
      caught = true
    check caught
