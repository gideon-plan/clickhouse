## ClickHouse column type encoding/decoding.
##
## All column data is column-major: all values for one column are written
## contiguously. See clickhouse_protocol.md section 13.

import std/net
import std/strutils
import std/parseutils

import basis/code/throw

import clickhouse/wire

standard_pragmas()

raises_error(io_err, [IOError], [ReadIOEffect, WriteIOEffect])
raises_error(parse_err, [ValueError], [])

# -----------------------------------------------------------------------
# Column type descriptor
# -----------------------------------------------------------------------

type
  CHTypeKind* = enum
    chkUInt8, chkUInt16, chkUInt32, chkUInt64, chkUInt128, chkUInt256
    chkInt8, chkInt16, chkInt32, chkInt64, chkInt128, chkInt256
    chkFloat32, chkFloat64
    chkBool
    chkString, chkFixedString
    chkDate, chkDate32, chkDateTime, chkDateTime64
    chkDecimal32, chkDecimal64, chkDecimal128, chkDecimal256
    chkUUID
    chkIPv4, chkIPv6
    chkEnum8, chkEnum16
    chkArray, chkNullable, chkTuple, chkMap
    chkLowCardinality
    chkNothing

  CHType* = ref object
    case kind*: CHTypeKind
    of chkFixedString:
      fixed_len*: int
    of chkDateTime:
      tz*: string
    of chkDateTime64:
      precision*: int
      tz64*: string
    of chkDecimal32, chkDecimal64, chkDecimal128, chkDecimal256:
      scale*: int
    of chkEnum8, chkEnum16:
      enum_values*: seq[(string, int16)]
    of chkArray:
      elem_type*: CHType
    of chkNullable:
      inner_type*: CHType
    of chkTuple:
      elem_types*: seq[CHType]
    of chkMap:
      key_type*: CHType
      val_type*: CHType
    of chkLowCardinality:
      dict_type*: CHType
    else:
      discard

# -----------------------------------------------------------------------
# Type string parser
# -----------------------------------------------------------------------

proc skip_ws(s: string; pos: int): int {.ok.} =
  var p = pos
  while p < s.len and s[p] == ' ': inc p
  p

proc parse_ch_type*(type_str: string): CHType {.parse_err.} =
  ## Parse a ClickHouse type string into a CHType descriptor.
  var s = type_str.strip()

  # Simple types
  case s
  of "UInt8": return CHType(kind: chkUInt8)
  of "UInt16": return CHType(kind: chkUInt16)
  of "UInt32": return CHType(kind: chkUInt32)
  of "UInt64": return CHType(kind: chkUInt64)
  of "UInt128": return CHType(kind: chkUInt128)
  of "UInt256": return CHType(kind: chkUInt256)
  of "Int8": return CHType(kind: chkInt8)
  of "Int16": return CHType(kind: chkInt16)
  of "Int32": return CHType(kind: chkInt32)
  of "Int64": return CHType(kind: chkInt64)
  of "Int128": return CHType(kind: chkInt128)
  of "Int256": return CHType(kind: chkInt256)
  of "Float32": return CHType(kind: chkFloat32)
  of "Float64": return CHType(kind: chkFloat64)
  of "Bool": return CHType(kind: chkBool)
  of "String": return CHType(kind: chkString)
  of "Date": return CHType(kind: chkDate)
  of "Date32": return CHType(kind: chkDate32)
  of "UUID": return CHType(kind: chkUUID)
  of "IPv4": return CHType(kind: chkIPv4)
  of "IPv6": return CHType(kind: chkIPv6)
  of "Nothing": return CHType(kind: chkNothing)
  else:
    discard

  # Parameterized types
  if s.starts_with("FixedString("):
    let inner = s[12 ..< s.len - 1]
    var n: int
    if parseInt(inner, n) == 0:
      raise newException(ValueError, "invalid FixedString length: " & inner)
    return CHType(kind: chkFixedString, fixed_len: n)

  if s.starts_with("DateTime64("):
    let inner = s[11 ..< s.len - 1]
    let parts = inner.split(",")
    var prec: int
    if parseInt(parts[0].strip(), prec) == 0:
      raise newException(ValueError, "invalid DateTime64 precision: " & parts[0])
    let tz_str = if parts.len > 1: parts[1].strip().strip(chars = {'\'', '"'}) else: ""
    return CHType(kind: chkDateTime64, precision: prec, tz64: tz_str)

  if s.starts_with("DateTime("):
    let inner = s[9 ..< s.len - 1].strip().strip(chars = {'\'', '"'})
    return CHType(kind: chkDateTime, tz: inner)

  if s == "DateTime":
    return CHType(kind: chkDateTime, tz: "")

  if s.starts_with("Decimal256("):
    let inner = s[11 ..< s.len - 1]
    var sc: int
    if parseInt(inner.strip(), sc) == 0:
      raise newException(ValueError, "invalid Decimal256 scale: " & inner)
    return CHType(kind: chkDecimal256, scale: sc)

  if s.starts_with("Decimal128("):
    let inner = s[11 ..< s.len - 1]
    var sc: int
    if parseInt(inner.strip(), sc) == 0:
      raise newException(ValueError, "invalid Decimal128 scale: " & inner)
    return CHType(kind: chkDecimal128, scale: sc)

  if s.starts_with("Decimal64("):
    let inner = s[10 ..< s.len - 1]
    var sc: int
    if parseInt(inner.strip(), sc) == 0:
      raise newException(ValueError, "invalid Decimal64 scale: " & inner)
    return CHType(kind: chkDecimal64, scale: sc)

  if s.starts_with("Decimal32("):
    let inner = s[10 ..< s.len - 1]
    var sc: int
    if parseInt(inner.strip(), sc) == 0:
      raise newException(ValueError, "invalid Decimal32 scale: " & inner)
    return CHType(kind: chkDecimal32, scale: sc)

  if s.starts_with("Decimal("):
    let inner = s[8 ..< s.len - 1]
    let parts = inner.split(",")
    if parts.len != 2:
      raise newException(ValueError, "invalid Decimal: " & inner)
    var prec, sc: int
    if parseInt(parts[0].strip(), prec) == 0:
      raise newException(ValueError, "invalid Decimal precision: " & parts[0])
    if parseInt(parts[1].strip(), sc) == 0:
      raise newException(ValueError, "invalid Decimal scale: " & parts[1])
    if prec <= 9: return CHType(kind: chkDecimal32, scale: sc)
    elif prec <= 18: return CHType(kind: chkDecimal64, scale: sc)
    elif prec <= 38: return CHType(kind: chkDecimal128, scale: sc)
    else: return CHType(kind: chkDecimal256, scale: sc)

  # Nested parameterized types -- need bracket matching
  if s.starts_with("Nullable("):
    let inner = s[9 ..< s.len - 1]
    return CHType(kind: chkNullable, inner_type: parse_ch_type(inner))

  if s.starts_with("Array("):
    let inner = s[6 ..< s.len - 1]
    return CHType(kind: chkArray, elem_type: parse_ch_type(inner))

  if s.starts_with("LowCardinality("):
    let inner = s[15 ..< s.len - 1]
    return CHType(kind: chkLowCardinality, dict_type: parse_ch_type(inner))

  if s.starts_with("Map("):
    let inner = s[4 ..< s.len - 1]
    # Split on first comma not inside parentheses
    var depth = 0
    var split_pos = -1
    for i in 0 ..< inner.len:
      if inner[i] == '(': inc depth
      elif inner[i] == ')': dec depth
      elif inner[i] == ',' and depth == 0:
        split_pos = i
        break
    if split_pos < 0:
      raise newException(ValueError, "invalid Map type: " & s)
    let kt = parse_ch_type(inner[0 ..< split_pos].strip())
    let vt = parse_ch_type(inner[split_pos + 1 ..< inner.len].strip())
    return CHType(kind: chkMap, key_type: kt, val_type: vt)

  if s.starts_with("Tuple("):
    let inner = s[6 ..< s.len - 1]
    var elems: seq[CHType] = @[]
    var depth = 0
    var start = 0
    for i in 0 ..< inner.len:
      if inner[i] == '(': inc depth
      elif inner[i] == ')': dec depth
      elif inner[i] == ',' and depth == 0:
        elems.add(parse_ch_type(inner[start ..< i].strip()))
        start = i + 1
    elems.add(parse_ch_type(inner[start ..< inner.len].strip()))
    return CHType(kind: chkTuple, elem_types: elems)

  if s.starts_with("Enum8(") or s.starts_with("Enum16("):
    let is8 = s.starts_with("Enum8(")
    let prefix_len = if is8: 6 else: 7
    let inner = s[prefix_len ..< s.len - 1]
    var values: seq[(string, int16)] = @[]
    for pair in inner.split(","):
      let p = pair.strip()
      let eq_pos = p.rfind('=')
      if eq_pos < 0:
        raise newException(ValueError, "invalid Enum entry: " & p)
      let name = p[0 ..< eq_pos].strip().strip(chars = {'\'', '"'})
      var val: int
      if parseInt(p[eq_pos + 1 ..< p.len].strip(), val) == 0:
        raise newException(ValueError, "invalid Enum value: " & p)
      values.add((name, int16(val)))
    if is8:
      return CHType(kind: chkEnum8, enum_values: values)
    else:
      return CHType(kind: chkEnum16, enum_values: values)

  raise newException(ValueError, "unknown ClickHouse type: " & s)

# -----------------------------------------------------------------------
# Column data -- generic value container
# -----------------------------------------------------------------------

type
  CHValue* = object
    case kind*: CHTypeKind
    of chkUInt8, chkBool: u8*: uint8
    of chkUInt16: u16*: uint16
    of chkUInt32, chkIPv4: u32*: uint32
    of chkUInt64: u64*: uint64
    of chkUInt128: u128_hi*, u128_lo*: uint64
    of chkUInt256: u256*: array[4, uint64]
    of chkInt8: i8*: int8
    of chkInt16, chkEnum8, chkEnum16: i16*: int16
    of chkInt32, chkDate32: i32*: int32
    of chkInt64, chkDateTime64: i64*: int64
    of chkInt128, chkDecimal128: i128_hi*, i128_lo*: uint64
    of chkInt256, chkDecimal256: i256*: array[4, uint64]
    of chkFloat32: f32*: float32
    of chkFloat64: f64*: float64
    of chkString: str*: string
    of chkFixedString: fixed_str*: seq[uint8]
    of chkDate: date_days*: uint16
    of chkDateTime: datetime_ts*: uint32
    of chkDecimal32: d32*: int32
    of chkDecimal64: d64*: int64
    of chkUUID: uuid_hi*, uuid_lo*: uint64
    of chkIPv6: ipv6*: array[16, uint8]
    of chkArray: arr*: seq[CHValue]
    of chkNullable:
      is_null*: bool
      nullable_val*: ref CHValue
    of chkTuple: tup*: seq[CHValue]
    of chkMap: map_keys*, map_vals*: seq[CHValue]
    of chkLowCardinality: lc_val*: ref CHValue
    of chkNothing: discard

  CHColumn* = object
    name*: string
    col_type*: CHType
    data*: seq[CHValue]

# -----------------------------------------------------------------------
# Read column data from socket
# -----------------------------------------------------------------------

proc read_column_data*(sock: Socket; col_type: CHType; num_rows: int): seq[CHValue] {.io_err.} =
  ## Read num_rows of column data for the given type.
  result = newSeq[CHValue](num_rows)
  case col_type.kind
  of chkUInt8, chkBool:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkUInt8, u8: sock.read_uint8())
  of chkUInt16:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkUInt16, u16: sock.read_uint16())
  of chkUInt32:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkUInt32, u32: sock.read_uint32())
  of chkUInt64:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkUInt64, u64: sock.read_uint64())
  of chkUInt128:
    for i in 0 ..< num_rows:
      let lo = sock.read_uint64()
      let hi = sock.read_uint64()
      result[i] = CHValue(kind: chkUInt128, u128_lo: lo, u128_hi: hi)
  of chkUInt256:
    for i in 0 ..< num_rows:
      var arr: array[4, uint64]
      for j in 0 ..< 4:
        arr[j] = sock.read_uint64()
      result[i] = CHValue(kind: chkUInt256, u256: arr)
  of chkInt8:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkInt8, i8: sock.read_int8())
  of chkInt16:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkInt16, i16: sock.read_int16())
  of chkInt32:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkInt32, i32: sock.read_int32())
  of chkInt64:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkInt64, i64: sock.read_int64())
  of chkInt128:
    for i in 0 ..< num_rows:
      let lo = sock.read_uint64()
      let hi = sock.read_uint64()
      result[i] = CHValue(kind: chkInt128, i128_lo: lo, i128_hi: hi)
  of chkInt256:
    for i in 0 ..< num_rows:
      var arr: array[4, uint64]
      for j in 0 ..< 4:
        arr[j] = sock.read_uint64()
      result[i] = CHValue(kind: chkInt256, i256: arr)
  of chkFloat32:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkFloat32, f32: sock.read_float32())
  of chkFloat64:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkFloat64, f64: sock.read_float64())
  of chkString:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkString, str: sock.read_string())
  of chkFixedString:
    for i in 0 ..< num_rows:
      var buf = newSeq[uint8](col_type.fixed_len)
      if col_type.fixed_len > 0:
        sock.read_raw(addr buf[0], col_type.fixed_len)
      result[i] = CHValue(kind: chkFixedString, fixed_str: buf)
  of chkDate:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkDate, date_days: sock.read_uint16())
  of chkDate32:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkDate32, i32: sock.read_int32())
  of chkDateTime:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkDateTime, datetime_ts: sock.read_uint32())
  of chkDateTime64:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkDateTime64, i64: sock.read_int64())
  of chkDecimal32:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkDecimal32, d32: sock.read_int32())
  of chkDecimal64:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkDecimal64, d64: sock.read_int64())
  of chkDecimal128:
    for i in 0 ..< num_rows:
      let lo = sock.read_uint64()
      let hi = sock.read_uint64()
      result[i] = CHValue(kind: chkDecimal128, i128_lo: lo, i128_hi: hi)
  of chkDecimal256:
    for i in 0 ..< num_rows:
      var arr: array[4, uint64]
      for j in 0 ..< 4:
        arr[j] = sock.read_uint64()
      result[i] = CHValue(kind: chkDecimal256, i256: arr)
  of chkUUID:
    for i in 0 ..< num_rows:
      let hi = sock.read_uint64()
      let lo = sock.read_uint64()
      result[i] = CHValue(kind: chkUUID, uuid_hi: hi, uuid_lo: lo)
  of chkIPv4:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkIPv4, u32: sock.read_uint32())
  of chkIPv6:
    for i in 0 ..< num_rows:
      var addr_bytes: array[16, uint8]
      sock.read_raw(addr addr_bytes[0], 16)
      result[i] = CHValue(kind: chkIPv6, ipv6: addr_bytes)
  of chkEnum8:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkEnum8, i16: int16(sock.read_int8()))
  of chkEnum16:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkEnum16, i16: sock.read_int16())
  of chkNullable:
    # Read null bitmap
    var nulls = newSeq[uint8](num_rows)
    for i in 0 ..< num_rows:
      nulls[i] = sock.read_uint8()
    # Read nested data
    let inner = sock.read_column_data(col_type.inner_type, num_rows)
    for i in 0 ..< num_rows:
      if nulls[i] != 0:
        result[i] = CHValue(kind: chkNullable, is_null: true, nullable_val: nil)
      else:
        var v = new(CHValue)
        v[] = inner[i]
        result[i] = CHValue(kind: chkNullable, is_null: false, nullable_val: v)
  of chkArray:
    # Read offsets
    var offsets = newSeq[uint64](num_rows)
    for i in 0 ..< num_rows:
      offsets[i] = sock.read_uint64()
    # Total elements = last offset
    let total = if num_rows > 0: int(offsets[num_rows - 1]) else: 0
    # Read all elements
    let all_elems = sock.read_column_data(col_type.elem_type, total)
    # Split by offsets
    var prev: int = 0
    for i in 0 ..< num_rows:
      let cur = int(offsets[i])
      result[i] = CHValue(kind: chkArray, arr: all_elems[prev ..< cur])
      prev = cur
  of chkTuple:
    # Each element type column is read sequentially
    var cols: seq[seq[CHValue]] = @[]
    for t in col_type.elem_types:
      cols.add(sock.read_column_data(t, num_rows))
    for i in 0 ..< num_rows:
      var tup: seq[CHValue] = @[]
      for c in cols:
        tup.add(c[i])
      result[i] = CHValue(kind: chkTuple, tup: tup)
  of chkMap:
    # Map is encoded as Array(Tuple(K, V))
    # Read offsets
    var offsets = newSeq[uint64](num_rows)
    for i in 0 ..< num_rows:
      offsets[i] = sock.read_uint64()
    let total = if num_rows > 0: int(offsets[num_rows - 1]) else: 0
    # Read keys and values
    let keys = sock.read_column_data(col_type.key_type, total)
    let vals = sock.read_column_data(col_type.val_type, total)
    var prev: int = 0
    for i in 0 ..< num_rows:
      let cur = int(offsets[i])
      result[i] = CHValue(kind: chkMap,
                           map_keys: keys[prev ..< cur],
                           map_vals: vals[prev ..< cur])
      prev = cur
  of chkLowCardinality:
    # Read serialization version (int64, always 1)
    discard sock.read_int64()
    # Read serialization type
    let ser_type = sock.read_int64()
    let key_type_idx = ser_type and 0x0F
    # Read dictionary
    let dict_size = sock.read_int64()
    let dict = sock.read_column_data(col_type.dict_type, int(dict_size))
    # Read keys
    let num_keys = sock.read_int64()
    var indices = newSeq[int](int(num_keys))
    case key_type_idx
    of 0: # UInt8
      for i in 0 ..< int(num_keys):
        indices[i] = int(sock.read_uint8())
    of 1: # UInt16
      for i in 0 ..< int(num_keys):
        indices[i] = int(sock.read_uint16())
    of 2: # UInt32
      for i in 0 ..< int(num_keys):
        indices[i] = int(sock.read_uint32())
    of 3: # UInt64
      for i in 0 ..< int(num_keys):
        indices[i] = int(sock.read_uint64())
    else:
      raise newException(IOError, "invalid LowCardinality key type: " & $key_type_idx)
    for i in 0 ..< int(num_keys):
      var v = new(CHValue)
      v[] = dict[indices[i]]
      result[i] = CHValue(kind: chkLowCardinality, lc_val: v)
  of chkNothing:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: chkNothing)

# -----------------------------------------------------------------------
# Write column data to socket
# -----------------------------------------------------------------------

proc write_column_data*(sock: Socket; col_type: CHType; data: openArray[CHValue]) {.io_err.} =
  ## Write column data for the given type.
  case col_type.kind
  of chkUInt8, chkBool:
    for v in data: sock.write_uint8(v.u8)
  of chkUInt16:
    for v in data: sock.write_uint16(v.u16)
  of chkUInt32:
    for v in data: sock.write_uint32(v.u32)
  of chkUInt64:
    for v in data: sock.write_uint64(v.u64)
  of chkUInt128:
    for v in data:
      sock.write_uint64(v.u128_lo)
      sock.write_uint64(v.u128_hi)
  of chkUInt256:
    for v in data:
      for j in 0 ..< 4:
        sock.write_uint64(v.u256[j])
  of chkInt8:
    for v in data: sock.write_int8(v.i8)
  of chkInt16:
    for v in data: sock.write_int16(v.i16)
  of chkInt32:
    for v in data: sock.write_int32(v.i32)
  of chkInt64:
    for v in data: sock.write_int64(v.i64)
  of chkInt128:
    for v in data:
      sock.write_uint64(v.i128_lo)
      sock.write_uint64(v.i128_hi)
  of chkInt256:
    for v in data:
      for j in 0 ..< 4:
        sock.write_uint64(v.i256[j])
  of chkFloat32:
    for v in data: sock.write_float32(v.f32)
  of chkFloat64:
    for v in data: sock.write_float64(v.f64)
  of chkString:
    for v in data: sock.write_string(v.str)
  of chkFixedString:
    for v in data:
      if v.fixed_str.len > 0:
        sock.write_raw(unsafeAddr v.fixed_str[0], col_type.fixed_len)
      else:
        var zeros = newSeq[uint8](col_type.fixed_len)
        sock.write_raw(addr zeros[0], col_type.fixed_len)
  of chkDate:
    for v in data: sock.write_uint16(v.date_days)
  of chkDate32:
    for v in data: sock.write_int32(v.i32)
  of chkDateTime:
    for v in data: sock.write_uint32(v.datetime_ts)
  of chkDateTime64:
    for v in data: sock.write_int64(v.i64)
  of chkDecimal32:
    for v in data: sock.write_int32(v.d32)
  of chkDecimal64:
    for v in data: sock.write_int64(v.d64)
  of chkDecimal128:
    for v in data:
      sock.write_uint64(v.i128_lo)
      sock.write_uint64(v.i128_hi)
  of chkDecimal256:
    for v in data:
      for j in 0 ..< 4:
        sock.write_uint64(v.i256[j])
  of chkUUID:
    for v in data:
      sock.write_uint64(v.uuid_hi)
      sock.write_uint64(v.uuid_lo)
  of chkIPv4:
    for v in data: sock.write_uint32(v.u32)
  of chkIPv6:
    for v in data:
      sock.write_raw(unsafeAddr v.ipv6[0], 16)
  of chkEnum8:
    for v in data: sock.write_int8(int8(v.i16))
  of chkEnum16:
    for v in data: sock.write_int16(v.i16)
  of chkNullable:
    # Null bitmap
    for v in data:
      sock.write_uint8(if v.is_null: 1'u8 else: 0'u8)
    # Nested data
    var inner_data = newSeq[CHValue](data.len)
    for i in 0 ..< data.len:
      if data[i].is_null:
        inner_data[i] = CHValue(kind: col_type.inner_type.kind)
      else:
        inner_data[i] = data[i].nullable_val[]
    sock.write_column_data(col_type.inner_type, inner_data)
  of chkArray:
    # Write offsets
    var offset: uint64 = 0
    for v in data:
      offset += uint64(v.arr.len)
      sock.write_uint64(offset)
    # Write all elements
    var all_elems: seq[CHValue] = @[]
    for v in data:
      all_elems.add(v.arr)
    sock.write_column_data(col_type.elem_type, all_elems)
  of chkTuple:
    for idx, t in col_type.elem_types:
      var col_data = newSeq[CHValue](data.len)
      for i in 0 ..< data.len:
        col_data[i] = data[i].tup[idx]
      sock.write_column_data(t, col_data)
  of chkMap:
    # Write offsets
    var offset: uint64 = 0
    for v in data:
      offset += uint64(v.map_keys.len)
      sock.write_uint64(offset)
    # Write keys
    var all_keys: seq[CHValue] = @[]
    var all_vals: seq[CHValue] = @[]
    for v in data:
      all_keys.add(v.map_keys)
      all_vals.add(v.map_vals)
    sock.write_column_data(col_type.key_type, all_keys)
    sock.write_column_data(col_type.val_type, all_vals)
  of chkLowCardinality:
    # Write as full dictionary per block (simple approach)
    sock.write_int64(1) # serialization version
    # Build dictionary
    var dict: seq[CHValue] = @[]
    var index_map: seq[int] = @[]
    for v in data:
      var found = -1
      for j in 0 ..< dict.len:
        if true: # simplified -- always add to dict
          discard
      dict.add(v.lc_val[])
      index_map.add(dict.len - 1)
    # Determine key type
    let key_type_idx: int64 = if dict.len <= 256: 0
                               elif dict.len <= 65536: 1
                               else: 2
    let ser_type = int64(0x600) or key_type_idx  # has_additional_keys | need_update_dictionary
    sock.write_int64(ser_type)
    sock.write_int64(int64(dict.len))
    sock.write_column_data(col_type.dict_type, dict)
    sock.write_int64(int64(data.len))
    case key_type_idx
    of 0:
      for idx in index_map: sock.write_uint8(uint8(idx))
    of 1:
      for idx in index_map: sock.write_uint16(uint16(idx))
    of 2:
      for idx in index_map: sock.write_uint32(uint32(idx))
    else:
      for idx in index_map: sock.write_uint64(uint64(idx))
  of chkNothing:
    discard
