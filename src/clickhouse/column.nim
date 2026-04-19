{.experimental: "strictFuncs".}
## ClickHouse column type encoding/decoding.
##
## All column data is column-major: all values for one column are written
## contiguously. See clickhouse_protocol.md section 13.

import std/[net, strutils, parseutils]

import basis/code/throw

import clickhouse/wire

standard_pragmas()

raises_error(io_err, [IOError], [ReadIOEffect, WriteIOEffect])
raises_error(parse_err, [ValueError], [])

#=======================================================================================================================
#== COLUMN TYPE DESCRIPTOR =============================================================================================
#=======================================================================================================================

type
  CHTypeKind* {.pure.} = enum
    UInt8, UInt16, UInt32, UInt64, UInt128, UInt256
    Int8, Int16, Int32, Int64, Int128, Int256
    Float32, Float64
    Bool
    String, FixedString
    Date, Date32, DateTime, DateTime64
    Decimal32, Decimal64, Decimal128, Decimal256
    UUID
    IPv4, IPv6
    Enum8, Enum16
    Array, Nullable, Tuple, Map
    LowCardinality
    Nothing

  CHKind* = ref object
    case kind*: CHTypeKind
    of CHTypeKind.FixedString:
      fixed_len*: int
    of CHTypeKind.DateTime:
      tz*: string
    of CHTypeKind.DateTime64:
      precision*: int
      tz64*: string
    of CHTypeKind.Decimal32, CHTypeKind.Decimal64, CHTypeKind.Decimal128, CHTypeKind.Decimal256:
      scale*: int
    of CHTypeKind.Enum8, CHTypeKind.Enum16:
      enum_values*: seq[(string, int16)]
    of CHTypeKind.Array:
      elem_type*: CHKind
    of CHTypeKind.Nullable:
      inner_type*: CHKind
    of CHTypeKind.Tuple:
      elem_types*: seq[CHKind]
    of CHTypeKind.Map:
      key_type*: CHKind
      val_type*: CHKind
    of CHTypeKind.LowCardinality:
      dict_type*: CHKind
    else:
      discard

#=======================================================================================================================
#== TYPE STRING PARSER =================================================================================================
#=======================================================================================================================

proc skip_ws(s: string; pos: int): int {.ok.} =
  var p = pos
  while p < s.len and s[p] == ' ': inc p
  p

proc parse_ch_type*(type_str: string): CHKind {.parse_err.} =
  ## Parse a ClickHouse type string into a CHKind descriptor.
  var s = type_str.strip()

  # Simple types
  case s
  of "UInt8": return CHKind(kind: CHTypeKind.UInt8)
  of "UInt16": return CHKind(kind: CHTypeKind.UInt16)
  of "UInt32": return CHKind(kind: CHTypeKind.UInt32)
  of "UInt64": return CHKind(kind: CHTypeKind.UInt64)
  of "UInt128": return CHKind(kind: CHTypeKind.UInt128)
  of "UInt256": return CHKind(kind: CHTypeKind.UInt256)
  of "Int8": return CHKind(kind: CHTypeKind.Int8)
  of "Int16": return CHKind(kind: CHTypeKind.Int16)
  of "Int32": return CHKind(kind: CHTypeKind.Int32)
  of "Int64": return CHKind(kind: CHTypeKind.Int64)
  of "Int128": return CHKind(kind: CHTypeKind.Int128)
  of "Int256": return CHKind(kind: CHTypeKind.Int256)
  of "Float32": return CHKind(kind: CHTypeKind.Float32)
  of "Float64": return CHKind(kind: CHTypeKind.Float64)
  of "Bool": return CHKind(kind: CHTypeKind.Bool)
  of "String": return CHKind(kind: CHTypeKind.String)
  of "Date": return CHKind(kind: CHTypeKind.Date)
  of "Date32": return CHKind(kind: CHTypeKind.Date32)
  of "UUID": return CHKind(kind: CHTypeKind.UUID)
  of "IPv4": return CHKind(kind: CHTypeKind.IPv4)
  of "IPv6": return CHKind(kind: CHTypeKind.IPv6)
  of "Nothing": return CHKind(kind: CHTypeKind.Nothing)
  else:
    discard

  # Parameterized types
  if s.starts_with("FixedString("):
    let inner = s[12 ..< s.len - 1]
    var n: int
    if parseInt(inner, n) == 0:
      raise newException(ValueError, "invalid FixedString length: " & inner)
    return CHKind(kind: CHTypeKind.FixedString, fixed_len: n)

  if s.starts_with("DateTime64("):
    let inner = s[11 ..< s.len - 1]
    let parts = inner.split(",")
    var prec: int
    if parseInt(parts[0].strip(), prec) == 0:
      raise newException(ValueError, "invalid DateTime64 precision: " & parts[0])
    let tz_str = if parts.len > 1: parts[1].strip().strip(chars = {'\'', '"'}) else: ""
    return CHKind(kind: CHTypeKind.DateTime64, precision: prec, tz64: tz_str)

  if s.starts_with("DateTime("):
    let inner = s[9 ..< s.len - 1].strip().strip(chars = {'\'', '"'})
    return CHKind(kind: CHTypeKind.DateTime, tz: inner)

  if s == "DateTime":
    return CHKind(kind: CHTypeKind.DateTime, tz: "")

  if s.starts_with("Decimal256("):
    let inner = s[11 ..< s.len - 1]
    var sc: int
    if parseInt(inner.strip(), sc) == 0:
      raise newException(ValueError, "invalid Decimal256 scale: " & inner)
    return CHKind(kind: CHTypeKind.Decimal256, scale: sc)

  if s.starts_with("Decimal128("):
    let inner = s[11 ..< s.len - 1]
    var sc: int
    if parseInt(inner.strip(), sc) == 0:
      raise newException(ValueError, "invalid Decimal128 scale: " & inner)
    return CHKind(kind: CHTypeKind.Decimal128, scale: sc)

  if s.starts_with("Decimal64("):
    let inner = s[10 ..< s.len - 1]
    var sc: int
    if parseInt(inner.strip(), sc) == 0:
      raise newException(ValueError, "invalid Decimal64 scale: " & inner)
    return CHKind(kind: CHTypeKind.Decimal64, scale: sc)

  if s.starts_with("Decimal32("):
    let inner = s[10 ..< s.len - 1]
    var sc: int
    if parseInt(inner.strip(), sc) == 0:
      raise newException(ValueError, "invalid Decimal32 scale: " & inner)
    return CHKind(kind: CHTypeKind.Decimal32, scale: sc)

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
    if prec <= 9: return CHKind(kind: CHTypeKind.Decimal32, scale: sc)
    elif prec <= 18: return CHKind(kind: CHTypeKind.Decimal64, scale: sc)
    elif prec <= 38: return CHKind(kind: CHTypeKind.Decimal128, scale: sc)
    else: return CHKind(kind: CHTypeKind.Decimal256, scale: sc)

  # Nested parameterized types -- need bracket matching
  if s.starts_with("Nullable("):
    let inner = s[9 ..< s.len - 1]
    return CHKind(kind: CHTypeKind.Nullable, inner_type: parse_ch_type(inner))

  if s.starts_with("Array("):
    let inner = s[6 ..< s.len - 1]
    return CHKind(kind: CHTypeKind.Array, elem_type: parse_ch_type(inner))

  if s.starts_with("LowCardinality("):
    let inner = s[15 ..< s.len - 1]
    return CHKind(kind: CHTypeKind.LowCardinality, dict_type: parse_ch_type(inner))

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
    return CHKind(kind: CHTypeKind.Map, key_type: kt, val_type: vt)

  if s.starts_with("Tuple("):
    let inner = s[6 ..< s.len - 1]
    var elems: seq[CHKind] = @[]
    var depth = 0
    var start = 0
    for i in 0 ..< inner.len:
      if inner[i] == '(': inc depth
      elif inner[i] == ')': dec depth
      elif inner[i] == ',' and depth == 0:
        elems.add(parse_ch_type(inner[start ..< i].strip()))
        start = i + 1
    elems.add(parse_ch_type(inner[start ..< inner.len].strip()))
    return CHKind(kind: CHTypeKind.Tuple, elem_types: elems)

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
      return CHKind(kind: CHTypeKind.Enum8, enum_values: values)
    else:
      return CHKind(kind: CHTypeKind.Enum16, enum_values: values)

  raise newException(ValueError, "unknown ClickHouse type: " & s)

#=======================================================================================================================
#== COLUMN DATA -- GENERIC VALUE CONTAINER =============================================================================
#=======================================================================================================================

type
  CHValue* = object
    case kind*: CHTypeKind
    of CHTypeKind.UInt8, CHTypeKind.Bool: u8*: uint8
    of CHTypeKind.UInt16: u16*: uint16
    of CHTypeKind.UInt32, CHTypeKind.IPv4: u32*: uint32
    of CHTypeKind.UInt64: u64*: uint64
    of CHTypeKind.UInt128: u128_hi*, u128_lo*: uint64
    of CHTypeKind.UInt256: u256*: array[4, uint64]
    of CHTypeKind.Int8: i8*: int8
    of CHTypeKind.Int16, CHTypeKind.Enum8, CHTypeKind.Enum16: i16*: int16
    of CHTypeKind.Int32, CHTypeKind.Date32: i32*: int32
    of CHTypeKind.Int64, CHTypeKind.DateTime64: i64*: int64
    of CHTypeKind.Int128, CHTypeKind.Decimal128: i128_hi*, i128_lo*: uint64
    of CHTypeKind.Int256, CHTypeKind.Decimal256: i256*: array[4, uint64]
    of CHTypeKind.Float32: f32*: float32
    of CHTypeKind.Float64: f64*: float64
    of CHTypeKind.String: str*: string
    of CHTypeKind.FixedString: fixed_str*: seq[uint8]
    of CHTypeKind.Date: date_days*: uint16
    of CHTypeKind.DateTime: datetime_ts*: uint32
    of CHTypeKind.Decimal32: d32*: int32
    of CHTypeKind.Decimal64: d64*: int64
    of CHTypeKind.UUID: uuid_hi*, uuid_lo*: uint64
    of CHTypeKind.IPv6: ipv6*: array[16, uint8]
    of CHTypeKind.Array: arr*: seq[CHValue]
    of CHTypeKind.Nullable:
      is_null*: bool
      nullable_val*: ref CHValue
    of CHTypeKind.Tuple: tup*: seq[CHValue]
    of CHTypeKind.Map: map_keys*, map_vals*: seq[CHValue]
    of CHTypeKind.LowCardinality: lc_val*: ref CHValue
    of CHTypeKind.Nothing: discard

  CHColumn* = object
    name*: string
    col_type*: CHKind
    data*: seq[CHValue]

#=======================================================================================================================
#== READ COLUMN DATA FROM SOCKET =======================================================================================
#=======================================================================================================================

proc read_column_data*(sock: Socket; col_type: CHKind; num_rows: int): seq[CHValue] {.io_err.} =
  ## Read num_rows of column data for the given type.
  result = newSeq[CHValue](num_rows)
  case col_type.kind
  of CHTypeKind.UInt8, CHTypeKind.Bool:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.UInt8, u8: sock.read_uint8())
  of CHTypeKind.UInt16:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.UInt16, u16: sock.read_uint16())
  of CHTypeKind.UInt32:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.UInt32, u32: sock.read_uint32())
  of CHTypeKind.UInt64:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.UInt64, u64: sock.read_uint64())
  of CHTypeKind.UInt128:
    for i in 0 ..< num_rows:
      let lo = sock.read_uint64()
      let hi = sock.read_uint64()
      result[i] = CHValue(kind: CHTypeKind.UInt128, u128_lo: lo, u128_hi: hi)
  of CHTypeKind.UInt256:
    for i in 0 ..< num_rows:
      var arr: array[4, uint64]
      for j in 0 ..< 4:
        arr[j] = sock.read_uint64()
      result[i] = CHValue(kind: CHTypeKind.UInt256, u256: arr)
  of CHTypeKind.Int8:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.Int8, i8: sock.read_int8())
  of CHTypeKind.Int16:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.Int16, i16: sock.read_int16())
  of CHTypeKind.Int32:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.Int32, i32: sock.read_int32())
  of CHTypeKind.Int64:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.Int64, i64: sock.read_int64())
  of CHTypeKind.Int128:
    for i in 0 ..< num_rows:
      let lo = sock.read_uint64()
      let hi = sock.read_uint64()
      result[i] = CHValue(kind: CHTypeKind.Int128, i128_lo: lo, i128_hi: hi)
  of CHTypeKind.Int256:
    for i in 0 ..< num_rows:
      var arr: array[4, uint64]
      for j in 0 ..< 4:
        arr[j] = sock.read_uint64()
      result[i] = CHValue(kind: CHTypeKind.Int256, i256: arr)
  of CHTypeKind.Float32:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.Float32, f32: sock.read_float32())
  of CHTypeKind.Float64:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.Float64, f64: sock.read_float64())
  of CHTypeKind.String:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.String, str: sock.read_string())
  of CHTypeKind.FixedString:
    for i in 0 ..< num_rows:
      var buf = newSeq[uint8](col_type.fixed_len)
      if col_type.fixed_len > 0:
        sock.read_raw(addr buf[0], col_type.fixed_len)
      result[i] = CHValue(kind: CHTypeKind.FixedString, fixed_str: buf)
  of CHTypeKind.Date:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.Date, date_days: sock.read_uint16())
  of CHTypeKind.Date32:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.Date32, i32: sock.read_int32())
  of CHTypeKind.DateTime:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.DateTime, datetime_ts: sock.read_uint32())
  of CHTypeKind.DateTime64:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.DateTime64, i64: sock.read_int64())
  of CHTypeKind.Decimal32:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.Decimal32, d32: sock.read_int32())
  of CHTypeKind.Decimal64:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.Decimal64, d64: sock.read_int64())
  of CHTypeKind.Decimal128:
    for i in 0 ..< num_rows:
      let lo = sock.read_uint64()
      let hi = sock.read_uint64()
      result[i] = CHValue(kind: CHTypeKind.Decimal128, i128_lo: lo, i128_hi: hi)
  of CHTypeKind.Decimal256:
    for i in 0 ..< num_rows:
      var arr: array[4, uint64]
      for j in 0 ..< 4:
        arr[j] = sock.read_uint64()
      result[i] = CHValue(kind: CHTypeKind.Decimal256, i256: arr)
  of CHTypeKind.UUID:
    for i in 0 ..< num_rows:
      let hi = sock.read_uint64()
      let lo = sock.read_uint64()
      result[i] = CHValue(kind: CHTypeKind.UUID, uuid_hi: hi, uuid_lo: lo)
  of CHTypeKind.IPv4:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.IPv4, u32: sock.read_uint32())
  of CHTypeKind.IPv6:
    for i in 0 ..< num_rows:
      var addr_bytes: array[16, uint8]
      sock.read_raw(addr addr_bytes[0], 16)
      result[i] = CHValue(kind: CHTypeKind.IPv6, ipv6: addr_bytes)
  of CHTypeKind.Enum8:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.Enum8, i16: int16(sock.read_int8()))
  of CHTypeKind.Enum16:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.Enum16, i16: sock.read_int16())
  of CHTypeKind.Nullable:
    # Read null bitmap
    var nulls = newSeq[uint8](num_rows)
    for i in 0 ..< num_rows:
      nulls[i] = sock.read_uint8()
    # Read nested data
    let inner = sock.read_column_data(col_type.inner_type, num_rows)
    for i in 0 ..< num_rows:
      if nulls[i] != 0:
        result[i] = CHValue(kind: CHTypeKind.Nullable, is_null: true, nullable_val: nil)
      else:
        var v = new(CHValue)
        v[] = inner[i]
        result[i] = CHValue(kind: CHTypeKind.Nullable, is_null: false, nullable_val: v)
  of CHTypeKind.Array:
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
      result[i] = CHValue(kind: CHTypeKind.Array, arr: all_elems[prev ..< cur])
      prev = cur
  of CHTypeKind.Tuple:
    # Each element type column is read sequentially
    var cols: seq[seq[CHValue]] = @[]
    for t in col_type.elem_types:
      cols.add(sock.read_column_data(t, num_rows))
    for i in 0 ..< num_rows:
      var tup: seq[CHValue] = @[]
      for c in cols:
        tup.add(c[i])
      result[i] = CHValue(kind: CHTypeKind.Tuple, tup: tup)
  of CHTypeKind.Map:
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
      result[i] = CHValue(kind: CHTypeKind.Map,
                           map_keys: keys[prev ..< cur],
                           map_vals: vals[prev ..< cur])
      prev = cur
  of CHTypeKind.LowCardinality:
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
      result[i] = CHValue(kind: CHTypeKind.LowCardinality, lc_val: v)
  of CHTypeKind.Nothing:
    for i in 0 ..< num_rows:
      result[i] = CHValue(kind: CHTypeKind.Nothing)

#=======================================================================================================================
#== WRITE COLUMN DATA TO SOCKET ========================================================================================
#=======================================================================================================================

proc write_column_data*(sock: Socket; col_type: CHKind; data: openArray[CHValue]) {.io_err.} =
  ## Write column data for the given type.
  case col_type.kind
  of CHTypeKind.UInt8, CHTypeKind.Bool:
    for v in data: sock.write_uint8(v.u8)
  of CHTypeKind.UInt16:
    for v in data: sock.write_uint16(v.u16)
  of CHTypeKind.UInt32:
    for v in data: sock.write_uint32(v.u32)
  of CHTypeKind.UInt64:
    for v in data: sock.write_uint64(v.u64)
  of CHTypeKind.UInt128:
    for v in data:
      sock.write_uint64(v.u128_lo)
      sock.write_uint64(v.u128_hi)
  of CHTypeKind.UInt256:
    for v in data:
      for j in 0 ..< 4:
        sock.write_uint64(v.u256[j])
  of CHTypeKind.Int8:
    for v in data: sock.write_int8(v.i8)
  of CHTypeKind.Int16:
    for v in data: sock.write_int16(v.i16)
  of CHTypeKind.Int32:
    for v in data: sock.write_int32(v.i32)
  of CHTypeKind.Int64:
    for v in data: sock.write_int64(v.i64)
  of CHTypeKind.Int128:
    for v in data:
      sock.write_uint64(v.i128_lo)
      sock.write_uint64(v.i128_hi)
  of CHTypeKind.Int256:
    for v in data:
      for j in 0 ..< 4:
        sock.write_uint64(v.i256[j])
  of CHTypeKind.Float32:
    for v in data: sock.write_float32(v.f32)
  of CHTypeKind.Float64:
    for v in data: sock.write_float64(v.f64)
  of CHTypeKind.String:
    for v in data: sock.write_string(v.str)
  of CHTypeKind.FixedString:
    for v in data:
      if v.fixed_str.len > 0:
        sock.write_raw(unsafeAddr v.fixed_str[0], col_type.fixed_len)
      else:
        var zeros = newSeq[uint8](col_type.fixed_len)
        sock.write_raw(addr zeros[0], col_type.fixed_len)
  of CHTypeKind.Date:
    for v in data: sock.write_uint16(v.date_days)
  of CHTypeKind.Date32:
    for v in data: sock.write_int32(v.i32)
  of CHTypeKind.DateTime:
    for v in data: sock.write_uint32(v.datetime_ts)
  of CHTypeKind.DateTime64:
    for v in data: sock.write_int64(v.i64)
  of CHTypeKind.Decimal32:
    for v in data: sock.write_int32(v.d32)
  of CHTypeKind.Decimal64:
    for v in data: sock.write_int64(v.d64)
  of CHTypeKind.Decimal128:
    for v in data:
      sock.write_uint64(v.i128_lo)
      sock.write_uint64(v.i128_hi)
  of CHTypeKind.Decimal256:
    for v in data:
      for j in 0 ..< 4:
        sock.write_uint64(v.i256[j])
  of CHTypeKind.UUID:
    for v in data:
      sock.write_uint64(v.uuid_hi)
      sock.write_uint64(v.uuid_lo)
  of CHTypeKind.IPv4:
    for v in data: sock.write_uint32(v.u32)
  of CHTypeKind.IPv6:
    for v in data:
      sock.write_raw(unsafeAddr v.ipv6[0], 16)
  of CHTypeKind.Enum8:
    for v in data: sock.write_int8(int8(v.i16))
  of CHTypeKind.Enum16:
    for v in data: sock.write_int16(v.i16)
  of CHTypeKind.Nullable:
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
  of CHTypeKind.Array:
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
  of CHTypeKind.Tuple:
    for idx, t in col_type.elem_types:
      var col_data = newSeq[CHValue](data.len)
      for i in 0 ..< data.len:
        col_data[i] = data[i].tup[idx]
      sock.write_column_data(t, col_data)
  of CHTypeKind.Map:
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
  of CHTypeKind.LowCardinality:
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
  of CHTypeKind.Nothing:
    discard
