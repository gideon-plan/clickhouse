{.experimental: "strictFuncs".}
## ClickHouse data block encoding/decoding.
##
## See clickhouse_protocol.md section 9.

import std/[net, strutils]

import basis/code/throw

import clickhouse/wire
import clickhouse/column
import clickhouse/protocol

standard_pragmas()

raises_error(io_err, [IOError, ValueError], [ReadIOEffect, WriteIOEffect])

#=======================================================================================================================
#== BLOCK INFO =========================================================================================================
#=======================================================================================================================

type
  BlockInfo* = object
    is_overflows*: uint8
    bucket_num*: int32

  CHBlock* = object
    info*: BlockInfo
    num_columns*: int
    num_rows*: int
    columns*: seq[CHColumn]

proc default_block_info(): BlockInfo {.ok.} =
  BlockInfo(is_overflows: 0, bucket_num: -1)

#=======================================================================================================================
#== TYPE STRING RECONSTRUCTION =========================================================================================
#=======================================================================================================================

proc type_string*(t: CHKind): string {.ok.} =
  case t.kind
  of CHTypeKind.UInt8: "UInt8"
  of CHTypeKind.UInt16: "UInt16"
  of CHTypeKind.UInt32: "UInt32"
  of CHTypeKind.UInt64: "UInt64"
  of CHTypeKind.UInt128: "UInt128"
  of CHTypeKind.UInt256: "UInt256"
  of CHTypeKind.Int8: "Int8"
  of CHTypeKind.Int16: "Int16"
  of CHTypeKind.Int32: "Int32"
  of CHTypeKind.Int64: "Int64"
  of CHTypeKind.Int128: "Int128"
  of CHTypeKind.Int256: "Int256"
  of CHTypeKind.Float32: "Float32"
  of CHTypeKind.Float64: "Float64"
  of CHTypeKind.Bool: "Bool"
  of CHTypeKind.String: "String"
  of CHTypeKind.FixedString: "FixedString(" & $t.fixed_len & ")"
  of CHTypeKind.Date: "Date"
  of CHTypeKind.Date32: "Date32"
  of CHTypeKind.DateTime:
    if t.tz.len > 0: "DateTime('" & t.tz & "')"
    else: "DateTime"
  of CHTypeKind.DateTime64:
    if t.tz64.len > 0: "DateTime64(" & $t.precision & ", '" & t.tz64 & "')"
    else: "DateTime64(" & $t.precision & ")"
  of CHTypeKind.Decimal32: "Decimal32(" & $t.scale & ")"
  of CHTypeKind.Decimal64: "Decimal64(" & $t.scale & ")"
  of CHTypeKind.Decimal128: "Decimal128(" & $t.scale & ")"
  of CHTypeKind.Decimal256: "Decimal256(" & $t.scale & ")"
  of CHTypeKind.UUID: "UUID"
  of CHTypeKind.IPv4: "IPv4"
  of CHTypeKind.IPv6: "IPv6"
  of CHTypeKind.Enum8: "Enum8()"
  of CHTypeKind.Enum16: "Enum16()"
  of CHTypeKind.Array: "Array(" & t.elem_type.type_string() & ")"
  of CHTypeKind.Nullable: "Nullable(" & t.inner_type.type_string() & ")"
  of CHTypeKind.Tuple:
    var parts: seq[string] = @[]
    for e in t.elem_types:
      parts.add(e.type_string())
    "Tuple(" & parts.join(", ") & ")"
  of CHTypeKind.Map: "Map(" & t.key_type.type_string() & ", " & t.val_type.type_string() & ")"
  of CHTypeKind.LowCardinality: "LowCardinality(" & t.dict_type.type_string() & ")"
  of CHTypeKind.Nothing: "Nothing"

#=======================================================================================================================
#== READ / WRITE BLOCK INFO ============================================================================================
#=======================================================================================================================

proc write_block_info(sock: Socket; info: BlockInfo; revision: uint64) {.io_err.} =
  if revision >= RevisionBlockInfo:
    sock.write_varuint(1)
    sock.write_uint8(info.is_overflows)
    sock.write_varuint(2)
    sock.write_int32(info.bucket_num)
    sock.write_varuint(0)

proc read_block_info(sock: Socket; revision: uint64): BlockInfo {.io_err.} =
  result = default_block_info()
  if revision >= RevisionBlockInfo:
    while true:
      let field_num = sock.read_varuint()
      if field_num == 0:
        break
      elif field_num == 1:
        result.is_overflows = sock.read_uint8()
      elif field_num == 2:
        result.bucket_num = sock.read_int32()
      else:
        raise newException(IOError, "unknown block info field: " & $field_num)

#=======================================================================================================================
#== READ BLOCK =========================================================================================================
#=======================================================================================================================

proc read_block*(sock: Socket; revision: uint64): CHBlock {.io_err.} =
  ## Read a data block from socket.
  discard sock.read_string()
  result.info = sock.read_block_info(revision)
  result.num_columns = int(sock.read_varuint())
  result.num_rows = int(sock.read_varuint())
  result.columns = newSeq[CHColumn](result.num_columns)
  for i in 0 ..< result.num_columns:
    let name = sock.read_string()
    let type_str = sock.read_string()
    let col_type = parse_ch_type(type_str)
    if revision >= RevisionCustomSerialization:
      let has_custom = sock.read_uint8()
      if has_custom != 0:
        raise newException(IOError, "custom serialization not supported")
    var data: seq[CHValue] = @[]
    if result.num_rows > 0:
      data = sock.read_column_data(col_type, result.num_rows)
    result.columns[i] = CHColumn(name: name, col_type: col_type, data: data)

#=======================================================================================================================
#== WRITE BLOCK ========================================================================================================
#=======================================================================================================================

proc write_block*(sock: Socket; blk: CHBlock; revision: uint64) {.io_err.} =
  ## Write a data block to socket.
  sock.write_string("")
  sock.write_block_info(blk.info, revision)
  sock.write_varuint(uint64(blk.num_columns))
  sock.write_varuint(uint64(blk.num_rows))
  for col in blk.columns:
    sock.write_string(col.name)
    sock.write_string(col.col_type.type_string())
    if revision >= RevisionCustomSerialization:
      sock.write_uint8(0)
    if blk.num_rows > 0:
      sock.write_column_data(col.col_type, col.data)

#=======================================================================================================================
#== EMPTY BLOCK ========================================================================================================
#=======================================================================================================================

proc empty_block*(): CHBlock {.ok.} =
  CHBlock(
    info: default_block_info(),
    num_columns: 0,
    num_rows: 0,
    columns: @[],
  )

proc is_empty*(blk: CHBlock): bool {.ok_inline.} =
  blk.num_columns == 0 and blk.num_rows == 0
