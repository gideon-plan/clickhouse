## Wire primitives for the ClickHouse native binary protocol.
##
## All multi-byte integers are little-endian. Strings are length-prefixed
## (varuint + UTF-8 bytes). VarUInt uses unsigned LEB128 encoding.

import std/net

import basis/code/throw

standard_pragmas()

raises_error(read_err, [IOError], [ReadIOEffect])
raises_error(write_err, [IOError], [WriteIOEffect])
raises_error(io_err, [IOError], [ReadIOEffect, WriteIOEffect])

#=======================================================================================================================
#== VARUINT (UNSIGNED LEB128) ==========================================================================================
#=======================================================================================================================

proc write_varuint*(sock: Socket; value: uint64) {.io_err.} =
  ## Encode an unsigned integer as LEB128 and write to socket.
  var v = value
  while v >= 0x80'u64:
    var buf: array[1, uint8]
    buf[0] = uint8(v and 0x7F) or 0x80'u8
    discard sock.send(addr buf[0], 1)
    v = v shr 7
  var buf: array[1, uint8]
  buf[0] = uint8(v and 0x7F)
  discard sock.send(addr buf[0], 1)

proc read_varuint*(sock: Socket): uint64 {.io_err.} =
  ## Read an unsigned LEB128 integer from socket.
  var shift: int = 0
  while true:
    var buf: array[1, uint8]
    let n = sock.recv(addr buf[0], 1)
    if n != 1:
      raise newException(IOError, "unexpected EOF reading varuint")
    let b = buf[0]
    result = result or (uint64(b and 0x7F) shl shift)
    if (b and 0x80) == 0:
      break
    shift += 7
    if shift > 63:
      raise newException(IOError, "varuint overflow (>10 bytes)")

#=======================================================================================================================
#== FIXED-WIDTH INTEGERS ===============================================================================================
#=======================================================================================================================

proc write_uint8*(sock: Socket; value: uint8) {.io_err.} =
  var buf: array[1, uint8] = [value]
  discard sock.send(addr buf[0], 1)

proc read_uint8*(sock: Socket): uint8 {.io_err.} =
  var buf: array[1, uint8]
  let n = sock.recv(addr buf[0], 1)
  if n != 1:
    raise newException(IOError, "unexpected EOF reading uint8")
  result = buf[0]

proc write_int8*(sock: Socket; value: int8) {.io_err.} =
  sock.write_uint8(cast[uint8](value))

proc read_int8*(sock: Socket): int8 {.io_err.} =
  cast[int8](sock.read_uint8())

proc write_uint16*(sock: Socket; value: uint16) {.io_err.} =
  var buf: array[2, uint8]
  buf[0] = uint8(value and 0xFF)
  buf[1] = uint8((value shr 8) and 0xFF)
  discard sock.send(addr buf[0], 2)

proc read_uint16*(sock: Socket): uint16 {.io_err.} =
  var buf: array[2, uint8]
  let n = sock.recv(addr buf[0], 2)
  if n != 2:
    raise newException(IOError, "unexpected EOF reading uint16")
  result = uint16(buf[0]) or (uint16(buf[1]) shl 8)

proc write_int16*(sock: Socket; value: int16) {.io_err.} =
  sock.write_uint16(cast[uint16](value))

proc read_int16*(sock: Socket): int16 {.io_err.} =
  cast[int16](sock.read_uint16())

proc write_uint32*(sock: Socket; value: uint32) {.io_err.} =
  var buf: array[4, uint8]
  buf[0] = uint8(value and 0xFF)
  buf[1] = uint8((value shr 8) and 0xFF)
  buf[2] = uint8((value shr 16) and 0xFF)
  buf[3] = uint8((value shr 24) and 0xFF)
  discard sock.send(addr buf[0], 4)

proc read_uint32*(sock: Socket): uint32 {.io_err.} =
  var buf: array[4, uint8]
  let n = sock.recv(addr buf[0], 4)
  if n != 4:
    raise newException(IOError, "unexpected EOF reading uint32")
  result = uint32(buf[0]) or (uint32(buf[1]) shl 8) or
           (uint32(buf[2]) shl 16) or (uint32(buf[3]) shl 24)

proc write_int32*(sock: Socket; value: int32) {.io_err.} =
  sock.write_uint32(cast[uint32](value))

proc read_int32*(sock: Socket): int32 {.io_err.} =
  cast[int32](sock.read_uint32())

proc write_uint64*(sock: Socket; value: uint64) {.io_err.} =
  var buf: array[8, uint8]
  for i in 0 ..< 8:
    buf[i] = uint8((value shr (i * 8)) and 0xFF)
  discard sock.send(addr buf[0], 8)

proc read_uint64*(sock: Socket): uint64 {.io_err.} =
  var buf: array[8, uint8]
  let n = sock.recv(addr buf[0], 8)
  if n != 8:
    raise newException(IOError, "unexpected EOF reading uint64")
  for i in 0 ..< 8:
    result = result or (uint64(buf[i]) shl (i * 8))

proc write_int64*(sock: Socket; value: int64) {.io_err.} =
  sock.write_uint64(cast[uint64](value))

proc read_int64*(sock: Socket): int64 {.io_err.} =
  cast[int64](sock.read_uint64())

#=======================================================================================================================
#== FLOATING POINT =====================================================================================================
#=======================================================================================================================

proc write_float32*(sock: Socket; value: float32) {.io_err.} =
  sock.write_uint32(cast[uint32](value))

proc read_float32*(sock: Socket): float32 {.io_err.} =
  cast[float32](sock.read_uint32())

proc write_float64*(sock: Socket; value: float64) {.io_err.} =
  sock.write_uint64(cast[uint64](value))

proc read_float64*(sock: Socket): float64 {.io_err.} =
  cast[float64](sock.read_uint64())

#=======================================================================================================================
#== STRINGS ============================================================================================================
#=======================================================================================================================

proc write_string*(sock: Socket; value: string) {.io_err.} =
  ## Write a length-prefixed string (varuint + UTF-8 bytes).
  sock.write_varuint(uint64(value.len))
  if value.len > 0:
    discard sock.send(unsafeAddr value[0], value.len)

proc read_string*(sock: Socket): string {.io_err.} =
  ## Read a length-prefixed string (varuint + UTF-8 bytes).
  let length = sock.read_varuint()
  if length == 0:
    return ""
  result = newString(int(length))
  let n = sock.recv(addr result[0], int(length))
  if n != int(length):
    raise newException(IOError, "unexpected EOF reading string")

#=======================================================================================================================
#== BINARY DATA (SAME WIRE FORMAT AS STRING, DIFFERENT SEMANTICS) ======================================================
#=======================================================================================================================

proc write_bytes*(sock: Socket; value: seq[uint8]) {.io_err.} =
  ## Write length-prefixed binary data.
  sock.write_varuint(uint64(value.len))
  if value.len > 0:
    discard sock.send(unsafeAddr value[0], value.len)

proc read_bytes*(sock: Socket): seq[uint8] {.io_err.} =
  ## Read length-prefixed binary data.
  let length = sock.read_varuint()
  if length == 0:
    return @[]
  result = newSeq[uint8](int(length))
  let n = sock.recv(addr result[0], int(length))
  if n != int(length):
    raise newException(IOError, "unexpected EOF reading bytes")

#=======================================================================================================================
#== FIXED-SIZE BYTE READS (FOR BULK COLUMN DATA) =======================================================================
#=======================================================================================================================

proc read_raw*(sock: Socket; buf: pointer; size: int) {.io_err.} =
  ## Read exactly `size` bytes into `buf`.
  var offset = 0
  while offset < size:
    let n = sock.recv(cast[pointer](cast[uint](buf) + uint(offset)), size - offset)
    if n <= 0:
      raise newException(IOError, "unexpected EOF reading raw bytes")
    offset += n

proc write_raw*(sock: Socket; buf: pointer; size: int) {.io_err.} =
  ## Write exactly `size` bytes from `buf`.
  discard sock.send(buf, size)
