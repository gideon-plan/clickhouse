## ClickHouse compression frame: CityHash128 checksum + LZ4 block.
##
## Frame layout (see clickhouse_protocol.md section 8):
##   [checksum: 16] [method: 1] [compressed_size+9: 4] [uncompressed_size: 4] [data: var]

import std/net

import basis/code/throw

import clickhouse/cityhash
import clickhouse/lz4
import clickhouse/wire

standard_pragmas()

raises_error(io_err, [IOError], [ReadIOEffect, WriteIOEffect])

const
  MethodLZ4*: uint8 = 0x82
  MethodZSTD*: uint8 = 0x90
  HeaderSize = 9 # method(1) + compressed_size_with_header(4) + uncompressed_size(4)

#=======================================================================================================================
#== COMPRESS ===========================================================================================================
#=======================================================================================================================

proc compress_block*(data: openArray[uint8]): seq[uint8] {.raises: [IOError].} =
  ## Compress data into a ClickHouse compression frame.
  let compressed = lz4_compress(data)
  let compressed_with_header = HeaderSize + compressed.len
  # Build frame body (without checksum)
  var body = newSeq[uint8](HeaderSize + compressed.len)
  body[0] = MethodLZ4
  # compressed_size_with_header (LE uint32)
  let csz = uint32(compressed_with_header)
  body[1] = uint8(csz and 0xFF)
  body[2] = uint8((csz shr 8) and 0xFF)
  body[3] = uint8((csz shr 16) and 0xFF)
  body[4] = uint8((csz shr 24) and 0xFF)
  # uncompressed_size (LE uint32)
  let usz = uint32(data.len)
  body[5] = uint8(usz and 0xFF)
  body[6] = uint8((usz shr 8) and 0xFF)
  body[7] = uint8((usz shr 16) and 0xFF)
  body[8] = uint8((usz shr 24) and 0xFF)
  # compressed data
  if compressed.len > 0:
    copyMem(addr body[HeaderSize], unsafeAddr compressed[0], compressed.len)
  # Checksum of body
  let hash = city_hash128(body)
  # Full frame: checksum + body
  result = newSeq[uint8](16 + body.len)
  # Write checksum LE: lo first, then hi
  for i in 0 ..< 8:
    result[i] = uint8((hash.lo shr (i * 8)) and 0xFF)
  for i in 0 ..< 8:
    result[8 + i] = uint8((hash.hi shr (i * 8)) and 0xFF)
  copyMem(addr result[16], addr body[0], body.len)

#=======================================================================================================================
#== DECOMPRESS =========================================================================================================
#=======================================================================================================================

proc decompress_block*(frame: openArray[uint8]): seq[uint8] {.raises: [IOError].} =
  ## Decompress a ClickHouse compression frame.
  if frame.len < 16 + HeaderSize:
    raise newException(IOError, "compression frame too short")
  # Verify checksum
  var expected_lo: uint64 = 0
  var expected_hi: uint64 = 0
  for i in 0 ..< 8:
    expected_lo = expected_lo or (uint64(frame[i]) shl (i * 8))
  for i in 0 ..< 8:
    expected_hi = expected_hi or (uint64(frame[8 + i]) shl (i * 8))
  let body_start = 16
  let body_len = frame.len - 16
  var body_slice = newSeq[uint8](body_len)
  copyMem(addr body_slice[0], unsafeAddr frame[body_start], body_len)
  let hash = city_hash128(body_slice)
  if hash.lo != expected_lo or hash.hi != expected_hi:
    raise newException(IOError, "compression checksum mismatch")
  # Parse header
  let method_byte = frame[16]
  if method_byte != MethodLZ4:
    raise newException(IOError, "unsupported compression method: " & $method_byte)
  var compressed_with_header: uint32 = 0
  for i in 0 ..< 4:
    compressed_with_header = compressed_with_header or (uint32(frame[17 + i]) shl (i * 8))
  var uncompressed_size: uint32 = 0
  for i in 0 ..< 4:
    uncompressed_size = uncompressed_size or (uint32(frame[21 + i]) shl (i * 8))
  let compressed_data_start = 16 + HeaderSize
  let compressed_data_len = int(compressed_with_header) - HeaderSize
  if compressed_data_len < 0 or compressed_data_start + compressed_data_len > frame.len:
    raise newException(IOError, "invalid compressed data size")
  let compressed_data = frame[compressed_data_start ..< compressed_data_start + compressed_data_len]
  result = lz4_decompress(compressed_data, int(uncompressed_size))

#=======================================================================================================================
#== SOCKET HELPERS =====================================================================================================
#=======================================================================================================================

proc write_compressed*(sock: Socket; data: openArray[uint8]) {.io_err.} =
  ## Compress and write a ClickHouse compression frame to socket.
  let frame = compress_block(data)
  sock.write_raw(addr frame[0], frame.len)

proc read_compressed*(sock: Socket): seq[uint8] {.io_err.} =
  ## Read and decompress a ClickHouse compression frame from socket.
  # Read checksum (16 bytes)
  var checksum: array[16, uint8]
  sock.read_raw(addr checksum[0], 16)
  # Read method byte
  let method_byte = sock.read_uint8()
  if method_byte != MethodLZ4:
    raise newException(IOError, "unsupported compression method: " & $method_byte)
  # Read sizes
  let compressed_with_header = sock.read_uint32()
  let uncompressed_size = sock.read_uint32()
  let compressed_data_len = int(compressed_with_header) - HeaderSize
  if compressed_data_len < 0:
    raise newException(IOError, "invalid compressed data size")
  # Read compressed data
  var compressed_data = newSeq[uint8](compressed_data_len)
  if compressed_data_len > 0:
    sock.read_raw(addr compressed_data[0], compressed_data_len)
  # Verify checksum
  var body = newSeq[uint8](HeaderSize + compressed_data_len)
  body[0] = method_byte
  let csz = compressed_with_header
  body[1] = uint8(csz and 0xFF)
  body[2] = uint8((csz shr 8) and 0xFF)
  body[3] = uint8((csz shr 16) and 0xFF)
  body[4] = uint8((csz shr 24) and 0xFF)
  let usz = uncompressed_size
  body[5] = uint8(usz and 0xFF)
  body[6] = uint8((usz shr 8) and 0xFF)
  body[7] = uint8((usz shr 16) and 0xFF)
  body[8] = uint8((usz shr 24) and 0xFF)
  if compressed_data_len > 0:
    copyMem(addr body[HeaderSize], addr compressed_data[0], compressed_data_len)
  let hash = city_hash128(body)
  var expected_lo: uint64 = 0
  var expected_hi: uint64 = 0
  for i in 0 ..< 8:
    expected_lo = expected_lo or (uint64(checksum[i]) shl (i * 8))
  for i in 0 ..< 8:
    expected_hi = expected_hi or (uint64(checksum[8 + i]) shl (i * 8))
  if hash.lo != expected_lo or hash.hi != expected_hi:
    raise newException(IOError, "compression checksum mismatch")
  # Decompress
  result = lz4_decompress(compressed_data, int(uncompressed_size))
