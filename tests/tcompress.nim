{.experimental: "strictFuncs".}
## Unit tests for LZ4 compression and CityHash128.

import std/unittest

import clickhouse/lz4
import clickhouse/cityhash
import clickhouse/compress

suite "lz4":
  test "compress/decompress round-trip - short":
    let data = @[1'u8, 2, 3, 4, 5]
    let compressed = lz4_compress(data)
    let decompressed = lz4_decompress(compressed, data.len)
    check decompressed == data

  test "compress/decompress round-trip - repeated":
    var data = newSeq[uint8](1000)
    for i in 0 ..< data.len:
      data[i] = uint8(i mod 4)
    let compressed = lz4_compress(data)
    let decompressed = lz4_decompress(compressed, data.len)
    check decompressed == data
    check compressed.len < data.len

  test "compress/decompress round-trip - sequential":
    var data = newSeq[uint8](256)
    for i in 0 ..< 256:
      data[i] = uint8(i)
    let compressed = lz4_compress(data)
    let decompressed = lz4_decompress(compressed, data.len)
    check decompressed == data

  test "empty input":
    let data: seq[uint8] = @[]
    let compressed = lz4_compress(data)
    check compressed.len == 0

suite "cityhash128":
  test "deterministic":
    let data = @[72'u8, 101, 108, 108, 111] # "Hello"
    let h1 = city_hash128(data)
    let h2 = city_hash128(data)
    check h1.lo == h2.lo
    check h1.hi == h2.hi

  test "different inputs produce different hashes":
    let d1 = @[1'u8, 2, 3]
    let d2 = @[4'u8, 5, 6]
    let h1 = city_hash128(d1)
    let h2 = city_hash128(d2)
    check h1.lo != h2.lo or h1.hi != h2.hi

  test "empty input":
    let data: seq[uint8] = @[]
    let h = city_hash128(data)
    check h.lo != 0 or h.hi != 0

suite "compression frame":
  test "compress/decompress block round-trip":
    var data = newSeq[uint8](500)
    for i in 0 ..< data.len:
      data[i] = uint8(i mod 7)
    let frame = compress_block(data)
    let result = decompress_block(frame)
    check result == data

  test "checksum verification":
    var data = newSeq[uint8](100)
    for i in 0 ..< data.len:
      data[i] = uint8(i)
    var frame = compress_block(data)
    # Corrupt a checksum byte
    frame[0] = frame[0] xor 0xFF
    var caught = false
    try:
      discard decompress_block(frame)
    except IOError:
      caught = true
    check caught
