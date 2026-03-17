## Unit tests for wire primitives.

import std/[net, unittest]

import clickhouse/wire

# Helper: create a socket pair for testing
proc make_pair(): (Socket, Socket) =
  let server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(0))
  let port = server.getLocalAddr()[1]
  server.listen()
  let client = newSocket()
  client.connect("127.0.0.1", port)
  var peer: Socket
  new(peer)
  var address: string
  server.acceptAddr(peer, address)
  server.close()
  (client, peer)

suite "varuint":
  test "round-trip boundary values":
    let (w, r) = make_pair()
    defer:
      w.close()
      r.close()
    let values = [0'u64, 1, 127, 128, 255, 256, 16383, 16384, 65535,
                  2097151'u64, 268435455'u64, uint64.high]
    for v in values:
      w.write_varuint(v)
    for v in values:
      let got = r.read_varuint()
      check got == v

suite "fixed-width integers":
  test "uint8 round-trip":
    let (w, r) = make_pair()
    defer:
      w.close()
      r.close()
    w.write_uint8(0)
    w.write_uint8(255)
    check r.read_uint8() == 0
    check r.read_uint8() == 255

  test "int16 round-trip":
    let (w, r) = make_pair()
    defer:
      w.close()
      r.close()
    w.write_int16(-32768)
    w.write_int16(32767)
    check r.read_int16() == -32768
    check r.read_int16() == 32767

  test "uint32 round-trip":
    let (w, r) = make_pair()
    defer:
      w.close()
      r.close()
    w.write_uint32(0)
    w.write_uint32(uint32.high)
    check r.read_uint32() == 0
    check r.read_uint32() == uint32.high

  test "int64 round-trip":
    let (w, r) = make_pair()
    defer:
      w.close()
      r.close()
    w.write_int64(int64.low)
    w.write_int64(int64.high)
    check r.read_int64() == int64.low
    check r.read_int64() == int64.high

  test "uint64 round-trip":
    let (w, r) = make_pair()
    defer:
      w.close()
      r.close()
    w.write_uint64(0)
    w.write_uint64(uint64.high)
    check r.read_uint64() == 0
    check r.read_uint64() == uint64.high

suite "floating point":
  test "float32 round-trip":
    let (w, r) = make_pair()
    defer:
      w.close()
      r.close()
    w.write_float32(3.14'f32)
    w.write_float32(-0.0'f32)
    check r.read_float32() == 3.14'f32
    check r.read_float32() == -0.0'f32

  test "float64 round-trip":
    let (w, r) = make_pair()
    defer:
      w.close()
      r.close()
    w.write_float64(2.718281828459045)
    check r.read_float64() == 2.718281828459045

suite "strings":
  test "string round-trip":
    let (w, r) = make_pair()
    defer:
      w.close()
      r.close()
    w.write_string("")
    w.write_string("hello")
    w.write_string("clickhouse native protocol")
    check r.read_string() == ""
    check r.read_string() == "hello"
    check r.read_string() == "clickhouse native protocol"

  test "bytes round-trip":
    let (w, r) = make_pair()
    defer:
      w.close()
      r.close()
    w.write_bytes(@[])
    w.write_bytes(@[1'u8, 2, 3, 255])
    check r.read_bytes() == newSeq[uint8]()
    check r.read_bytes() == @[1'u8, 2, 3, 255]
