## CityHash128 -- pure Nim port of Google's CityHash128.
##
## Reference: https://github.com/google/cityhash
## Only CityHash128 is implemented (used by ClickHouse compression frames).

import basis/code/throw

standard_pragmas()

#=======================================================================================================================
#== TYPES ==============================================================================================================
#=======================================================================================================================

type UInt128* = object
  lo*: uint64
  hi*: uint64

#=======================================================================================================================
#== CONSTANTS ==========================================================================================================
#=======================================================================================================================

const
  k0 = 0xc3a5c85c97cb3127'u64
  k1 = 0xb492b66fbe98f273'u64
  k2 = 0x9ae16a3b2f90404f'u64
  k3 = 0xc949d7c7509e6557'u64

#=======================================================================================================================
#== HELPERS ============================================================================================================
#=======================================================================================================================

proc fetch64(p: openArray[uint8]; pos: int): uint64 {.ok.} =
  uint64(p[pos]) or (uint64(p[pos+1]) shl 8) or
  (uint64(p[pos+2]) shl 16) or (uint64(p[pos+3]) shl 24) or
  (uint64(p[pos+4]) shl 32) or (uint64(p[pos+5]) shl 40) or
  (uint64(p[pos+6]) shl 48) or (uint64(p[pos+7]) shl 56)

proc fetch32(p: openArray[uint8]; pos: int): uint32 {.ok.} =
  uint32(p[pos]) or (uint32(p[pos+1]) shl 8) or
  (uint32(p[pos+2]) shl 16) or (uint32(p[pos+3]) shl 24)

proc rot(val: uint64; shift: int): uint64 {.ok_inline.} =
  if shift == 0: val
  else: (val shr shift) or (val shl (64 - shift))

proc shift_mix(val: uint64): uint64 {.ok_inline.} =
  val xor (val shr 47)

proc hash128to64(lo, hi: uint64): uint64 {.ok.} =
  let kMul = 0x9ddfea08eb382d69'u64
  var a = (lo xor hi) * kMul
  a = a xor (a shr 47)
  var b = (hi xor a) * kMul
  b = b xor (b shr 47)
  b

proc hash_len16(u, v, mul: uint64): uint64 {.ok.} =
  var a = (u xor v) * mul
  a = a xor (a shr 47)
  var b = (v xor a) * mul
  b = b xor (b shr 47)
  b

proc weak_hash_len32_with_seeds(w, x, y, z, a, b: uint64): UInt128 {.ok.} =
  var aa = a + w
  var bb = rot(b + aa + z, 21)
  let c = aa
  aa += x
  aa += y
  bb += rot(aa, 44)
  UInt128(lo: bb + c, hi: aa + z)

proc weak_hash_len32_with_seeds(s: openArray[uint8]; pos: int; a, b: uint64): UInt128 {.ok.} =
  weak_hash_len32_with_seeds(
    fetch64(s, pos), fetch64(s, pos + 8),
    fetch64(s, pos + 16), fetch64(s, pos + 24),
    a, b
  )

#=======================================================================================================================
#== CITYHASH64 (NEEDED INTERNALLY BY CITYHASH128) ======================================================================
#=======================================================================================================================

proc hash_len0to16(s: openArray[uint8]; pos, length: int): uint64 {.ok.} =
  if length >= 8:
    let mul = k2 + uint64(length) * 2
    let a = fetch64(s, pos) + k2
    let b = fetch64(s, pos + length - 8)
    let c = rot(b, 37) * mul + a
    let d = (rot(a, 25) + b) * mul
    hash_len16(c, d, mul)
  elif length >= 4:
    let mul = k2 + uint64(length) * 2
    let a = uint64(fetch32(s, pos))
    hash_len16(uint64(length) + (a shl 3), uint64(fetch32(s, pos + length - 4)), mul)
  elif length > 0:
    let a = s[pos]
    let b = s[pos + (length shr 1)]
    let c = s[pos + length - 1]
    let y = uint32(a) + (uint32(b) shl 8)
    let z = uint32(length) + (uint32(c) shl 2)
    shift_mix(uint64(y) * k2 xor uint64(z) * k0) * k2
  else:
    k2

proc hash_len17to32(s: openArray[uint8]; pos, length: int): uint64 {.ok.} =
  let mul = k2 + uint64(length) * 2
  let a = fetch64(s, pos) * k1
  let b = fetch64(s, pos + 8)
  let c = fetch64(s, pos + length - 8) * mul
  let d = fetch64(s, pos + length - 16) * k2
  hash_len16(
    rot(a + b, 43) + rot(c, 30) + d,
    a + rot(b + k2, 18) + c, mul
  )

proc hash_len33to64(s: openArray[uint8]; pos, length: int): uint64 {.ok.} =
  let mul = k2 + uint64(length) * 2
  let a = fetch64(s, pos) * k2
  let b = fetch64(s, pos + 8)
  let c = fetch64(s, pos + length - 8) * mul
  let d = fetch64(s, pos + length - 16) * k2
  let e = fetch64(s, pos + 16) * k2
  let f = fetch64(s, pos + 24) * 9
  let g = fetch64(s, pos + length - 32)
  let h = fetch64(s, pos + length - 24) * mul
  let u = rot(a + g, 43) + (rot(b, 30) + c) * 9
  let v = ((a + g) xor d) + f + 1
  let w = rot(u + v, 28) * mul + h
  let x = rot(e + f, 42) + c
  let y = rot(v + w, 28) * mul + g
  hash_len16(rot(x + y, 43) + w, u + rot(e, 18) + b, mul)

proc city_hash64(s: openArray[uint8]; pos, length: int): uint64 {.ok.} =
  if length <= 16:
    return hash_len0to16(s, pos, length)
  if length <= 32:
    return hash_len17to32(s, pos, length)
  if length <= 64:
    return hash_len33to64(s, pos, length)
  var x = fetch64(s, pos + length - 40)
  var y = fetch64(s, pos + length - 16) + fetch64(s, pos + length - 56)
  var z = hash128to64(fetch64(s, pos + length - 48) + uint64(length),
                       fetch64(s, pos + length - 24))
  var v = weak_hash_len32_with_seeds(s, pos + length - 64, uint64(length), z)
  var w = weak_hash_len32_with_seeds(s, pos + length - 32, y + k1, x)
  x = x * k1 + fetch64(s, pos)
  var tail_done = 0
  var sp = pos
  while true:
    x = rot(x + y + v.lo + fetch64(s, sp + 8), 37) * k1
    y = rot(y + v.hi + fetch64(s, sp + 48), 42) * k1
    x = x xor w.hi
    y += v.lo + fetch64(s, sp + 40)
    z = rot(z + w.lo, 33) * k1
    v = weak_hash_len32_with_seeds(s, sp, v.hi * k1, x + w.lo)
    w = weak_hash_len32_with_seeds(s, sp + 32, z + w.hi, y + fetch64(s, sp + 16))
    swap(z, x)
    sp += 64
    tail_done += 64
    if tail_done + 64 > length:
      break
  let mul = k1 + ((z and 0xFF) shl 1)
  sp = pos + length - tail_done
  w = UInt128(lo: w.lo + (uint64(length - 1) and 63), hi: w.hi)
  v = UInt128(lo: v.lo + w.lo, hi: v.hi)
  w = UInt128(lo: w.lo + v.lo, hi: w.hi)
  x = rot(x + y + v.lo + fetch64(s, sp + 8), 37) * mul
  y = rot(y + v.hi + fetch64(s, sp + 48), 42) * mul
  x = x xor w.hi * 9
  y += v.lo * 9 + fetch64(s, sp + 40)
  z = rot(z + w.lo, 33) * mul
  v = weak_hash_len32_with_seeds(s, sp, v.hi * mul, x + w.lo)
  w = weak_hash_len32_with_seeds(s, sp + 32, z + w.hi, y + fetch64(s, sp + 16))
  swap(z, x)
  hash_len16(hash_len16(v.lo, w.lo, mul) + shift_mix(y) * k0 + z,
             hash_len16(v.hi, w.hi, mul) + x, mul)

#=======================================================================================================================
#== CITYHASH128 ========================================================================================================
#=======================================================================================================================

proc city_hash_128_with_seed(s: openArray[uint8]; pos, length: int; lo0, hi0: uint64): UInt128 {.ok.} =
  if length < 128:
    return UInt128(
      lo: hash128to64(city_hash64(s, pos, length) xor lo0, hi0),
      hi: hash128to64(hi0, city_hash64(s, pos, length) xor lo0)
    )
  var x = lo0
  var y = hi0
  var z = uint64(length) * k1
  let v_lo_init = rot(y xor k1, 49) * k1 + fetch64(s, pos)
  var v = UInt128(
    lo: v_lo_init,
    hi: rot(v_lo_init, 42) * k1 + fetch64(s, pos + 8)
  )
  var w = UInt128(
    lo: rot(y + z, 35) * k1 + x,
    hi: rot(x + fetch64(s, pos + 88), 53) * k1
  )
  var sp = pos
  while true:
    x = rot(x + y + v.lo + fetch64(s, sp + 16), 37) * k1
    y = rot(y + v.hi + fetch64(s, sp + 48), 42) * k1
    x = x xor w.hi
    y += v.lo + fetch64(s, sp + 40)
    z = rot(z + w.lo, 33) * k1
    v = weak_hash_len32_with_seeds(s, sp, v.hi * k1, x + w.lo)
    w = weak_hash_len32_with_seeds(s, sp + 32, z + w.hi, y + fetch64(s, sp + 16))
    swap(z, x)
    sp += 64
    x = rot(x + y + v.lo + fetch64(s, sp + 16), 37) * k1
    y = rot(y + v.hi + fetch64(s, sp + 48), 42) * k1
    x = x xor w.hi
    y += v.lo + fetch64(s, sp + 40)
    z = rot(z + w.lo, 33) * k1
    v = weak_hash_len32_with_seeds(s, sp, v.hi * k1, x + w.lo)
    w = weak_hash_len32_with_seeds(s, sp + 32, z + w.hi, y + fetch64(s, sp + 16))
    swap(z, x)
    sp += 64
    if sp + 128 > pos + length:
      break
  y += rot(w.lo, 37) * k0 + z
  x += rot(v.lo + z, 49) * k0
  var sp2 = pos + length - 128
  for i in 0 ..< 2:
    x = rot(x + y + v.lo + fetch64(s, sp2 + 16), 37) * k1
    y = rot(y + v.hi + fetch64(s, sp2 + 48), 42) * k1
    x = x xor w.hi
    y += v.lo + fetch64(s, sp2 + 40)
    z = rot(z + w.lo, 33) * k1
    v = weak_hash_len32_with_seeds(s, sp2, v.hi * k1, x + w.lo)
    w = weak_hash_len32_with_seeds(s, sp2 + 32, z + w.hi, y + fetch64(s, sp2 + 16))
    swap(z, x)
    sp2 += 64
  UInt128(
    lo: hash_len16(hash_len16(v.lo, w.lo, k1) + shift_mix(y) * k1 + z,
                    hash_len16(v.hi, w.hi, k1) + x, k1),
    hi: hash_len16(hash_len16(v.lo, w.lo, k1) + x,
                    hash_len16(v.hi, w.hi, k1) + shift_mix(y) * k1 + z, k1)
  )

proc city_hash128*(data: openArray[uint8]): UInt128 {.ok.} =
  ## Compute CityHash128 of a byte sequence.
  let length = data.len
  if length >= 16:
    city_hash_128_with_seed(
      data, 16, length - 16,
      fetch64(data, 0) xor k3,
      fetch64(data, 8)
    )
  elif length >= 8:
    city_hash_128_with_seed(
      @[], 0, 0,
      fetch64(data, 0) xor (uint64(length) * k0),
      fetch64(data, length - 8) xor k1
    )
  else:
    city_hash_128_with_seed(data, 0, length, k0, k1)
