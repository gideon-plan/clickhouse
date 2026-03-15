## LZ4 block compression/decompression -- pure Nim.
##
## Implements the LZ4 block format (not frame format).
## Reference: https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md

import basis/code/throw

standard_pragmas()

raises_error(lz4_err, [IOError], [])

const
  MinMatch = 4
  HashLog = 12
  HashSize = 1 shl HashLog
  MaxDistance = 65535
  MFLimit = 12
  LastLiterals = 5
  MinLength = MFLimit + 1

# -----------------------------------------------------------------------
# Decompression
# -----------------------------------------------------------------------

proc lz4_decompress*(src: openArray[uint8]; uncompressed_size: int): seq[uint8] {.lz4_err.} =
  ## Decompress an LZ4 block. Caller must know the uncompressed size.
  result = newSeq[uint8](uncompressed_size)
  var sp = 0
  var dp = 0
  let src_len = src.len
  while sp < src_len:
    let token = src[sp]
    inc sp
    # Literal length
    var lit_len = int(token shr 4)
    if lit_len == 15:
      while sp < src_len:
        let extra = src[sp]
        inc sp
        lit_len += int(extra)
        if extra != 255: break
    # Copy literals
    if dp + lit_len > uncompressed_size or sp + lit_len > src_len:
      raise newException(IOError, "lz4: corrupt input (literal overflow)")
    copyMem(addr result[dp], unsafeAddr src[sp], lit_len)
    sp += lit_len
    dp += lit_len
    if dp >= uncompressed_size:
      break
    # Match offset
    if sp + 2 > src_len:
      raise newException(IOError, "lz4: corrupt input (missing offset)")
    let offset = int(src[sp]) or (int(src[sp + 1]) shl 8)
    sp += 2
    if offset == 0 or offset > dp:
      raise newException(IOError, "lz4: corrupt input (invalid offset)")
    # Match length
    var match_len = int(token and 0x0F) + MinMatch
    if (token and 0x0F) == 15:
      while sp < src_len:
        let extra = src[sp]
        inc sp
        match_len += int(extra)
        if extra != 255: break
    if dp + match_len > uncompressed_size:
      raise newException(IOError, "lz4: corrupt input (match overflow)")
    # Copy match (may overlap)
    var match_pos = dp - offset
    for i in 0 ..< match_len:
      result[dp] = result[match_pos]
      inc dp
      inc match_pos

# -----------------------------------------------------------------------
# Compression
# -----------------------------------------------------------------------

proc lz4_compress*(src: openArray[uint8]): seq[uint8] {.lz4_err.} =
  ## Compress data using LZ4 block format.
  let src_len = src.len
  if src_len == 0:
    return @[]
  # Worst case: input + overhead
  result = newSeq[uint8](src_len + (src_len div 255) + 16)
  var dp = 0
  if src_len < MinLength:
    # Too short to compress -- emit as literals
    let token = min(src_len, 15)
    result[dp] = uint8(token shl 4)
    inc dp
    if src_len >= 15:
      var remaining = src_len - 15
      while remaining >= 255:
        result[dp] = 255
        inc dp
        remaining -= 255
      result[dp] = uint8(remaining)
      inc dp
    copyMem(addr result[dp], unsafeAddr src[0], src_len)
    dp += src_len
    result.setLen(dp)
    return

  var hash_table: array[HashSize, int]
  for i in 0 ..< HashSize:
    hash_table[i] = 0

  var sp = 0
  var anchor = 0
  let src_limit = src_len - LastLiterals

  template hash4(p: int): int =
    int(((uint32(src[p]) or (uint32(src[p+1]) shl 8) or
          (uint32(src[p+2]) shl 16) or (uint32(src[p+3]) shl 24)) *
         2654435761'u32) shr (32 - HashLog))

  while sp < src_limit:
    let h = hash4(sp)
    var ref_pos = hash_table[h]
    hash_table[h] = sp
    # Check match
    if ref_pos > 0 and sp - ref_pos <= MaxDistance and
       src[ref_pos] == src[sp] and src[ref_pos + 1] == src[sp + 1] and
       src[ref_pos + 2] == src[sp + 2] and src[ref_pos + 3] == src[sp + 3]:
      # Found a match
      let lit_len = sp - anchor
      # Extend match forward
      var match_len = MinMatch
      while sp + match_len < src_len and src[ref_pos + match_len] == src[sp + match_len]:
        inc match_len
      # Encode token
      let ml_code = match_len - MinMatch
      if lit_len < 15 and ml_code < 15:
        result[dp] = uint8((lit_len shl 4) or ml_code)
        inc dp
      else:
        let lt = min(lit_len, 15)
        let mt = min(ml_code, 15)
        result[dp] = uint8((lt shl 4) or mt)
        inc dp
        if lit_len >= 15:
          var remaining = lit_len - 15
          while remaining >= 255:
            result[dp] = 255
            inc dp
            remaining -= 255
          result[dp] = uint8(remaining)
          inc dp
        # Literals
        if lit_len > 0:
          # Ensure capacity
          while dp + lit_len + match_len + 16 > result.len:
            result.setLen(result.len * 2)
          copyMem(addr result[dp], unsafeAddr src[anchor], lit_len)
          dp += lit_len
        # Offset
        let offset = sp - ref_pos
        result[dp] = uint8(offset and 0xFF)
        result[dp + 1] = uint8((offset shr 8) and 0xFF)
        dp += 2
        if ml_code >= 15:
          var remaining = ml_code - 15
          while remaining >= 255:
            result[dp] = 255
            inc dp
            remaining -= 255
          result[dp] = uint8(remaining)
          inc dp
        sp += match_len
        anchor = sp
        continue

      # Literals (for simple token path)
      if lit_len > 0:
        while dp + lit_len + match_len + 16 > result.len:
          result.setLen(result.len * 2)
        copyMem(addr result[dp], unsafeAddr src[anchor], lit_len)
        dp += lit_len
      # Offset
      let offset = sp - ref_pos
      result[dp] = uint8(offset and 0xFF)
      result[dp + 1] = uint8((offset shr 8) and 0xFF)
      dp += 2
      sp += match_len
      anchor = sp
    else:
      inc sp

  # Last literals
  let lit_len = src_len - anchor
  if lit_len > 0:
    while dp + lit_len + 16 > result.len:
      result.setLen(result.len * 2)
    let lt = min(lit_len, 15)
    result[dp] = uint8(lt shl 4)
    inc dp
    if lit_len >= 15:
      var remaining = lit_len - 15
      while remaining >= 255:
        result[dp] = 255
        inc dp
        remaining -= 255
      result[dp] = uint8(remaining)
      inc dp
    copyMem(addr result[dp], unsafeAddr src[anchor], lit_len)
    dp += lit_len
  result.setLen(dp)
