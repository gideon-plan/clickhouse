## ClickHouse server exception decoding.

import std/net

import basis/code/throw

import clickhouse/wire

standard_pragmas()

raises_error(io_err, [IOError], [ReadIOEffect, WriteIOEffect])

# -----------------------------------------------------------------------
# Types
# -----------------------------------------------------------------------

type
  CHError* = object of IOError
    ## ClickHouse server exception.
    error_code*: int32
    error_name*: string
    error_message*: string
    stack_trace*: string
    nested*: ref CHError

# -----------------------------------------------------------------------
# Decode
# -----------------------------------------------------------------------

proc read_exception*(sock: Socket): ref CHError {.io_err.} =
  ## Read a server Exception packet from the wire.
  let code = sock.read_int32()
  let name = sock.read_string()
  let message = sock.read_string()
  let stack = sock.read_string()
  let has_nested = sock.read_uint8()
  var nested: ref CHError = nil
  if has_nested != 0:
    nested = sock.read_exception()
  result = (ref CHError)(
    msg: name & ": " & message,
    error_code: code,
    error_name: name,
    error_message: message,
    stack_trace: stack,
    nested: nested,
  )
