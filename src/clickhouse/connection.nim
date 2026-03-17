## ClickHouse TCP connection and handshake.

import std/net

import basis/code/throw

import clickhouse/wire
import clickhouse/protocol
import clickhouse/error
import clickhouse/datablock

standard_pragmas()

raises_error(io_err, [IOError, OSError, TimeoutError, CHError, ValueError], [ReadIOEffect, WriteIOEffect, RootEffect])

#=======================================================================================================================
#== TYPES ==============================================================================================================
#=======================================================================================================================

type
  ServerInfo* = object
    name*: string
    version_major*: uint64
    version_minor*: uint64
    revision*: uint64
    timezone*: string
    display_name*: string
    version_patch*: uint64

  ProgressInfo* = object
    rows*: uint64
    bytes*: uint64
    total_rows*: uint64
    written_rows*: uint64
    written_bytes*: uint64
    elapsed_ns*: uint64

  ProfileInfo* = object
    rows*: uint64
    blocks*: uint64
    bytes*: uint64
    applied_limit*: bool
    rows_before_limit*: uint64
    calculated_rows_before_limit*: bool

  CHConnection* = object
    sock*: Socket
    server*: ServerInfo
    connected*: bool

#=======================================================================================================================
#== CONNECT AND HANDSHAKE ==============================================================================================
#=======================================================================================================================

proc send_hello(conn: var CHConnection; database: DbName; user: DbUser; password: DbPassword) {.io_err.} =
  conn.sock.write_varuint(uint64(ord(ClientHello)))
  conn.sock.write_string(ClientName)
  conn.sock.write_varuint(ClientVersionMajor)
  conn.sock.write_varuint(ClientVersionMinor)
  conn.sock.write_varuint(ClientRevision)
  conn.sock.write_string($database)
  conn.sock.write_string($user)
  conn.sock.write_string($password)

proc receive_hello(conn: var CHConnection) {.io_err.} =
  let packet_type = conn.sock.read_varuint()
  if packet_type == uint64(ord(ServerException)):
    let ex = conn.sock.read_exception()
    raise ex
  if packet_type != uint64(ord(ServerHello)):
    raise newException(IOError, "expected ServerHello, got " & $packet_type)
  conn.server.name = conn.sock.read_string()
  conn.server.version_major = conn.sock.read_varuint()
  conn.server.version_minor = conn.sock.read_varuint()
  conn.server.revision = conn.sock.read_varuint()
  if conn.server.revision >= RevisionServerTimezone:
    conn.server.timezone = conn.sock.read_string()
  if conn.server.revision >= RevisionServerDisplayName:
    conn.server.display_name = conn.sock.read_string()
  if conn.server.revision >= RevisionVersionPatch:
    conn.server.version_patch = conn.sock.read_varuint()
  # Note: password complexity rules and nonce are only sent if
  # min(client_revision, server_revision) >= their thresholds.
  # Since ClientRevision = 54458 < 54461, server will not send them.

proc send_addendum(conn: var CHConnection) {.io_err.} =
  ## Send client addendum after hello (revision >= 54458).
  let used_rev = min(ClientRevision, conn.server.revision)
  if used_rev >= RevisionAddendum:
    conn.sock.write_string("") # quota_key

proc connect*(host: Host; port: uint16 = DefaultPort;
              database: DbName = DefaultDatabase;
              user: DbUser = DefaultUser;
              password: DbPassword = DefaultPassword;
              timeout_ms: int = DefaultConnectTimeoutSec * 1000): CHConnection {.io_err.} =
  ## Connect to a ClickHouse server and perform handshake.
  result.sock = newSocket()
  result.sock.connect($host, Port(port), timeout_ms)
  result.send_hello(database, user, password)
  result.receive_hello()
  result.send_addendum()
  result.connected = true

proc disconnect*(conn: var CHConnection) {.io_err.} =
  ## Close the connection.
  if conn.connected:
    conn.sock.close()
    conn.connected = false

proc revision*(conn: CHConnection): uint64 {.ok_inline.} =
  conn.server.revision

#=======================================================================================================================
#== SEND QUERY =========================================================================================================
#=======================================================================================================================

proc send_query*(conn: var CHConnection; query_id: QueryId; query_text: QueryText;
                 compression: Compression = CompressionDisabled) {.io_err.} =
  ## Send a Query packet.
  let rev = min(ClientRevision, conn.server.revision)
  conn.sock.write_varuint(uint64(ord(ClientQuery)))
  conn.sock.write_string($query_id)
  # ClientInfo
  if rev >= RevisionClientWriteInfo:
    conn.sock.write_uint8(uint8(ord(QueryInitial))) # query_kind
    conn.sock.write_string("") # initial_user
    conn.sock.write_string("") # initial_query_id
    conn.sock.write_string("[::ffff:127.0.0.1]:0") # initial_address
    if rev >= RevisionQueryStartTime:
      conn.sock.write_int64(0) # initial_time
    conn.sock.write_uint8(uint8(ord(InterfaceTCP))) # interface
    conn.sock.write_string("") # os_user
    conn.sock.write_string("") # client_hostname
    conn.sock.write_string(ClientName)
    conn.sock.write_varuint(ClientVersionMajor)
    conn.sock.write_varuint(ClientVersionMinor)
    conn.sock.write_varuint(ClientRevision)
    if rev >= RevisionQuotaKeyInClientInfo:
      conn.sock.write_string("") # quota_key
    if rev >= RevisionDistributedDepth:
      conn.sock.write_varuint(0) # distributed_depth
    if rev >= RevisionVersionPatch:
      conn.sock.write_varuint(ClientVersionPatch)
    if rev >= RevisionOpenTelemetry:
      conn.sock.write_uint8(0) # no trace context
    if rev >= RevisionParallelReplicas:
      conn.sock.write_varuint(0) # collaborate_with_initiator
      conn.sock.write_varuint(0) # count_participating_replicas
      conn.sock.write_varuint(0) # number_of_current_replica
  # Settings (empty -- just terminator)
  conn.sock.write_string("")
  # Inter-server secret
  if rev >= RevisionInterServerSecret:
    conn.sock.write_string("")
  # Stage = Complete
  conn.sock.write_varuint(uint64(ord(Complete)))
  # Compression
  conn.sock.write_varuint(uint64(ord(compression)))
  # Query text
  conn.sock.write_string($query_text)
  # Parameters
  if rev >= RevisionParameters:
    conn.sock.write_string("")
  # Send empty data block
  conn.sock.write_varuint(uint64(ord(ClientData)))
  conn.sock.write_block(empty_block(), rev)

#=======================================================================================================================
#== SEND DATA BLOCK (FOR INSERTS) ======================================================================================
#=======================================================================================================================

proc send_data*(conn: var CHConnection; blk: CHBlock) {.io_err.} =
  conn.sock.write_varuint(uint64(ord(ClientData)))
  conn.sock.write_block(blk, min(ClientRevision, conn.server.revision))

#=======================================================================================================================
#== SEND PING ==========================================================================================================
#=======================================================================================================================

proc send_ping*(conn: var CHConnection) {.io_err.} =
  conn.sock.write_varuint(uint64(ord(ClientPing)))

#=======================================================================================================================
#== SEND CANCEL ========================================================================================================
#=======================================================================================================================

proc send_cancel*(conn: var CHConnection) {.io_err.} =
  conn.sock.write_varuint(uint64(ord(ClientCancel)))

#=======================================================================================================================
#== RECEIVE PACKETS ====================================================================================================
#=======================================================================================================================

proc read_progress*(conn: var CHConnection): ProgressInfo {.io_err.} =
  let rev = min(ClientRevision, conn.server.revision)
  result.rows = conn.sock.read_varuint()
  result.bytes = conn.sock.read_varuint()
  if rev >= RevisionTotalRowsInProgress:
    result.total_rows = conn.sock.read_varuint()
  if rev >= RevisionClientWriteInfo:
    result.written_rows = conn.sock.read_varuint()
    result.written_bytes = conn.sock.read_varuint()
  if rev >= RevisionServerQueryTimeInProgress:
    result.elapsed_ns = conn.sock.read_varuint()

proc read_profile_info*(conn: var CHConnection): ProfileInfo {.io_err.} =
  result.rows = conn.sock.read_varuint()
  result.blocks = conn.sock.read_varuint()
  result.bytes = conn.sock.read_varuint()
  result.applied_limit = conn.sock.read_uint8() != 0
  result.rows_before_limit = conn.sock.read_varuint()
  result.calculated_rows_before_limit = conn.sock.read_uint8() != 0

type
  PacketKind* = enum
    pkData
    pkProgress
    pkProfileInfo
    pkEndOfStream
    pkException
    pkTotals
    pkExtremes
    pkTableColumns
    pkProfileEvents
    pkPong
    pkLog

  Packet* = object
    case kind*: PacketKind
    of pkData, pkTotals, pkExtremes, pkProfileEvents, pkLog:
      data*: CHBlock
    of pkProgress:
      progress*: ProgressInfo
    of pkProfileInfo:
      profile*: ProfileInfo
    of pkEndOfStream, pkPong:
      discard
    of pkException:
      error*: ref CHError
    of pkTableColumns:
      discard

proc receive_packet*(conn: var CHConnection): Packet {.io_err.} =
  ## Read and dispatch one server packet.
  let code = conn.sock.read_varuint()
  let rev = min(ClientRevision, conn.server.revision)
  case code
  of uint64(ord(ServerData)):
    Packet(kind: pkData, data: conn.sock.read_block(rev))
  of uint64(ord(ServerException)):
    let ex = conn.sock.read_exception()
    Packet(kind: pkException, error: ex)
  of uint64(ord(ServerProgress)):
    Packet(kind: pkProgress, progress: conn.read_progress())
  of uint64(ord(ServerPong)):
    Packet(kind: pkPong)
  of uint64(ord(ServerEndOfStream)):
    Packet(kind: pkEndOfStream)
  of uint64(ord(ServerProfileInfo)):
    Packet(kind: pkProfileInfo, profile: conn.read_profile_info())
  of uint64(ord(ServerTotals)):
    Packet(kind: pkTotals, data: conn.sock.read_block(rev))
  of uint64(ord(ServerExtremes)):
    Packet(kind: pkExtremes, data: conn.sock.read_block(rev))
  of uint64(ord(ServerTableColumns)):
    # TableColumns sends two strings: external_table_name, column_description
    discard conn.sock.read_string()
    discard conn.sock.read_string()
    Packet(kind: pkTableColumns)
  of uint64(ord(ServerLog)):
    Packet(kind: pkLog, data: conn.sock.read_block(rev))
  of uint64(ord(ServerProfileEvents)):
    Packet(kind: pkProfileEvents, data: conn.sock.read_block(rev))
  else:
    raise newException(IOError, "unknown server packet code: " & $code)
