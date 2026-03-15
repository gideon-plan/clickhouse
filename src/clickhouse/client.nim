## High-level ClickHouse client API.

import std/net

import basis/code/throw

import clickhouse/wire
import clickhouse/protocol
import clickhouse/error
import clickhouse/column
import clickhouse/datablock
import clickhouse/connection

standard_pragmas()

raises_error(ch_err, [IOError, OSError, TimeoutError, CHError, ValueError], [ReadIOEffect, WriteIOEffect, RootEffect])

# -----------------------------------------------------------------------
# Types
# -----------------------------------------------------------------------

type
  CHClient* = object
    conn*: CHConnection

  CHResult* = object
    columns*: seq[CHColumn]
    num_rows*: int
    profile*: ProfileInfo

# -----------------------------------------------------------------------
# Open / close
# -----------------------------------------------------------------------

proc open*(host: string; port: uint16 = DefaultPort;
           database: string = DefaultDatabase;
           user: string = DefaultUser;
           password: string = DefaultPassword): CHClient {.ch_err.} =
  ## Connect to a ClickHouse server.
  CHClient(conn: connect(host, port, database, user, password))

proc close*(client: var CHClient) {.ch_err.} =
  ## Close the connection.
  client.conn.disconnect()

# -----------------------------------------------------------------------
# Ping
# -----------------------------------------------------------------------

proc ping*(client: var CHClient): bool {.ch_err.} =
  ## Ping the server. Returns true if pong received.
  client.conn.send_ping()
  let pkt = client.conn.receive_packet()
  pkt.kind == pkPong

# -----------------------------------------------------------------------
# Query (SELECT)
# -----------------------------------------------------------------------

proc query*(client: var CHClient; sql: string; query_id: string = ""): CHResult {.ch_err.} =
  ## Execute a SELECT query and return all result data.
  client.conn.send_query(query_id, sql)
  var all_columns: seq[CHColumn] = @[]
  var total_rows = 0
  var initialized = false
  while true:
    let pkt = client.conn.receive_packet()
    case pkt.kind
    of pkData:
      if not pkt.data.is_empty():
        if not initialized:
          all_columns = newSeq[CHColumn](pkt.data.num_columns)
          for i in 0 ..< pkt.data.num_columns:
            all_columns[i] = CHColumn(
              name: pkt.data.columns[i].name,
              col_type: pkt.data.columns[i].col_type,
              data: pkt.data.columns[i].data,
            )
          initialized = true
        else:
          for i in 0 ..< pkt.data.num_columns:
            all_columns[i].data.add(pkt.data.columns[i].data)
        total_rows += pkt.data.num_rows
    of pkProgress:
      discard
    of pkProfileInfo:
      result.profile = pkt.profile
    of pkTotals, pkExtremes, pkTableColumns, pkProfileEvents, pkLog:
      discard
    of pkEndOfStream:
      break
    of pkException:
      raise pkt.error
    of pkPong:
      discard
  result.columns = all_columns
  result.num_rows = total_rows

# -----------------------------------------------------------------------
# Execute (DDL/DML, no result)
# -----------------------------------------------------------------------

proc execute*(client: var CHClient; sql: string; query_id: string = "") {.ch_err.} =
  ## Execute a DDL/DML statement (no result data expected).
  client.conn.send_query(query_id, sql)
  while true:
    let pkt = client.conn.receive_packet()
    case pkt.kind
    of pkData, pkProgress, pkProfileInfo, pkTotals, pkExtremes,
       pkTableColumns, pkProfileEvents, pkLog, pkPong:
      discard
    of pkEndOfStream:
      break
    of pkException:
      raise pkt.error

# -----------------------------------------------------------------------
# Insert
# -----------------------------------------------------------------------

proc insert*(client: var CHClient; sql: string; blk: CHBlock;
             query_id: string = "") {.ch_err.} =
  ## Execute an INSERT query and send data.
  ## `sql` should be like "INSERT INTO table (col1, col2) VALUES"
  client.conn.send_query(query_id, sql)
  # Receive packets until we get the column info data block
  var got_sample = false
  while not got_sample:
    let pkt = client.conn.receive_packet()
    case pkt.kind
    of pkData:
      got_sample = true
    of pkTableColumns:
      discard # metadata, skip
    of pkProgress, pkProfileInfo, pkLog, pkProfileEvents, pkPong:
      discard
    of pkException:
      raise pkt.error
    of pkEndOfStream, pkTotals, pkExtremes:
      return
  # Send data block
  client.conn.send_data(blk)
  # Send empty block to signal end
  client.conn.send_data(empty_block())
  # Receive EndOfStream
  while true:
    let pkt = client.conn.receive_packet()
    case pkt.kind
    of pkProgress, pkProfileInfo, pkData, pkTableColumns, pkProfileEvents,
       pkLog, pkPong, pkTotals, pkExtremes:
      discard
    of pkEndOfStream:
      break
    of pkException:
      raise pkt.error
