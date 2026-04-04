{.experimental: "strictFuncs".}
## Unit tests for protocol constants.

import std/unittest

import clickhouse/protocol

suite "protocol constants":
  test "client packet codes":
    check ord(ClientHello) == 0
    check ord(ClientQuery) == 1
    check ord(ClientData) == 2
    check ord(ClientCancel) == 3
    check ord(ClientPing) == 4
    check ord(ClientTablesStatusRequest) == 5

  test "server packet codes":
    check ord(ServerHello) == 0
    check ord(ServerData) == 1
    check ord(ServerException) == 2
    check ord(ServerProgress) == 3
    check ord(ServerPong) == 4
    check ord(ServerEndOfStream) == 5
    check ord(ServerProfileInfo) == 6
    check ord(ServerTotals) == 7
    check ord(ServerExtremes) == 8
    check ord(ServerTablesStatusResponse) == 9
    check ord(ServerLog) == 10
    check ord(ServerTableColumns) == 11
    check ord(ServerProfileEvents) == 14
    check ord(ServerTimezoneUpdate) == 17

  test "compression method bytes":
    check CompressionMethodLZ4 == 0x82'u8
    check CompressionMethodZSTD == 0x90'u8

  test "revision constants":
    check RevisionBlockInfo == 51903'u64
    check RevisionServerTimezone == 54058'u64
    check RevisionClientWriteInfo == 54420'u64
    check RevisionSettingsAsStrings == 54429'u64
    check RevisionAddendum == 54458'u64
    check RevisionParameters == 54459'u64

  test "defaults":
    check DefaultPort == 9000
    check DefaultUser == "default"
    check DefaultPassword == ""
