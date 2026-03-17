## ClickHouse native binary protocol constants.

import basis/code/throw

standard_pragmas()

# -----------------------------------------------------------------------
# Distinct string types
# -----------------------------------------------------------------------

type
  Host* = distinct string
  DbName* = distinct string
  DbUser* = distinct string
  DbPassword* = distinct string
  QueryId* = distinct string
  QueryText* = distinct string

func `$`*(v: Host): string {.borrow.}
func `$`*(v: DbName): string {.borrow.}
func `$`*(v: DbUser): string {.borrow.}
func `$`*(v: DbPassword): string {.borrow.}
func `$`*(v: QueryId): string {.borrow.}
func `$`*(v: QueryText): string {.borrow.}
func `==`*(a, b: Host): bool {.borrow.}
func `==`*(a, b: DbName): bool {.borrow.}
func `==`*(a, b: DbUser): bool {.borrow.}
func `==`*(a, b: DbPassword): bool {.borrow.}
func `==`*(a, b: QueryId): bool {.borrow.}
func `==`*(a, b: QueryText): bool {.borrow.}
func len*(v: Host): int {.borrow.}
func len*(v: DbName): int {.borrow.}
func len*(v: DbUser): int {.borrow.}
func len*(v: DbPassword): int {.borrow.}
func len*(v: QueryId): int {.borrow.}
func len*(v: QueryText): int {.borrow.}

# -----------------------------------------------------------------------
# Client packet types
# -----------------------------------------------------------------------

type ClientCode* = enum
  ClientHello = 0
  ClientQuery = 1
  ClientData = 2
  ClientCancel = 3
  ClientPing = 4
  ClientTablesStatusRequest = 5

# -----------------------------------------------------------------------
# Server packet types
# -----------------------------------------------------------------------

type ServerCode* = enum
  ServerHello = 0
  ServerData = 1
  ServerException = 2
  ServerProgress = 3
  ServerPong = 4
  ServerEndOfStream = 5
  ServerProfileInfo = 6
  ServerTotals = 7
  ServerExtremes = 8
  ServerTablesStatusResponse = 9
  ServerLog = 10
  ServerTableColumns = 11
  ServerPartUUIDs = 12
  ServerReadTaskRequest = 13
  ServerProfileEvents = 14
  ServerMergeTreeAllRangesAnnouncement = 15
  ServerMergeTreeReadTaskRequest = 16
  ServerTimezoneUpdate = 17

# -----------------------------------------------------------------------
# Compression
# -----------------------------------------------------------------------

type Compression* = enum
  CompressionDisabled = 0
  CompressionEnabled = 1

type CompressionMethod* = enum
  LZ4 = 1
  LZ4HC = 2
  ZSTD = 3

const
  CompressionMethodLZ4*: uint8 = 0x82
  CompressionMethodZSTD*: uint8 = 0x90

# -----------------------------------------------------------------------
# Query kind
# -----------------------------------------------------------------------

type QueryKind* = enum
  QueryNone = 0
  QueryInitial = 1
  QuerySecondary = 2

# -----------------------------------------------------------------------
# Client interface
# -----------------------------------------------------------------------

type ClientInterface* = enum
  InterfaceTCP = 1
  InterfaceHTTP = 2

# -----------------------------------------------------------------------
# Query stage
# -----------------------------------------------------------------------

type QueryStage* = enum
  FetchColumns = 0
  WithMergeableState = 1
  Complete = 2
  WithMergeableStateAfterAggregation = 3
  WithMergeableStateAfterAggregationAndLimit = 4

# -----------------------------------------------------------------------
# Protocol revision constants
# -----------------------------------------------------------------------

const
  RevisionTempTables* = 50264'u64
  RevisionTotalRowsInProgress* = 51554'u64
  RevisionBlockInfo* = 51903'u64
  RevisionClientInfo* = 54032'u64
  RevisionServerTimezone* = 54058'u64
  RevisionQuotaKeyInClientInfo* = 54060'u64
  RevisionServerDisplayName* = 54372'u64
  RevisionVersionPatch* = 54401'u64
  RevisionServerLogs* = 54406'u64
  RevisionColumnDefaultsMetadata* = 54410'u64
  RevisionClientWriteInfo* = 54420'u64
  RevisionSettingsAsStrings* = 54429'u64
  RevisionInterServerSecret* = 54441'u64
  RevisionOpenTelemetry* = 54442'u64
  RevisionXForwardedFor* = 54443'u64
  RevisionReferer* = 54447'u64
  RevisionDistributedDepth* = 54448'u64
  RevisionQueryStartTime* = 54449'u64
  RevisionProfileEvents* = 54451'u64
  RevisionParallelReplicas* = 54453'u64
  RevisionCustomSerialization* = 54454'u64
  RevisionProfileEventsInInsert* = 54456'u64
  RevisionAddendum* = 54458'u64
  RevisionQuotaKey* = 54458'u64
  RevisionParameters* = 54459'u64
  RevisionServerQueryTimeInProgress* = 54460'u64
  RevisionPasswordComplexityRules* = 54461'u64
  RevisionInterServerSecretV2* = 54462'u64
  RevisionTotalBytesInProgress* = 54463'u64
  RevisionTimezoneUpdates* = 54464'u64
  RevisionSystemKeywordsTable* = 54468'u64

# -----------------------------------------------------------------------
# Defaults
# -----------------------------------------------------------------------

const
  DefaultPort* = 9000'u16
  DefaultSecurePort* = 9440'u16
  DefaultDatabase* = DbName("")
  DefaultUser* = DbUser("default")
  DefaultPassword* = DbPassword("")
  DefaultConnectTimeoutSec* = 10
  DefaultTimeoutSec* = 300
  DefaultCompressBlockSize* = 1048576
  DefaultInsertBlockSize* = 1048576
  BufferSize* = 1048576

# -----------------------------------------------------------------------
# Client identification
# -----------------------------------------------------------------------

const
  ClientName* = "nim-clickhouse"
  ClientVersionMajor* = 24'u64
  ClientVersionMinor* = 1'u64
  ClientVersionPatch* = 0'u64
  ClientRevision* = RevisionAddendum
