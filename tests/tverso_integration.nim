{.experimental: "strictFuncs".}
## Integration test: verso against live ClickHouse server.
##
## Requires: podman run -d --name verso-ch -p 9000:9000 -e CLICKHOUSE_USER=default -e CLICKHOUSE_PASSWORD=test docker.io/clickhouse/clickhouse-server:latest

import std/unittest
import basis/code/choice
import basis/code/verso
import clickhouse/client
import clickhouse/column
import clickhouse/datablock
import clickhouse/protocol

proc ch_val_str(s: string): CHValue =
  CHValue(kind: CHTypeKind.String, str: s)

proc ch_val_i64(v: int64): CHValue =
  CHValue(kind: CHTypeKind.Int64, i64: v)

proc ch_val_i32(v: int32): CHValue =
  CHValue(kind: CHTypeKind.Int32, i32: v)

suite "verso clickhouse integration":
  var c: CHClient

  setup:
    c = open(Host("localhost"), uint16(9000), DbName("default"), DbUser("default"), DbPassword("test"))
    c.execute(QueryText("DROP TABLE IF EXISTS verso_mutation"))
    c.execute(QueryText("DROP TABLE IF EXISTS verso_entity"))
    c.execute(QueryText("DROP TABLE IF EXISTS verso_delta"))
    c.execute(QueryText("CREATE TABLE verso_mutation (id String, parent String, actor String, timestamp Int64, plan_version Int32, space String, partition Int32) ENGINE = MergeTree() ORDER BY id"))
    c.execute(QueryText("CREATE TABLE verso_entity (mutation_id String, link_type String, instance_id String, life Int32) ENGINE = MergeTree() ORDER BY mutation_id"))
    c.execute(QueryText("CREATE TABLE verso_delta (mutation_id String, knot String, value String, op Int32, life Int32) ENGINE = MergeTree() ORDER BY mutation_id"))

  teardown:
    c.close()

  test "store and query mutation":
    var m = Mutation(parent: "", actor: "admin", timestamp: 100,
                     plan_version: 1, space: "home", partition: pData,
                     entities: @[entity("Person", "abc")],
                     deltas: @[delta_add("name", "Alice")])
    stamp(m)

    # Insert mutation
    let mut_blk = CHBlock(
      num_columns: 7, num_rows: 1,
      columns: @[
        CHColumn(name: "id", col_type: parse_ch_type("String"), data: @[ch_val_str(m.id)]),
        CHColumn(name: "parent", col_type: parse_ch_type("String"), data: @[ch_val_str(m.parent)]),
        CHColumn(name: "actor", col_type: parse_ch_type("String"), data: @[ch_val_str(m.actor)]),
        CHColumn(name: "timestamp", col_type: parse_ch_type("Int64"), data: @[ch_val_i64(m.timestamp)]),
        CHColumn(name: "plan_version", col_type: parse_ch_type("Int32"), data: @[ch_val_i32(int32(m.plan_version))]),
        CHColumn(name: "space", col_type: parse_ch_type("String"), data: @[ch_val_str(m.space)]),
        CHColumn(name: "partition", col_type: parse_ch_type("Int32"), data: @[ch_val_i32(int32(ord(m.partition)))]),
      ])
    c.insert(QueryText("INSERT INTO verso_mutation VALUES"), mut_blk)

    # Insert entity
    for e in m.entities:
      let ent_blk = CHBlock(
        num_columns: 4, num_rows: 1,
        columns: @[
          CHColumn(name: "mutation_id", col_type: parse_ch_type("String"), data: @[ch_val_str(m.id)]),
          CHColumn(name: "link_type", col_type: parse_ch_type("String"), data: @[ch_val_str(e.link_type)]),
          CHColumn(name: "instance_id", col_type: parse_ch_type("String"), data: @[ch_val_str(e.instance_id)]),
          CHColumn(name: "life", col_type: parse_ch_type("Int32"), data: @[ch_val_i32(int32(ord(e.life)))]),
        ])
      c.insert(QueryText("INSERT INTO verso_entity VALUES"), ent_blk)

    # Insert delta
    for d in m.deltas:
      let del_blk = CHBlock(
        num_columns: 5, num_rows: 1,
        columns: @[
          CHColumn(name: "mutation_id", col_type: parse_ch_type("String"), data: @[ch_val_str(m.id)]),
          CHColumn(name: "knot", col_type: parse_ch_type("String"), data: @[ch_val_str(d.knot)]),
          CHColumn(name: "value", col_type: parse_ch_type("String"), data: @[ch_val_str(d.value)]),
          CHColumn(name: "op", col_type: parse_ch_type("Int32"), data: @[ch_val_i32(int32(ord(d.op)))]),
          CHColumn(name: "life", col_type: parse_ch_type("Int32"), data: @[ch_val_i32(int32(ord(d.life)))]),
        ])
      c.insert(QueryText("INSERT INTO verso_delta VALUES"), del_blk)

    # Query back
    let r = c.query(QueryText("SELECT id, actor, timestamp, space FROM verso_mutation WHERE id = '" & m.id & "'"))
    check r.columns.len == 4
    check r.columns[0].data.len == 1
    check r.columns[0].data[0].str == m.id
    check r.columns[1].data[0].str == "admin"
    check r.columns[2].data[0].i64 == 100
    check r.columns[3].data[0].str == "home"

    # Query entity
    let er = c.query(QueryText("SELECT link_type, instance_id FROM verso_entity WHERE mutation_id = '" & m.id & "'"))
    check er.columns[0].data[0].str == "Person"
    check er.columns[1].data[0].str == "abc"

    # Query delta
    let dr = c.query(QueryText("SELECT knot, value, op FROM verso_delta WHERE mutation_id = '" & m.id & "'"))
    check dr.columns[0].data[0].str == "name"
    check dr.columns[1].data[0].str == "Alice"
    check dr.columns[2].data[0].i32 == int32(ord(doAdd))

  test "query nonexistent returns empty":
    let r = c.query(QueryText("SELECT * FROM verso_mutation WHERE id = 'nonexistent'"))
    check r.columns[0].data.len == 0

  test "CollapsingMergeTree for auto-Smash":
    # CollapsingMergeTree uses sign column (+1/-1) that auto-cancels on merge
    c.execute(QueryText("DROP TABLE IF EXISTS verso_collapsing"))
    c.execute(QueryText("CREATE TABLE verso_collapsing (mutation_id String, knot String, value String, sign Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY (mutation_id, knot)"))

    # Add a value
    let add_blk = CHBlock(
      num_columns: 4, num_rows: 1,
      columns: @[
        CHColumn(name: "mutation_id", col_type: parse_ch_type("String"), data: @[ch_val_str("m1")]),
        CHColumn(name: "knot", col_type: parse_ch_type("String"), data: @[ch_val_str("name")]),
        CHColumn(name: "value", col_type: parse_ch_type("String"), data: @[ch_val_str("Alice")]),
        CHColumn(name: "sign", col_type: parse_ch_type("Int8"), data: @[CHValue(kind: CHTypeKind.Int8, i8: 1)]),
      ])
    c.insert(QueryText("INSERT INTO verso_collapsing VALUES"), add_blk)

    # Remove old + add new (swap)
    let swap_blk = CHBlock(
      num_columns: 4, num_rows: 2,
      columns: @[
        CHColumn(name: "mutation_id", col_type: parse_ch_type("String"), data: @[ch_val_str("m1"), ch_val_str("m1")]),
        CHColumn(name: "knot", col_type: parse_ch_type("String"), data: @[ch_val_str("name"), ch_val_str("name")]),
        CHColumn(name: "value", col_type: parse_ch_type("String"), data: @[ch_val_str("Alice"), ch_val_str("Bob")]),
        CHColumn(name: "sign", col_type: parse_ch_type("Int8"), data: @[CHValue(kind: CHTypeKind.Int8, i8: -1), CHValue(kind: CHTypeKind.Int8, i8: 1)]),
      ])
    c.insert(QueryText("INSERT INTO verso_collapsing VALUES"), swap_blk)

    # Before merge: 3 rows visible
    let pre = c.query(QueryText("SELECT count() FROM verso_collapsing"))
    check pre.columns[0].data[0].u64 == 3

    # FINAL keyword gives collapsed view (Alice +1/-1 cancel, Bob +1 remains)
    let post = c.query(QueryText("SELECT value, sum(sign) as s FROM verso_collapsing FINAL GROUP BY value HAVING s > 0"))
    check post.columns[0].data.len == 1
    check post.columns[0].data[0].str == "Bob"

  test "all Life states persist":
    var m = Mutation(parent: "", actor: "admin", timestamp: 999,
                     plan_version: 42, space: "test", partition: pWork,
                     entities: @[entity("A", "a1", Life.Smash)],
                     deltas: @[Delta(knot: "x", value: "1", op: doRemove, life: Life.Gone)])
    stamp(m)

    let mut_blk = CHBlock(
      num_columns: 7, num_rows: 1,
      columns: @[
        CHColumn(name: "id", col_type: parse_ch_type("String"), data: @[ch_val_str(m.id)]),
        CHColumn(name: "parent", col_type: parse_ch_type("String"), data: @[ch_val_str(m.parent)]),
        CHColumn(name: "actor", col_type: parse_ch_type("String"), data: @[ch_val_str(m.actor)]),
        CHColumn(name: "timestamp", col_type: parse_ch_type("Int64"), data: @[ch_val_i64(m.timestamp)]),
        CHColumn(name: "plan_version", col_type: parse_ch_type("Int32"), data: @[ch_val_i32(int32(m.plan_version))]),
        CHColumn(name: "space", col_type: parse_ch_type("String"), data: @[ch_val_str(m.space)]),
        CHColumn(name: "partition", col_type: parse_ch_type("Int32"), data: @[ch_val_i32(int32(ord(m.partition)))]),
      ])
    c.insert(QueryText("INSERT INTO verso_mutation VALUES"), mut_blk)

    let ent_blk = CHBlock(
      num_columns: 4, num_rows: 1,
      columns: @[
        CHColumn(name: "mutation_id", col_type: parse_ch_type("String"), data: @[ch_val_str(m.id)]),
        CHColumn(name: "link_type", col_type: parse_ch_type("String"), data: @[ch_val_str("A")]),
        CHColumn(name: "instance_id", col_type: parse_ch_type("String"), data: @[ch_val_str("a1")]),
        CHColumn(name: "life", col_type: parse_ch_type("Int32"), data: @[ch_val_i32(int32(ord(Life.Smash)))]),
      ])
    c.insert(QueryText("INSERT INTO verso_entity VALUES"), ent_blk)

    let er = c.query(QueryText("SELECT life FROM verso_entity WHERE mutation_id = '" & m.id & "'"))
    check er.columns[0].data[0].i32 == int32(ord(Life.Smash))

    let mr = c.query(QueryText("SELECT partition FROM verso_mutation WHERE id = '" & m.id & "'"))
    check mr.columns[0].data[0].i32 == int32(ord(pWork))
