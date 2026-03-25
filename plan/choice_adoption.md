# Choice/Life Adoption Plan: clickhouse

## Summary

- **No local lattice.nim** -- uses `basis/code/maybe` (legacy)
- **Call sites**: 6 procs returning `Maybe[T, ref Error]`
- **Pattern**: `try: Maybe[T, ref E].yes(expr) except E as e: Maybe[T, ref E].no(e)`
- **Life**: Not applicable (already has basis dep)

## Steps

1. Replace `import basis/code/maybe` with `import basis/code/choice`
2. Replace `Maybe[T, ref E].yes(v)` with `good(v)`
3. Replace `Maybe[T, ref E].no(e)` with `bad[T]("clickhouse", e.msg)`
4. Replace return type `Maybe[T, ref E]` with `Choice[T]`
5. Replace predicate `.good` with `.is_good`, `.bad` with `.is_bad`
6. Update tests
