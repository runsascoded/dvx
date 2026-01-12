# Multi-Output Computations

## Problem

Some computations produce multiple output files. For example, a notebook that harmonizes municipality codes might output:
- `njdot/data/muni_codes.parquet`
- `njsp/data/muni_codes.parquet`
- `data/county-city-codes.parquet`
- `www/public/njdot/cc2mc2mn.json`

Each output needs its own `.dvc` file (since DVX tracks files individually), but they all come from the same command: `njsp harmonize_muni_codes`.

## Current Approach: Identical `cmd` as Canonical Identifier

Multiple `.dvc` files can share the same `meta.computation.cmd`. This is a natural way to express "these outputs come from the same computation":

```yaml
# njdot/data/muni_codes.parquet.dvc
outs:
  - md5: f17152b058737a72c4d68dd932be21dd
    size: 4620
    path: muni_codes.parquet
meta:
  computation:
    cmd: njsp harmonize_muni_codes
    deps:
      njdot/data/crashes.parquet: 4d56ec989865608563225eb8f84cef74
      www/public/Municipal_Boundaries_of_NJ.geojson: abc123...
```

```yaml
# njsp/data/muni_codes.parquet.dvc
outs:
  - md5: 255189f356e609eb0ffab1d74420a559
    size: 4119
    path: muni_codes.parquet
meta:
  computation:
    cmd: njsp harmonize_muni_codes
    deps:
      njsp/data/crashes.parquet: def456...
      www/public/Municipal_Boundaries_of_NJ.geojson: abc123...
```

### Key Insight

If two `.dvc` files have **identical `cmd` strings**, they are co-outputs of the same computation. DVX can use this to:

1. **Dedupe at planning time**: When building the execution plan, group outputs by `cmd` and schedule each unique command once
2. **Update all co-outputs after execution**: When a command completes, mark all outputs with that `cmd` as fresh
3. **Skip redundant execution**: If output #2 is scheduled but the command already ran for output #1, skip it (the file is already fresh)

## Per-Output Dependencies

Even though outputs share a command, they may have **different dependencies**. For example:
- `njdot/data/muni_codes.parquet` depends on NJDOT crash data + NJGIN geojson
- `njsp/data/muni_codes.parquet` depends on NJSP crash data + NJGIN geojson
- `data/county-city-codes.parquet` depends only on NJGIN geojson

This allows for more precise staleness detection:
- If only NJSP data changes, only `njsp/data/muni_codes.parquet` becomes stale
- The other outputs remain fresh (their deps haven't changed)

However, when the command runs, **all outputs get regenerated** regardless of which one triggered the run.

## Execution Engine Behavior

### Current Behavior (No Deduplication)
If all 4 outputs are stale:
```
Level 1: 4 computations
  ⟳ njdot/data/muni_codes.parquet: running njsp harmonize_muni_codes...
  ⟳ njsp/data/muni_codes.parquet: running njsp harmonize_muni_codes...
  ⟳ data/county-city-codes.parquet: running njsp harmonize_muni_codes...
  ⟳ www/public/njdot/cc2mc2mn.json: running njsp harmonize_muni_codes...
```
The command runs 4 times (wasteful).

### Proposed Behavior (With Deduplication)
```
Level 1: 4 outputs from 1 computation
  ⟳ njsp harmonize_muni_codes: running... (produces 4 outputs)
  ✓ njdot/data/muni_codes.parquet: completed
  ✓ njsp/data/muni_codes.parquet: completed
  ✓ data/county-city-codes.parquet: completed
  ✓ www/public/njdot/cc2mc2mn.json: completed
```
The command runs once, all 4 outputs marked fresh.

### Alternative: Lazy Skip
Even without explicit deduplication, DVX could check freshness before each execution:
```
Level 1: 4 computations
  ⟳ njdot/data/muni_codes.parquet: running njsp harmonize_muni_codes...
  ✓ njdot/data/muni_codes.parquet: completed
  ⟳ njsp/data/muni_codes.parquet: checking freshness... already fresh, skipping
  ⟳ data/county-city-codes.parquet: checking freshness... already fresh, skipping
  ⟳ www/public/njdot/cc2mc2mn.json: checking freshness... already fresh, skipping
```
This is simpler but adds freshness-check overhead for each output.

## Implementation Notes

1. **Command identity**: The `cmd` string is the canonical identifier. Exact string match determines co-outputs.

2. **Deps union vs. intersection**: When deduping, should the "effective deps" for the computation be:
   - **Union**: Command is stale if *any* output's deps changed (conservative)
   - **Per-output**: Each output has its own staleness, but command runs if *any* are stale

3. **Output verification**: After running, verify all expected outputs exist and have valid checksums.

4. **Partial failures**: If the command runs but only produces some outputs, mark the missing ones as failed.

## Future Considerations

- **Explicit multi-output syntax**: Could add optional `co_outputs` field to make the relationship explicit
- **Shared deps**: Factor out common deps into a shared block to reduce duplication
- **Incremental outputs**: Some commands might only update specific outputs based on which deps changed
