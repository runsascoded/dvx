# Transitive staleness in `dvx status`

## Problem

`dvx status` only shows directly stale stages (own dep hashes don't match). It doesn't show stages that are transitively stale — i.e. "fresh" by their own dep hashes, but with a stale ancestor that will change their deps when it runs.

Example: refresh → update_pqts → harmonize. If refresh is stale:
- `update_pqts`: directly stale (git_dep on XMLs changed)
- `harmonize`: appears fresh (dep on crashes.parquet hasn't changed YET)

But harmonize WILL need to run after update_pqts produces a new crashes.parquet. Showing it as "fresh" is misleading.

## Proposed

### Default: show both direct and transitive staleness

```
$ dvx status
✗ njsp/data/refresh.dvc (fetch schedule due)
✗ njsp/data/update_pqts.dvc (git dep changed: data/FAUQStats2026.xml)
⚠ njsp/data/harmonize.dvc (upstream stale: njsp/data/update_pqts.dvc)
⚠ www/public/njsp/csvs.dvc (upstream stale: njsp/data/update_pqts.dvc)
✓ www/deploy.dvc

Fresh: 1, Directly stale: 2, Transitively stale: 2
```

- `✗` (red): directly stale — own deps changed
- `⚠` (yellow): transitively stale — an ancestor is stale
- `✓` (green): fresh

### Flags

```
dvx status                    # default: show all (direct + transitive)
dvx status --direct           # only directly stale
dvx status --transitive       # only transitively stale  
dvx status --no-transitive    # hide transitive (old behavior)
```

### Implementation

Walk the DAG from each stale stage and mark all descendants as transitively stale. This requires resolving the dep graph (which deps point to which `.dvc` outputs). The DAG is already computed for `dvx run` — reuse it.

### Interaction with `dvx run`

`dvx run` already handles this correctly by executing stages in dependency order — when update_pqts runs and updates crashes.parquet, harmonize's dep hash changes and it becomes directly stale. But `dvx status` should show the full picture upfront.
