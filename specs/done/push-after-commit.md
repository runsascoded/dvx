# Push after per-stage commits

## Problem

DVX can commit after each stage via `$DVX_COMMIT_MSG_FILE`, but doesn't push. In CI, we want each commit pushed immediately so downstream jobs / the repo stays current. Locally, we just want commits without pushing.

## Proposed solution

`dvx run --push` or `$DVX_PUSH=1` env var. When enabled, DVX runs `git push` after each successful commit.

### Behavior

- Only pushes if a commit was actually made (stage wrote to `$DVX_COMMIT_MSG_FILE` and there were dirty files)
- Push failure is non-fatal (logged as warning) — the commit is still valid locally
- Can be combined with `--force` etc.

### CI usage

```yaml
- name: DVX run
  env:
    DVX_PUSH: "1"
  run: dvx run
```

Or:
```yaml
  run: dvx run --push
```

### Local usage

```bash
dvx run njsp/data/refresh.dvc  # commits but doesn't push
```
