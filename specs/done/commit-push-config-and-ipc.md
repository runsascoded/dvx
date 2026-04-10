# Flexible commit/push config + stage↔harness IPC

## Problems

1. **Push-per-stage is wasteful**: `--push` after every commit means N git pushes for N stages. Most stages don't need immediate push — batch commits and push once (or at milestones) is better.

2. **Env var file protocol is clunky**: stages write to `$DVX_COMMIT_MSG_FILE` by path, which requires `open(os.environ['DVX_COMMIT_MSG_FILE'], 'w')` boilerplate. Should be a library call.

3. **No config for per-stage behavior**: all stages get the same commit/push treatment. Need per-stage control.

## 1. Push strategies

### Simple options (flags)

```bash
dvx run                    # commit per-stage, no push
dvx run --push             # commit per-stage, push after each commit
dvx run --push-at-end      # commit per-stage, single push at the end
dvx run --push-after <pat> # commit per-stage, push after stages matching pattern
```

`--push-at-end` is the common CI case: accumulate commits, push once when `dvx run` finishes. Avoids N pushes but still gets per-stage commit history.

### Config file (full flexibility)

`dvx.yml` or `.dvx/config.yml` in repo root:

```yaml
# .dvx/config.yml
run:
  # Default: commit per-stage if $DVX_COMMIT_MSG_FILE written
  commit: auto

  # Push strategy: 'never' | 'each' | 'end' | 'after'
  push: end

  # Per-stage overrides
  stages:
    njsp/data/refresh.dvc:
      push: true          # push immediately after this stage
    www/deploy.dvc:
      push: true          # also push after deploy
    api/d1-import.dvc:
      commit: false       # don't commit (side-effect only, no local changes)
```

### Env var overrides

```bash
DVX_PUSH=end    # push strategy (none, each, end, after:<pattern>)
DVX_COMMIT=auto # commit strategy (auto, always, never)
```

CLI flags override env vars, env vars override config file.

## 2. Stage ↔ harness IPC library

### Problem

Current protocol: DVX sets `$DVX_COMMIT_MSG_FILE` env var, stage cmd opens and writes to that path. This is:
- Low-level (file I/O boilerplate)
- One-way (stage → harness only)
- Limited (only commit message, no other signals)

### Proposed: `dvx.stage` library

Python library that stages import to communicate with the DVX harness:

```python
from dvx.stage import stage

# Write commit message (replaces manual $DVX_COMMIT_MSG_FILE write)
stage.commit("Refresh NJSP data")

# Write summary (shown in dvx run output)
stage.summary("3 new crashes found")

# Signal that this stage should trigger a push
stage.push()

# Read harness config
if stage.is_dvx_run:
    # We're running under dvx run, not standalone
    ...

# Set arbitrary key-value outputs (like GHA $GITHUB_OUTPUT)
stage.output("new_crashes", 42)
stage.output("refresh_sha", "abc123")

# Read outputs from upstream stages (if DVX exposes them)
upstream_sha = stage.input("refresh_sha")
```

### Implementation

Under the hood, `dvx.stage` reads/writes env var file paths:

```python
# dvx/stage.py
import os

class _Stage:
    @property
    def is_dvx_run(self) -> bool:
        return 'DVX_COMMIT_MSG_FILE' in os.environ

    def commit(self, message: str) -> None:
        path = os.environ.get('DVX_COMMIT_MSG_FILE')
        if path:
            with open(path, 'w') as f:
                f.write(message)

    def summary(self, text: str) -> None:
        path = os.environ.get('DVX_SUMMARY_FILE')
        if path:
            with open(path, 'w') as f:
                f.write(text)

    def push(self) -> None:
        path = os.environ.get('DVX_PUSH_FILE')
        if path:
            with open(path, 'w') as f:
                f.write('1')

    def output(self, key: str, value) -> None:
        path = os.environ.get('DVX_OUTPUT_FILE')
        if path:
            with open(path, 'a') as f:
                f.write(f"{key}={value}\n")

stage = _Stage()
```

### Non-Python stages

Shell/other-language stages still use env var files directly. The Python library is a convenience, not a requirement.

## 3. Crash-bot integration example

With the library, `njsp/cli/base.py` becomes:

```python
from dvx.stage import stage

def command(fn):
    @njsp.command(fn.__name__)
    @pass_context
    @wraps(fn)
    def _fn(ctx, *args, **kwargs):
        msg = fn(*args, **kwargs)
        if msg:
            stage.commit(msg)  # replaces manual file write
        # ... existing -cc logic as fallback ...
    return _fn
```

## Implementation status

1. ✅ **Push strategies** — `--push each` (per-commit) and `--push end` (batch at finish), also via `$DVX_PUSH` env var
2. ✅ **`dvx.stage` library** — `stage.commit()`, `stage.summary()`, `stage.push()` + `$DVX_PUSH_FILE` env var
3. ✅ **Config file** — `.dvx/config.yml` or `dvx.yml` with `run.commit`, `run.push`, per-stage overrides. Priority: CLI flags > env vars > config > defaults
