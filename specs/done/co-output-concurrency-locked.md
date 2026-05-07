# Co-output stages race + `database is locked` under partial cache

## Symptom

`dvx run -v <every .dvc>` in `hccs/path` daily CI:

```
Level 1/3: 2 computation(s)
  ✗ data/2026.pqt: database is locked
  ○ data/2026-day-types.pqt: up-to-date
```

`data/2026.pqt.dvc` and `data/2026-day-types.pqt.dvc` are co-outputs —
both have `meta.computation.cmd: path-data monthly -y 2026`. DVX's
re-run of that cmd directly (`subprocess.Popen([cmd])`) finishes in ~3s
with exit 0, so the cmd itself isn't the problem.

Run that fails: <https://github.com/hudcostreets/path/actions/runs/25469756488>

## Why it didn't fail before

Same repo, same code, same parallelism (CPU-count workers). On May 4
when DVX ran with empty cache, the same Level 1 had both 2026 .dvcs
and the scheduler did the right thing:

```
Level 1/3: 2 computation(s)
  ⟳ data/2026-day-types.pqt: running...
  ◐ data/2026.pqt: waiting (same cmd running)...
       → data/2026-day-types.pqt.dvc
  ✓ data/2026.pqt: co-output ready
  ✓ data/2026-day-types.pqt: completed (3.1s)
```

— one worker runs the cmd, sibling co-output gets `co-output ready`
without re-running.

So the bug only triggers when cache is partially warm: stages 3..N
were just `dvx pull`'d and are up-to-date by hash, leaving only stages
1..2 stale at Level 1, and both happen to share a cmd.

## Pre-diagnosis hypotheses (superseded — kept for context)

Initial guesses pointed at the scheduler (cmd-dedup race at small
levels) or sqltrie/`data_index` contention from `repo.push()`. Both
turned out to be wrong: the path workflow doesn't even use `--push
each`, and the failure fires too fast (2.5ms) for any contention-with-
retry pattern. The actual cause was simpler — see Diagnosis below.

## Diagnosis

Pulled the failing GHA log (`hudcostreets/path` run 25469756488). Key
observation: `data/2026.pqt` reports `database is locked` **2.5 ms**
after `Level 1/3` starts — far too fast for any sustained-contention
scenario or `busy_timeout` retry-storm to be the cause.

The path workflow runs `dvx run -v <targets>` **without** `--push each`
or `--push end`, so `repo.push()` (and DVC's `data_index/db.db` via
sqltrie) is *not* on the hot path during the failure.

Reproduced locally: two worker threads concurrently calling
`is_output_fresh` on a freshly-created `.dvc/dvx.db` race ~10–20% of
trials with `database is locked` from
**`ArtifactStatusDB._get_connection` running `PRAGMA journal_mode=WAL`
on each per-thread connection**. Stack trace:

```
File "src/dvx/run/dvc_files.py", line 628, in is_output_fresh
  current_md5, _, _was_cached = get_artifact_hash_cached(path, compute_md5)
File "src/dvx/run/status.py", line 219, in get_artifact_hash_cached
  db = get_status_db()
File "src/dvx/run/status.py", line 202, in get_status_db
  _default_db = ArtifactStatusDB()      # racy singleton
File "src/dvx/run/status.py", line 61, in __init__
  self._ensure_schema()
File "src/dvx/run/status.py", line 93, in _ensure_schema
  conn = self._get_connection()
File "src/dvx/run/status.py", line 85, in _get_connection
  conn.execute("PRAGMA journal_mode=WAL")
sqlite3.OperationalError: database is locked
```

Two compounding bugs:

1. **`get_status_db()` is not thread-safe.** Two workers both pass
   `_default_db is None`, both instantiate `ArtifactStatusDB`, both
   open their own connection and race on init.
2. **`PRAGMA journal_mode=WAL` ignores `busy_timeout`.** Switching
   journal modes requires an exclusive file lock and fails
   instantly with `SQLITE_BUSY` if any other connection holds a
   lock. So even with `timeout=30.0` set on the connection, the
   pragma error is unrecoverable.

"Why only with partial cache?" — when cache is fully empty, every
stage's output is missing, so `is_output_fresh` short-circuits before
touching `dvx.db` (no need to hash). When all stages are fresh, no
work happens. With *partial* cache, threads concurrently hit the
"output exists, hash it, cache mtime" branch and pile into the
same singleton init.

## Fix — **DONE**

Two-line architectural fix in `src/dvx/run/status.py`:

1. **Thread-safe singleton.** Wrap `_default_db` lazy creation with a
   `threading.Lock` (double-checked).
2. **Set WAL mode once, not per connection.** WAL is *persistent* in
   the SQLite file (it survives across connections and process
   restarts). Move the pragma into a one-shot `_init_db()` call from
   `__init__` that opens a connection, runs `PRAGMA journal_mode=WAL`
   + schema, then closes. Per-thread `_get_connection()` only sets
   `busy_timeout` (defense-in-depth) and `foreign_keys` — no more
   journal-mode races on cold start.

Regression test: `tests/test_status_thread_safety.py` —
`test_concurrent_is_output_fresh_no_db_lock` runs 100 trials of
2-thread `is_output_fresh` on a cold DB. Pre-fix: ~10–20% trial
failure rate. Post-fix: 0/100 (and 0/500 in extended runs).

## Out of scope

The earlier hypothesis that this was about co-output scheduler races
turned out wrong (the failure happens in `_should_run`, before the
cmd-dedup block ever runs). If we want to harden the cmd-dedup path
further (e.g. move it ahead of `_should_run` so freshness checks
don't run for stages that will be skipped via co-output), that's a
separate, smaller cleanup unrelated to the lock symptom.
