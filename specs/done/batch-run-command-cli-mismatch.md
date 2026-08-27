# `dvx batch`: `run_command` emits CLI that `dvx run` can't parse; per-stage push is commit-gated

Found on nj-crashes' first two real `dvx batch submit`s (2026-08-26, jobs `aacad5c7`/`9c42c237`).

## 1. CLI mismatch (hard failure)

`run_command` builds `["run", "--commit", "never", "--push", "each", ...]`, but:

- `dvx run --commit` / `-c` is a **boolean flag** — it takes no value, so `never` falls through to the targets list.
- `--push` is `Choice([each, end])` — no `never` there either (that path just wasn't hit).

Container output: `Error: target not found: never` — pleasingly, caught by `0ec2b1635`'s missing-target guard; pre-guard this would have been a silent green no-op job. `test_batch.py` asserts the built string but never parses it with the real `run` command — an integration-shaped gap worth one test that round-trips `run_command(...)` through `dvx run`'s click parser (`--dry-run` suffices).

## 2. Per-stage cache push is nested inside the commit branch (contract failure)

`executor.py` (~line 858): the entire per-stage push block — including `_push_cache_blobs` — lives under `if commit_msg:`. Consequences for the batch contract ("every completed stage flushed to the remote before the next starts"):

- `--commit never` (the batch default per the spec) ⇒ zero per-stage pushes ⇒ a Spot reclaim loses *everything since job start*, not one stage.
- Interim workaround that works today: submit `["run", "--commit", "--push", "each", ...]` (flag form ⇒ commit-per-stage; the container's commits are local-only and die with it; `git push` fails ⚠ but `_push_cache_blobs` still runs). Requires a git identity in the image or `git commit` itself fails and skips the blob push — nj-crashes' Dockerfile now sets one.

## Suggested fix

- `dvx run --commit` grows value modes (`never|auto|always`, flag-without-value ≡ `always` for BC with `daily.yml`-style `--commit --push each` usage), or `run_command` stops emitting `never` and instead omits the flag…
- …**and** per-stage `_push_cache_blobs` is hoisted out of the `if commit_msg:` branch so `--push each` means "flush this stage's blobs now" independent of git-commit strategy. Git push stays commit-gated; cache push shouldn't be.
- Then `run_command`'s default can return to the honest `--commit never --push each`, and nj-crashes drops its workaround.

## 3. Confirmed blocker: dep-missing vs output-missing classification (Fargate run 3, `dvx-cells-smoke`)

With the workaround command (`run --commit --push each -v <cells targets>`) the whole batch stack works — until the DAG's interior. On the fresh clone, per-year `Accidents.pqt` stages split nondeterministically: 9 skipped, **19 re-ran and failed** on their unmaterialized `.txt` deps (`FileNotFoundError: njdot/data/<year>/...Accidents.txt`). Local Docker runs of the same targets split 6/22 — scheduling-order-dependent.

Mechanism: the upstream txt stages are *pruned as fresh* (recorded md5s all consistent), so they're not in the plan and never materialize; the pqt stage's `is_output_fresh` then reports `dep missing` (file absent on disk) — a reason auto-pull doesn't cover (it fires only on `output missing`) — so the stage re-runs against inputs that don't exist.

Fix shape: when a stage is stale *only* because dep files are absent while every recorded md5 is consistent (dep `.dvc`s agree), materialize the deps (pull by recorded md5) instead of re-running — or extend `_try_materialize_from_remote` to cover the stage's dep closure before classifying. Invariant to test: **on a fresh clone whose recorded closure exists in the remote, `dvx run <target>` executes zero cmds.** This blocks every fresh-machine `dvx batch` run of a real DAG; it's the last piece between nj-crashes and a working Phase-1 batch reproc.

## Resolution

All three fixed:

**§1 — CLI**: `dvx run -c/--commit` became a paired tri-state flag
(`--commit/--no-commit`, default `None`): bare `--commit` → `always`
(BC with `daily.yml`-style `dvx run --commit --push each`),
`--no-commit` → `never`, absent → `auto`. This deviates from the
spec's "value modes" suggestion deliberately — click 8.3's
`is_flag=False, flag_value=...` optional-value pattern doesn't
actually parse a bare `--commit` (probed: `Option '--commit' requires
an argument`), and a value-taking flag would swallow an adjacent
target (`dvx run --commit foo.dvc`). The paired form has zero
ambiguity. `--push` grew a `never` choice.

`run_command` now emits `--no-commit` for `commit="never"` (bare
`--commit` for `always`, omitted for `auto`) and round-trips through
`dvx run`'s real click parser in tests
(`test_run_command_parses_with_real_run_cli` covers every mode —
the integration-shaped gap this spec called out).

**§2 — push hoisted**: `_handle_stage_output` resolves the push
strategy *before* the commit branch; `_push_cache_blobs` now runs on
`--push each` (or a stage's explicit `stage.push()`) regardless of
commit strategy. Git push stays commit-gated. The `--push end` path
was already commit-independent. Test:
`test_no_commit_still_pushes_cache_blobs` (HEAD unchanged; blob in
remote).

**§3 — dep materialization**: `_should_run`'s pre-pass became a
bounded classify→pull loop over both triggers: `output missing` pulls
the stage's own `.dvc`; `dep missing` pulls the absent dep files —
via the dep's own `.dvc`, or its **parent tracked-dir `.dvc`**
(`find_parent_dvc_dir`) for files inside tracked directories (the
nj-crashes shape: the dir stage isn't in the plan because nothing
deps on the dir as recorded). Each iteration must change the
freshness classification or the loop bails to the rerun. Test:
`test_fresh_clone_dep_inside_tracked_dir_materializes` asserts the
invariant verbatim — fresh clone + full remote closure ⇒
`Executed: 0`, everything `fetched (up-to-date)`. Verified red
pre-fix (both §2 and §3 tests fail against the pre-fix executor).
