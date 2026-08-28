# `_wait_for_co_outputs` deadlocks when a same-level co-output is skipped as fresh

nj-crashes Fargate runs 7 and 8 (jobs `7c44b15f`, `d70382bb`), both at the same point, both silent and indefinite. Run 8 was on `e4896b575`, so the new bounded/instrumented push is **not** implicated — its pre-push log line (`📤 pushing N object(s)`) never printed, which proves the hang happens *before* `_push_cache_blobs` is reached. My earlier "unbounded S3 upload" theory in `push-each-hang-after-stage.md` was wrong; that fix is still good (and its logging is what let me exclude it in one run), but it wasn't this bug.

## Mechanism

`njsp match_njdot` produces two co-outputs, both scheduled in Level 4:

```
Level 4/7: 2 computation(s)
  ○ njsp/data/njsp_njdot_match.parquet: up-to-date        <- SKIPPED (fresh)
  ⟳ njsp/data/njsp_njdot_residuals.parquet: running...    <- STALE, executes
  ✓ njsp/data/njsp_njdot_residuals.parquet: completed (47.1s)
<silence; killed at 46 min / 13 min>
```

After the primary stage completes, it calls `_wait_for_co_outputs(cmd, my_path)`, which selects same-level co-outputs (`p in self._scheduled_paths`) and does an **unbounded `ev.wait()`** on each one's `_dvc_done_events` entry.

`match` *is* in `_scheduled_paths` (same level, so it passes the cross-level filter), but it took the **skip path** — `_execute_artifact`'s `if not should_run:` early-return at ~line 483 returns `ExecutionResult(skipped=True)` **without calling `_signal_dvc_done(path)`**. The only two signal sites (~667, ~788) are on the execute and co-output-handler paths. So the event is never set, and the primary waits forever.

The existing cross-level guard reasons about artifacts whose `_execute_artifact` is *never invoked*. This is the sibling case the guard misses: invoked, but returning early.

## Why it took until now to surface

It needs one co-output stale while a same-level sibling is fresh. That's the "co-output stamp divergence" I flagged in `parallel-pull-lock-contention.md` §secondary: the daily CI runs `$DVX njsp/data/njsp_njdot_match.parquet.dvc` as a targeted stage, which re-stamps `match.dvc` but leaves `residuals.dvc`'s dep stamps days behind. On dev machines both are usually fresh (nothing runs) or both stale (full run), so the divergent state is a CI/Batch artifact. It also required every *other* stage to be correctly fetched — i.e. it was hidden behind the six earlier bugs until run 7 finally got a stage to execute.

## Fix

Signal on **every** exit from `_execute_artifact`, not just the executing ones — most simply `try/finally: self._signal_dvc_done(path)` around the body, which also covers the `MaterializeError` and failure returns (a failed co-output currently strands its primary the same way).

Belt-and-braces, per the "no indefinite waits" principle already applied to push: give `ev.wait()` a bounded timeout and log `⚠ co-output <path> never signalled (Ns) — proceeding` rather than hanging. A deadlock is strictly worse than a slightly-incomplete `git add -u`.

Test shape: two co-outputs of one cmd in one level, one fresh / one stale, assert the run completes (it currently hangs). The nj-crashes case reproduces deterministically on Fargate but should reduce to a unit test with two `.dvc`s sharing a `cmd` and divergent dep stamps.

## Resolution (dvx, 2026-08-28)

Both the fix and the belt-and-braces guard, as specified.

**Signal on every exit** — `_execute_artifact` is now a thin try/finally wrapper
around `_execute_artifact_inner`, signalling `_signal_dvc_done(path)` unconditionally.
That covers the skip path, the `MaterializeError` return, and the failed-cmd
returns; `Event.set()` is idempotent, so the two mid-body signal sites are
unaffected. This mirrors `_handle_co_output`'s existing try/finally, which fixed
the same class of hang for *failed* co-outputs — the skip case was its blind spot.

**Bounded wait** — `_wait_for_co_outputs` now waits with
`ExecutionConfig.co_output_timeout` (default 1800s) and logs
`⚠ co-output <path> never signalled (1800s) — proceeding without it` instead of
blocking forever. No CLI flag: with the signal fix in place this is unreachable
except from a genuinely wedged thread, so it's a safety net rather than a knob.

**Test** — `test_stale_co_output_does_not_deadlock_on_skipped_sibling`: two
co-outputs of one cmd in one level, `a.txt` fresh (output + recorded md5) and
`b.txt` stale, asserting the run completes. Verified all three states:

| executor state | outcome |
| --- | --- |
| before the fix | hangs indefinitely (pytest itself couldn't exit — the pool threads were blocked) |
| bounded wait only (signal removed, timeout 3s) | completes in 6s with the warning |
| as shipped | completes immediately, no warning |

### Also: push log phrasing

Taking the run-9 observation. `📤 pushing 3 objects (83.8 MB)...` → `📤 cache pushed
(0 blobs)` did read like a failure. The completion line is now uniformly
`📤 cache pushed (N new, M already in remote)`, so the byte-identical-rebuild case
reports `(0 new, 3 already in remote)` and the pre-push count always reconciles.

### On the earlier theory

Agreed the "unbounded S3 upload" reading was wrong — and worth stating plainly here
so the spec record is accurate: `push-each-hang-after-stage.md`'s *symptom* (silent
indefinite hang after the first executed stage) was this deadlock, not the push.
That fix stands on its own — lock-free, bounded, and instrumented is right
regardless — and its pre-push log line is what let run 8 exclude the push in one
run instead of another round of guessing.
