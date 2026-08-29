# `output hash changed` warning

## Problem

`dvx run` had no way to say that a stage it just reran produced *different bytes* than its `.dvc` recorded. The `.dvc` was rewritten, the new blob cached and pushed, and the log said:

```
  ✓ njdot/data/2023/crashes.parquet: completed (41.2s)
```

— exactly what a byte-for-byte reproduction says. The two outcomes were indistinguishable from the run's output, its exit code, and its summary.

That's a gap in ordinary use (a stage silently changing its output is worth noticing) and a hole in the one workflow whose entire purpose is to detect it. nj-crashes ran a full-DAG reproducibility audit on AWS Batch — regenerate all 136 stages from scratch against a scratch cache, compare to prod. It reported "243 outputs byte-identical, 0 divergences" for three rounds. The real number was 3 of 122: a `pyarrow` 20→21 bump had changed every parquet output's footer, and ~105 of them changed size by ~2.4%. Nothing in `dvx run`'s output could have said so, because `dvx run` never looked.

The recorded md5 was right there in the `.dvc` DVX had just read, and the produced md5 right there in the value it was about to write. The comparison cost nothing; it simply wasn't made.

## Model

One warning per drifting **output**, not per stage — a stage whose 2nd of 3 outputs drifted should name that output:

```
  ⚠ <out path>: output hash changed (recorded <md5> → produced <md5>)
```

Both hashes appear, so a reader can look either one up in the cache without re-deriving it.

**Size delta, when the size moved:**

```
  ⚠ <path>: output hash changed (recorded … → produced …); size 8,412,003 → 8,613,441 (+201,438 B, +2.4%)
```

Same-size drift and size-changing drift have different causes — a parquet footer's `created_by` string versus an encoding or content change — and separating them at a glance is most of the diagnosis. Appending the delta only when it's nonzero keeps the common line short and makes the presence of the clause itself informative.

**A first recording is not a change.** A `.dvc` declaring `outs: [{path: x}]` with no `md5` yet has nothing to differ from. Guard is `if not recorded_md5` — which also covers the `md5: ""` that `read_dvc_file` produces for that shape.

**It is a warning, not a failure.** Regenerating changed bytes is legal and usually intended; the stage succeeded. Turning drift into an error would break every ordinary rerun. So the signal has to survive some other way, which is what the summary is for.

## Summary line

`ExecutionResult.hash_changed` carries it out of the executor, and `dvx run` prints:

```
Summary:
  Total: 136
  Executed: 130
  Skipped: 6
  Hash changed: 119
```

Omitted when zero, like `Failed`. This is the number an audit reads — a 136-stage log is not something anyone scrolls, and "119" versus "0" is the whole result.

`hash_changed` is per-*stage* (true if any of its outs drifted), while the log lines are per-*out*. A multi-out stage with two drifting outs logs two lines and counts once. The summary answers "how many stages didn't reproduce", the log answers "which files".

## Surface

| Site | Recorded from | Label |
| --- | --- | --- |
| Multi-out loop | `declared.md5` / `declared.size` (the `.dvc`'s `outs[i]`) | the out's own path |
| Single-out | `existing_info.md5` / `.size` | the stage path |
| Co-output | `read_dvc_file(out)` — this path doesn't otherwise read it | the co-output path |

`existing_info` is read after the cmd runs but before `write_dvc_file`, so it still holds the pre-run recorded values.

Side-effect stages have no output to hash, so they're not in scope.

## Resolution

Implemented as `ParallelExecutor._report_hash_change`, called from all three hashing sites; `ExecutionResult.hash_changed`; the `Hash changed:` summary line in `dvx/cli/run_cmd.py`.

`tests/test_hash_change_warning.py` covers: both hashes in the line; the size clause appearing only on a size change; a byte-identical rerun staying silent; a first recording not warning; per-out labelling in a multi-out stage (the 2nd of 3 drifts, one line, named `b.txt`); a co-output drifting while its primary reproduces exactly; and the summary line's presence and absence.

The tests' `_stage()` helper records a hash and then dirties the file on disk — a *fresh* stage is skipped, and a skipped stage hashes nothing, so there'd be no comparison to make. That's also why the first-recording test uses an `outs:`-with-no-`md5` `.dvc` rather than `write_dvc_file(cmd=...)` alone: the latter has no `outs` at all and is read as a side-effect stage, which is fresh by definition.

### Not done

- **No `--fail-on-hash-change`.** Plausible for CI ("this pipeline must be reproducible"), but nothing needs it yet, and the summary count plus a nonzero grep serves the same purpose from a shell.
- **No diff of *what* changed.** `dvx diff` already does that, and the warning's job is to tell you to go look.
