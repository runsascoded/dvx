# `write_dvc()` should preserve committed `outs` when the output isn't materialized locally

Status: proposed (2026-08-31). Surfaced by the ctbk (`~/c/hccs/ctbk`) reproducibility-audit session as the blocker for backfilling provenance onto already-produced-but-not-checked-out outputs. Companion to the ctbk-side recorder fix (which routes `create`/`update` through `write_dvc()` so new outputs carry provenance) — this makes the *backfill* of existing bare `.dvc`s a clean `prep`, not a ruamel post-patch.

## The bug

`Artifact.write_dvc()` (`src/dvx/run/artifact.py`) computes the output hash only when the file exists locally; otherwise it writes a **placeholder** `.dvc` with no `md5`/`size`:

```python
md5 = self.md5
size = self.size
if md5 is None and path.exists():
    md5 = compute_md5(path)
    size = compute_file_size(path)
# If still no hash, leave as None - write_dvc_file omits these fields
```

But `prep` (and any `write_dvc()` caller) frequently runs against an output that is **already produced and committed** — its `.dvc` on disk carries a valid `outs` block — just not *materialized* in the working tree (it lives in the cache/remote, was never `dvx pull`ed, or is a multi-GB dir nobody wants to download). In that case `write_dvc()` **overwrites the committed `.dvc`, dropping its `outs`** and replacing the whole file with just `meta.computation`.

Observed in ctbk (2026-08-31), `ctbk agg prep -g e -a c 202512` on a non-local output:

```diff
-outs:
-- md5: a06fb2c47ae86e4b652e59ec9d132aec
-  size: 25204
-  hash: md5
-  path: e_c_202512.parquet
+meta:
+  computation:
+    cmd: ctbk agg create -w0 -g e -a c 202512
+    deps:
+      /s3/ctbk/normalized/202512.parquet: a61a93f13d32b16e1d375680c6e64122
```

The `outs` are gone. So `prep` is unusable for its natural job — recording provenance onto existing artifacts — unless every output is first pulled locally. That's the difference between a cheap metadata sweep and downloading GBs of dir outputs.

## The fix

When `write_dvc()` can't compute a hash (output not local) **and** a committed `.dvc` already exists at `f"{path}.dvc"`, reuse that spec's `outs` (md5, size, and any `nfiles`/`hash`/dir markers) instead of writing a placeholder. Only fall through to the placeholder when there is genuinely no prior spec — the true "output doesn't exist yet" two-phase-prep case.

Sketch (adjust to the codebase's `read_dvc_file`/`DVCFileInfo` accessors):

```python
if md5 is None and path.exists():
    md5 = compute_md5(path); size = compute_file_size(path)
elif md5 is None:
    prior = read_dvc_file(Path(f"{path}.dvc"))  # None if absent
    if prior and prior.md5 is not None:
        md5, size = prior.md5, prior.size
        # also carry nfiles / is_dir / hash-name through write_dvc_file
```

Multi-output stages (co-outputs) should preserve each committed out by path. The dir case matters most for ctbk (norm dirs carry `nfiles`), so the preserved fields must include `nfiles` and the `.dir` md5 form.

This makes `prep` **idempotent and non-destructive**: it records/refreshes `meta.computation` and leaves committed `outs` intact whether or not the output is materialized. It changes nothing for the `run`/materialize path (there the output was just built, so `path.exists()` and the first branch fires) — verified against ctbk's full reproc, which rebuilds every output locally.

## TFFP

1. Write a `.dvc` with a valid `outs` block (md5/size, and a dir variant with `nfiles`); do **not** create the output file.
2. Build an `Artifact(path, computation=Computation(cmd=..., deps=[...]))` and call `write_dvc()`.
3. Assert the resulting `.dvc` **retains** the original `outs` (md5/size/nfiles unchanged) **and** gains `meta.computation` (cmd + deps). Pre-fix: `outs` is dropped.
4. Regression guard: with the output file present, `write_dvc()` still recomputes from disk (unchanged behavior).

## Downstream (ctbk)

Unblocks the head-month provenance backfill: `ctbk <stage> prep -d 202512-202607` across the 9 stage families (~72 bare `.dvc`s CI added via the old provenance-blind `dvx add`) becomes a clean, download-free metadata sweep — no ruamel patching. Requires bumping ctbk's dvx pin (`9c22fc08c` → the SHA carrying this fix, on top of `95db406f7` to align with the reproc container). The container/reproc image needs no rebuild — `dvx run` never hits the placeholder branch (it rebuilds outputs locally).

## Resolution

Implemented in `src/dvx/run/artifact.py` (`Artifact.write_dvc`). When the
output hash is unknown and the output isn't materialized locally, `write_dvc`
now reads the committed `.dvc` via `read_dvc_file(path)` and, if it carries
real outputs, passes them straight through as `outs=` to `write_dvc_file`:

```python
if md5 is None and path.exists():
    md5 = compute_md5(path); size = compute_file_size(path)
elif md5 is None:
    prior = read_dvc_file(path)
    if prior and prior.outs and any(o.md5 for o in prior.outs):
        preserved_outs = prior.outs
...
return write_dvc_file(..., outs=preserved_outs)
```

`prior.outs` is a list of `OutputInfo` (path/md5/size/is_dir/nfiles) parsed
verbatim, so every committed output — file or dir, including the `.dir` md5
suffix and `nfiles` — round-trips unchanged, and multi-output stages preserve
each entry by path. Only the genuine "no prior spec, output absent" case falls
through to the existing side-effect-shaped placeholder (cmd + deps, no `outs`).
The `run`/materialize path is untouched: there the output was just built, so
`path.exists()` and the hash is recomputed from disk.

Tests (`tests/test_run_artifact.py`):

- `test_write_dvc_preserves_committed_outs_when_output_absent` — file output
  absent + committed `.dvc`; asserts `outs` retained exactly and
  `meta.computation` (cmd + deps) added. TFFP-verified (pre-fix: `outs`
  dropped → `KeyError`).
- `test_write_dvc_preserves_committed_dir_outs_with_nfiles` — dir output; the
  `.dir` md5 + `nfiles` are preserved.
- `test_write_dvc_placeholder_when_no_prior_and_output_absent` — genuine
  two-phase-prep case still writes the placeholder (no `outs`).
- `test_write_dvc_recomputes_from_disk_when_output_present` — regression guard:
  a materialized output is re-hashed, ignoring stale committed `outs`.

Full suite green (371 passed, 4 skipped).

### Downstream (ctbk)

Unblocks the head-month provenance backfill: bump ctbk's dvx pin to the SHA
carrying this fix, then `ctbk <stage> prep -d 202512-202607` across the 9 stage
families is a download-free metadata sweep — no ruamel patching. The reproc
container needs no rebuild (`dvx run` never hits this branch).
