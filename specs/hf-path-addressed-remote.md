# HuggingFace as a path-addressed DVX remote

## Problem

DVX's existing remote model (DVC-compatible) is **hash-addressed**: blobs
are stored in the remote under `cache/files/md5/{ab}/{rest}`. This is
optimal for dedup and backend-agnostic, but it means the remote isn't
useful for anything else — a `git clone` of the remote doesn't give you
a readable dataset, just a hash-keyed blob tree.

HuggingFace repos are **path-addressed**: files live at their real paths
(`njsp/data/crashes.parquet`), backed by LFS or Xet content stores. This
makes HF repos directly readable as datasets without DVX.

We want DVX to fetch/push against an HF repo *without moving files into
DVX's hash-keyed layout*, so the HF repo remains a first-class readable
dataset for non-DVX consumers.

## Design: `layout: path` remote

Today a DVX remote is implicitly hash-addressed. Add a `layout` option:

```yaml
# .dvx/config.yml
remote:
  hf:
    url: hf://runsascoded/nj-crashes
    ref: main                  # default branch/ref for fetch
    layout: path               # default: hash (DVC-compatible)
```

With `layout: path`, DVX:

- **Fetch**: given a local path `njsp/data/crashes.parquet`, fetch from
  `hf://runsascoded/nj-crashes/njsp/data/crashes.parquet@ref`. Verify
  MD5 after download against the `.dvc` file's recorded hash.
- **Push**: upload blob to the same path in the HF repo. Write a Git
  commit to the HF repo (message derived from local commit or stage).

Paths in the local DVX repo correspond 1:1 to paths in the HF repo. No
per-`.dvc` `hf:` metadata needed — the DVX `.dvc` file is the same in
both layouts; the difference is entirely in how the remote is accessed.

## 2-way bind

The isomorphism:
- DVX `.dvc` file at `njsp/data/crashes.parquet.dvc` with `md5: abc123`
- HF file at `njsp/data/crashes.parquet` (revision has MD5 abc123)

Given either side, you can derive the other:
- Forward (DVX → HF): `path` in `.dvc` file → HF repo path.
- Reverse (HF → DVX): HF path + MD5 of its content → DVX output path + hash.

Consumers:
- **Plain `git clone`** of the HF repo gives a usable dataset.
- **`dvx pull`** from the HF remote gives the same files but with
  DVX-managed freshness / pipeline state / provenance.

## Protocol

Backed by the `huggingface_hub` Python client (or raw HTTP + LFS
pointer resolution for read-only use cases). Two main operations:

```python
# Fetch: resolve HF path at ref, download file to local cache/workspace
hf_hub.hf_hub_download(
    repo_id="runsascoded/nj-crashes",
    filename="njsp/data/crashes.parquet",
    revision=ref,
    local_dir=...,
)

# Push: upload file + git commit
hf_hub.upload_file(
    path_or_fileobj="njsp/data/crashes.parquet",
    path_in_repo="njsp/data/crashes.parquet",
    repo_id="runsascoded/nj-crashes",
    commit_message="Update crash data",
)
```

For Xet-backed repos, the client handles CDC under the hood —
DVX doesn't need to know.

## Hash model: coexistence with current MD5

DVX today uses whole-file MD5 (inherited from DVC). HF uses SHA256
(LFS) or chunked SHA256 (Xet). These are independent:

- DVX computes MD5 on fetch to verify content (one full read).
- HF's internal hashes are not exposed as DVX's trust anchor.
- Extra hashing cost ≈ one filesystem scan per fetched file; amortized
  against the network transfer, it's negligible.

This is the "works today" story. See the migration section for the
longer-term SHA256/CDC alignment.

## Remote configuration

Remotes are configured via `.dvx/config.yml` (extends the existing
config schema):

```yaml
# .dvx/config.yml
remote:
  default: hf
  hf:
    url: hf://runsascoded/nj-crashes
    ref: main
    layout: path
    token_env: HF_TOKEN        # env var with an HF API token (for push / private repos)
  s3:
    url: s3://njsp-backup
    layout: hash               # default, can omit
```

`hf://user/repo` is DVX's own URL scheme. It dispatches to
`huggingface_hub` under the hood.

## URL scheme

- `hf://user/repo` — repo ref defaults from config
- `hf://user/repo@ref` — explicit ref override
- `hf://user/repo@ref/path/to/file` — full path reference (for
  `dvx import-url hf://...`-style one-off imports)

`dataset`/`model`/`space` kind: default to `dataset`; `hf://user/repo?type=model`
or a config key for non-dataset repos.

## Local layout equivalence

For a path-addressed remote to work, the DVX repo layout must mirror
the HF repo layout. This is the natural case: if `crashes.parquet.dvc`
is at `njsp/data/crashes.parquet.dvc` locally, the HF repo has
`njsp/data/crashes.parquet` at the same path.

If the layouts diverge (e.g. DVX has `data/crashes.parquet.dvc` but HF
has `njsp/data/crashes.parquet`), a `path_prefix` remote option can
handle simple cases:

```yaml
remote:
  hf:
    url: hf://runsascoded/nj-crashes
    path_prefix: njsp/         # HF repo path = path_prefix + DVX path
```

More complex rewrites are out of scope for P1.

## Read-only first

Initial implementation: read-only fetch from HF. Push comes later
(needs to decide commit strategy, handle conflicts, etc.). Read-only
is already the most valuable case — it lets anyone publish an HF
dataset and have DVX consume it with full pipeline semantics.

## Integration with existing commands

- `dvx pull [target]`: if `remote.default` is an HF path-remote, fetch
  via HF. Otherwise behaves as today.
- `dvx import-url hf://user/repo/path@ref`: one-off import, creates a
  `.dvc` file with the recorded MD5. Already mostly works through the
  existing `import-url` path if we register an `hf://` URL handler.
- `dvx push`: deferred to P2 (write support).

## Migration (SHA256/CDC alignment)

A separate future spec will address whether DVX should adopt SHA256
(or chunked SHA256 matching Xet) as its canonical hash. This PR does
not change DVX's hash model — it just adds the path-addressed remote.

### Liftover for MD5 → SHA256 migration

If/when DVX adopts SHA256, pinning the transition to a specific git
ref avoids re-hashing the entire history:

- Before ref `R`: `.dvc` files contain MD5 hashes (legacy).
- At/after ref `R`: `.dvc` files contain SHA256 hashes (and/or chunk
  roots).
- A **liftover map** translates legacy MD5 → SHA256, populated lazily
  as blobs are accessed. Storage: a local SQLite table (same DB as the
  P2 relation DB from the GC spec), or a side file in the cache.

This means the SHA migration can happen gradually: the liftover lets
`dvx checkout -R <old-ref>` still resolve MD5-era `.dvc` files by
looking up the SHA256 for each recorded MD5 and fetching via the
modern path.

## Out of scope

- Push support (P2).
- Non-dataset HF repo types (model, space) — probably work with the
  same protocol, but untested.
- Xet chunk-level fetch (DVX would still pull whole files; Xet handles
  the chunking opaquely).
- Bi-directional sync / conflict resolution.

## Test plan

- Unit: URL scheme parsing (`hf://user/repo@ref/path`).
- Integration: fetch a small public HF dataset, verify MD5, round-trip
  through `dvx pull`.
- Integration: `dvx import-url hf://...` creates a valid `.dvc` file.
- Unit: `path_prefix` rewrite.
- Unit: fetch with/without explicit ref override.
