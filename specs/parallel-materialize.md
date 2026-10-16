# Parallel dep materialization; `batch submit` passthrough of `run` flags

From jc-taxes (2026-09-28): a Batch (Fargate) job whose stage depends on a ~70k-file tracked dir (`data/cache`, ~600 MB of small `.json.gz`) spent 3+ h pulling it without finishing; locally the same pull runs at ~67 objects/s.

## 1. `materialize_targets` fetches serially

`cache.py` `materialize_targets` (≈1476–1537) walks the `.dir` manifest and calls `_fetch_blob` (one `odb.fs.get`) + `_checkout_file` per entry, one at a time. For many-small-object dirs, per-request latency dominates (R2 from us-east-1 Fargate is much slower per GET than from a laptop).

Proposal: fetch blobs through a thread pool (e.g. `min(32, n)` like the batch existence check at ≈1208–1241, or the run's `-j`), then check out. Keep the `dest.exists()` skip. Log progress (N/M) at intervals so a long pull is visible in Batch logs (today the log sits at `Level 1/1: 1 computation(s)` with no output).

## 2. `dvx batch submit` can't pass `run` flags like `--cached`

With `-f`, `force` applies to every artifact in the plan, including an upstream whose output is merely absent in the fresh container (it's in the remote). That upstream then re-runs, pulling its own huge deps. `dvx run -f --cached 'data/hls/*'` avoids it (cached wins over force; the downstream's dep pull materializes the upstream's output), but `batch submit` has no way to pass `--cached`; jc-taxes had to `aws batch submit-job` with a `containerOverrides.command`.

Proposals (either):
- `batch submit` passes through arbitrary `run` args after `--` (`dvx batch submit -P jct -f -- --cached 'data/hls/*' targets…`), and/or grows `--cached` / `--force-upstream`.
- Make `-f` mean "force the named targets", not upstreams that are absent-but-fetchable: an upstream whose output is in the remote and whose deps are fresh by `.dvc` md5 should be materialized, not forced (`--force-upstream` remains the way to force those).

## Minor

`-j` help says "default: CPU count", but omitting it passes `None` to `ThreadPoolExecutor` (Python's `min(32, cpu+4)`).
