# Foreign S3 import: fix dvc-data bug + expose `--no-download` + `dvx update`

Track files in external/foreign S3 buckets (e.g. `s3://materialsproject-parsed/chgcars/`,
~198K samples, ~3-4TB) without mirroring data to a DVC remote. Record ETags for
change detection; pull on demand directly from source.

## Approach: upstream bug fix

DVC's `import-url --no-download` + `dvc pull` was broken since DVC 3.43.0 due to
a one-line bug in `dvc-data`'s `fetch.py:79` ([treeverse/dvc#10594]).

**Fix:** `Meta.from_info(info)` → `Meta.from_info(info, data_fs.protocol)` in
`src/dvc_data/index/fetch.py` line 79. S3's `s3fs` returns ETags with uppercase
`"ETag"` key; without the `protocol` arg, `Meta.from_info` only checks lowercase
`"etag"`, so `meta.etag` is always `None`.

**Fork:** [runsascoded/dvc-data] @ `9956cd7` has the fix. DVX pins to this via
`pyproject.toml`. PR submitted upstream.

This eliminates the need for a custom direct-source pull workaround in DVX.

## Changes made

1. **dvc-data fork fix** — one-line fix in `fetch.py:79`
2. **`pyproject.toml`** — pin `dvc-data` to fork commit `9956cd7`
3. **`src/dvx/cli/external.py`** — expanded `import-url` with `-F/--fs-config`,
   `-N/--no-download`, `-V/--version-aware`; added `update` command with
   `-N/--no-download`, `-r/--recursive`
4. **`src/dvx/repo.py`** — expanded `imp_url()` signature; added `update()` method
5. **`src/dvx/cli/main.py`** — registered `update` command
6. **`README.md`** — updated DVC link to GitHub repo

## Usage

```bash
# Track a public S3 file (metadata only)
dvx import-url --no-download \
  --fs-config allow_anonymous_login=true \
  s3://materialsproject-parsed/chgcars/mp-1775579.json.gz \
  -o data/test.json.gz

# Pull from source (bug fix enables this)
dvx pull data/test.json.gz

# Re-check ETag without downloading
dvx update --no-download data/test.json.gz.dvc
```

[treeverse/dvc#10594]: https://github.com/treeverse/dvc/issues/10594
[runsascoded/dvc-data]: https://github.com/runsascoded/dvc-data
