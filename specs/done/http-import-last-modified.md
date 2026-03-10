# Store `Last-Modified` for HTTP imports

## Context

`dvx import-url` tracks HTTP resources by storing `ETag` (as `checksum`) and `size` in `.dvc` deps. However, it doesn't capture the `Last-Modified` header, which is useful for:

1. **Audit trail**: knowing *when* an upstream source was published (not just *that* it changed)
2. **Scheduling**: downstream consumers can analyze publication cadence to set cron intervals
3. **Freshness checks**: `Last-Modified` is more human-readable than ETags for quick inspection

### Motivating use case

The [PATH ridership project][path] imports PDFs from the Port Authority website (~monthly). The PDFs are currently git-tracked, but should switch to `dvx import-url`. To automate the update pipeline, we need to know when PDFs are published (≈2-month lag after the data month). Recording `Last-Modified` on each `import-url` / `update` would build a history of publication timestamps in the `.dvc` file git log, enabling data-driven cron scheduling.

[path]: https://github.com/hudcostreets/path

## Current behavior

`Meta.from_info()` in `dvc_data/hashfile/meta.py` extracts HTTP metadata:

```python
elif protocol and protocol.startswith("http") and ("ETag" in info or "Content-MD5" in info):
    checksum = info.get("ETag") or info.get("Content-MD5")
```

The `Meta` class has a `PARAM_MTIME = "mtime"` field, but it's not populated from HTTP `Last-Modified`.

### What gets stored today

```yaml
deps:
- path: https://example.com/data.pdf
  checksum: '"abc123"'    # ETag
  size: 651234
```

### What's missing

- `Last-Modified` header is not captured or stored
- `update --no-download` on HTTP imports doesn't re-check metadata (only repo/db imports get updated in `imports.py` lines 58-59)

## Proposed changes

### 1. Capture `Last-Modified` in `Meta.from_info()`

When the protocol is HTTP(S), extract `Last-Modified` from the fsspec `info` dict and store it in the `mtime` field (which already exists on `Meta` but isn't serialized for HTTP):

```python
elif protocol and protocol.startswith("http"):
    if "ETag" in info or "Content-MD5" in info:
        checksum = info.get("ETag") or info.get("Content-MD5")
    last_modified = info.get("Last-Modified")
    if last_modified:
        from dateutil.parser import parse
        mtime = parse(last_modified).timestamp()
```

Then ensure `mtime` is included in `to_dict()` serialization (it currently is, but only when set).

### 2. Serialize `mtime` in `.dvc` deps

The resulting `.dvc` file should look like:

```yaml
deps:
- path: https://example.com/data.pdf
  checksum: '"abc123"'
  size: 651234
  mtime: 1739282189.0     # 2025-02-11T14:56:29Z
```

This is already handled by `Meta.to_dict()` if `mtime` is set — just need to populate it.

### 3. Fix `update --no-download` for HTTP imports

In `dvc/stage/imports.py`, the `update_import()` function only calls `dep.update()` for `repo` or `db` imports when `no_download=True`. HTTP imports should also get their metadata refreshed:

```python
# Current (imports.py ~line 56-59):
if no_download and isinstance(stage.deps[0], (RepoDependency, DbDependency)):
    stage.deps[0].update(rev=rev)

# Should also handle HTTP deps:
if no_download:
    dep = stage.deps[0]
    if isinstance(dep, (RepoDependency, DbDependency)):
        dep.update(rev=rev)
    elif hasattr(dep.fs, 'protocol') and dep.fs.protocol.startswith('http'):
        dep.save()  # Re-fetch metadata from URL
```

This way `dvx update --no-download <target>.dvc` re-checks the ETag and Last-Modified without downloading the file.

### 4. Human-readable mtime format (optional)

Consider storing mtime as an ISO 8601 string instead of a Unix timestamp for readability in `.dvc` files:

```yaml
mtime: '2025-02-11T14:56:29+00:00'   # vs 1739282189.0
```

This would require a minor change to `Meta.to_dict()` / `Meta.from_dict()` serialization, but makes git diffs and manual inspection much more useful.

## Testing

1. `dvx import-url https://httpbin.org/response-headers?Last-Modified=... -o test.dat`
   - Verify `.dvc` file contains `mtime` field
2. `dvx import-url --no-download <url> -o test.dat`
   - Verify `mtime` is captured even without download
3. `dvx update --no-download test.dat.dvc`
   - Verify metadata is refreshed (new ETag/mtime if source changed)
4. Verify backward compatibility: existing `.dvc` files without `mtime` still work

## Scope

This is a small, focused change:
- `dvc_data/hashfile/meta.py`: populate `mtime` from HTTP `Last-Modified`
- `dvc/stage/imports.py`: allow `update --no-download` for HTTP deps
- Tests for the above

No changes to the DVX wrapper layer needed — it's all in the underlying DVC code. If DVC is vendored/forked, the changes go there; otherwise this could be an upstream DVC PR.

## Implementation status

All items done:
- Items 1–2: `dvc-data` pinned to `9b27dc6` which captures HTTP `Last-Modified` → `Meta.mtime` via `from_info()`. `_compat.py` patches DVC schema + `Meta.to_dict()` to serialize `mtime` for HTTP sources (distinguished from local fs mtime by absence of `inode`)
- Item 3: `update --no-download` already works for HTTP deps — `update_import()` calls `deps[0].update()` unconditionally, which re-fetches metadata
- Item 4: `mtime` serialized as ISO 8601 string (e.g. `'2003-11-07T05:51:11+00:00'`); `_compat.py` patches `Meta.from_dict()` to parse ISO strings back to float. Legacy float values are accepted for backward compat
