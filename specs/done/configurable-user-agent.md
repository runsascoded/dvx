# Configurable User-Agent for HTTP imports

## Context

`dvx import-url --git` sends `User-Agent: dvx/0.1` which gets 403'd by sites with bot protection (e.g. `njsp.njoag.gov` uses Cloudflare). A browser-like User-Agent works fine via `curl -H "User-Agent: Mozilla/5.0 ..."`.

## Proposed behavior

### 1. CLI flag: `--user-agent` / `-A`

```bash
dvx import-url --git -A "Mozilla/5.0" \
  https://njsp.njoag.gov/.../2024-UCR.xlsx \
  -o crime/2024-UCR.xlsx
```

### 2. Stored in `.dvc` file

The User-Agent is needed for subsequent `dvx update` calls too, so persist it in the `.dvc` deps:

```yaml
deps:
- path: https://njsp.njoag.gov/.../2024-UCR.xlsx
  checksum: '"etag"'
  size: 204114
  mtime: '2026-02-24T00:00:00+00:00'
  user_agent: 'Mozilla/5.0 (compatible; dvx/0.1)'
outs:
- md5: e2154bc8...
  path: 2024-UCR.xlsx
meta:
  git_tracked: true
```

`dvx update` reads `user_agent` from the dep and uses it for HEAD/GET requests.

### 3. Global config fallback

```bash
dvx config http.user_agent "Mozilla/5.0 (compatible; dvx/0.1)"
```

Stored in `.dvc/config` (or `.dvc/config.local`). Per-dep `user_agent` in `.dvc` overrides the global config.

### 4. Default

Keep `dvx/0.1` as the default (honest about what we are). Only override when needed.

## Implementation

### `src/dvx/git_import.py`

```python
def _get_headers(user_agent: str | None = None) -> dict:
    ua = user_agent or dvc_config_get("http.user_agent", "dvx/0.1")
    return {"User-Agent": ua}

def git_import_url(url, out, no_download=False, user_agent=None):
    headers = _get_headers(user_agent)
    req = Request(url, headers=headers)
    # ... download ...
    # Store user_agent in dep if non-default
    dep_info = {"path": url, ...}
    if user_agent:
        dep_info["user_agent"] = user_agent

def update_git_import(dvc_path, no_download=False):
    # Read user_agent from existing dep
    dep = load_dvc(dvc_path)["deps"][0]
    user_agent = dep.get("user_agent")
    headers = _get_headers(user_agent)
    # ... HEAD/GET with headers ...
```

### `src/dvx/cli/external.py`

Add `-A`/`--user-agent` option to `import-url` and `update`.

### DVC core (`dvc_data`)

For non-git-tracked HTTP imports (regular `import-url`), DVC uses fsspec's `HTTPFileSystem`. User-Agent can be passed via `client_kwargs`:

```python
fs = HTTPFileSystem(client_kwargs={"headers": {"User-Agent": ua}})
```

This is already configurable via `--fs-config` but that's verbose. The `http.user_agent` config key would be a convenience.

## Scope

Minimal: just `git_import.py` + CLI flag + `.dvc` persistence. The global config and fsspec integration can follow.

## Implementation status

Items 1, 2, 4 done:
- `-A`/`--user-agent` flag on `dvx import-url` (passed through to `git_import_url()`)
- `user_agent` persisted in `.dvc` dep; `update_git_import()` reads it back for subsequent requests
- Default remains `dvx/0.1`

Item 3 (global config fallback) deferred — not needed for the immediate use case.
