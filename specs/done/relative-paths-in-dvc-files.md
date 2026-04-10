# Relative paths in .dvc files

## Problem

Currently all paths in `.dvc` files (deps, git_deps, cmd) are relative to the repo root, regardless of where the `.dvc` file lives:

```yaml
# www/deploy.dvc — paths are repo-root-relative
meta:
  computation:
    cmd: ./www/deploy.sh
    git_deps:
      www/deploy.sh: abc123...
      www/src: def456...
      www/public: 789abc...
```

This is redundant — `www/deploy.dvc` already implies a `www/` context. Files in subdirectories shouldn't need to repeat the prefix.

## Proposed behavior

Paths in `.dvc` files are relative to the `.dvc` file's directory:

```yaml
# www/deploy.dvc — paths relative to www/
meta:
  computation:
    cmd: ./deploy.sh
    git_deps:
      deploy.sh: abc123...
      src: def456...
      public: 789abc...
```

For deps that reference files outside the `.dvc` file's directory, use `../`:

```yaml
# www/public/njsp/csvs.dvc
meta:
  computation:
    cmd: njsp update_www_data -f
    deps:
      ../../../njsp/data/crashes.parquet: cfb770bf...
```

Or allow absolute-from-root paths with a leading `/`:

```yaml
    deps:
      /njsp/data/crashes.parquet: cfb770bf...
```

## DVX resolution

When DVX reads a `.dvc` file, it resolves all paths relative to the `.dvc` file's parent directory. Internally it converts to repo-root-relative for git operations and hash lookups.

## `cmd` execution

The `cmd` should run with CWD set to the `.dvc` file's directory (not repo root). This is more natural — `www/deploy.dvc` with `cmd: ./deploy.sh` runs `deploy.sh` from `www/`.

## Migration

Existing `.dvc` files with repo-root-relative paths continue to work — DVX can detect whether a path starts with the `.dvc` file's directory prefix and handle both conventions. Or provide a `dvx migrate-paths` command to convert.

## Benefits

- Less path repetition in `.dvc` files
- More portable — moving a `.dvc` file and its outputs to a subdirectory doesn't require rewriting all paths
- `cmd` CWD matches expectations (script in same dir as `.dvc` just works)
- Consistent with how DVC originally handled paths
