# Per-stage commits via env vars + better error output

## 1. Stage output protocol (env vars)

### Concept

DVX sets environment variables before running each stage's `cmd`. The cmd can
write to files referenced by these env vars to communicate back to DVX — similar
to GHA's `$GITHUB_OUTPUT` / `$GITHUB_STEP_SUMMARY`.

### Env vars

| Var | Purpose | If present after cmd |
|-----|---------|---------------------|
| `$DVX_COMMIT_MSG_FILE` | Path to write commit message | DVX runs `git add -u && git commit -F $file` |
| `$DVX_SUMMARY_FILE` | Path to write a short summary line | DVX displays it in status output |

### Behavior

1. Before each stage, DVX creates temp files and sets the env vars
2. Cmd runs with these env vars available
3. After successful cmd:
   - If `$DVX_COMMIT_MSG_FILE` exists and is non-empty: DVX commits staged changes with that message
   - If `$DVX_SUMMARY_FILE` exists: DVX prints the summary
4. After failed cmd: no commit, show error output (see §2)
5. Temp files are cleaned up

### Example

```bash
# njsp refresh_data --s3
# ... fetches data, writes to njsp/data/FAUQStats*.xml ...

# If data changed, write a commit message:
if git diff --quiet njsp/data/; then
    echo "No new crash data"
else
    echo "Refresh NJSP data" > "$DVX_COMMIT_MSG_FILE"
    echo "Updated FAUQStats XML files" >> "$DVX_COMMIT_MSG_FILE"
fi
```

Or the `njsp` CLI could handle it internally:

```python
if os.environ.get('DVX_COMMIT_MSG_FILE') and data_changed:
    with open(os.environ['DVX_COMMIT_MSG_FILE'], 'w') as f:
        f.write(f"Refresh NJSP data\n\n{len(new_crashes)} new crashes")
```

### Fallback: `--commit` flag

For stages that don't write to `$DVX_COMMIT_MSG_FILE`, `dvx run --commit`
can auto-commit after each stage using a default message derived from the
`.dvc` filename (e.g. "Run njsp/data/refresh"). This is the simple path
for stages that don't need custom messages.

## 2. Better error output on failure

### Problem

When a stage fails, `dvx run` shows:

```
  ✗ refresh: failed
```

No stderr, no exit code, no traceback.

### Solution

Capture subprocess stdout + stderr (via `subprocess.PIPE` or tee to temp file).
On failure, print:
- Exit code
- Last N lines of stderr (default 50, configurable)
- Path to full log file

```
  ✗ refresh: failed (exit code 1)

    stderr (last 20 lines):
      Traceback (most recent call last):
        File "/usr/bin/njsp", line 10, in <module>
          sys.exit(main())
        ...
      ConnectionError: Failed to fetch https://njoag.gov/...

    Full output: /tmp/dvx-run-refresh.log
```

On success, stderr is discarded (or saved to a debug log if `--verbose`).

## 3. DVX's role vs CI's role

- **DVX handles**: running cmds, detecting failures, committing per-stage (if requested via env var or `--commit`), updating `.dvc` hashes
- **CI handles**: `git push` at the end (DVX doesn't push — that's a CI concern with auth, branch protection, etc.)
- **Cmds handle**: deciding whether to commit (by writing to `$DVX_COMMIT_MSG_FILE`) and what the message says
