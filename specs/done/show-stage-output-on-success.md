# Show stage stdout/stderr on success (not just failure)

## Bug

`dvx run` only shows stage subprocess output on failure. On success, stdout and stderr are silently discarded. This makes debugging impossible — when a stage "succeeds" but doesn't do what's expected, there's no output to inspect.

## Repro

```
$ dvx run --commit -v njsp/data/slack_post.dvc
  ⟳ njsp/data/slack_post: running...
  ✓ njsp/data/slack_post: side-effect completed (2.4s)
    📝 committed: Post crash updates to Slack
```

The stage completed in 2.4s and DVX says success, but the Slack script didn't actually post anything. No way to tell from DVX output — no stage logs shown.

## Expected

At minimum with `-v`/`--verbose`, DVX should show (or save) the stage's stdout+stderr:

```
$ dvx run --commit -v njsp/data/slack_post.dvc
  ⟳ njsp/data/slack_post: running...
    stdout: Posting Slack updates for refresh commit: 149bc607...
    stderr: Resolving channel: C05JZ0C5LEL
    stderr: Slack: fetched batch of 1000 messages
    ...
  ✓ njsp/data/slack_post: side-effect completed (15.3s)
```

## Options

1. **`-v` shows output inline** — stream or buffer stage output, display after completion
2. **Always save to log file** — `tmp/dvx-run-<stage>.log` on both success and failure (currently only on failure)
3. **`--show-output`** flag — explicit opt-in to see stage output
4. **Pass through by default** — let stage output flow to terminal (simplest, like Make)

Option 4 (pass through) is probably best for interactive use. Option 2 for CI. They're not mutually exclusive — stream to terminal AND save to log.

## Current behavior

- Failure: shows last 20 lines of stderr + saves to `tmp/dvx-run-<stage>.log`
- Success: discards all output, no log file saved
