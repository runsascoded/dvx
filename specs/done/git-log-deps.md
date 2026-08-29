# `git_log_deps`: depending on a path's Git history

## Problem

DVX deps name **files in the working tree**. Some stages consume a path's
**history** — every past version, not the one at HEAD. nj-crashes'
`crash_log` walks the git history of `data/FAUQStats*.xml` (1,348 commits
since 2022-05-25) to build a log of what NJSP had published on each day.

No dep declaration can express that, so the stage did the only thing it
could: it read **its own output** to find the last SHA it had processed, then
walked forward from there. That makes it unreproducible by construction — on
a clean machine there's no prior output, so it crashes. It also conflates two
different jobs into one value:

- **freshness** — should this stage run at all?
- **resume cursor** — if it runs, where does it pick up?

## Model

A new `meta.computation.git_log_deps` mapping, symmetric with `git_deps`:

```yaml
meta:
  computation:
    cmd: njsp crash_log compute -i -v
    git_log_deps:
      /data/FAUQStats*.xml: 1a8824595a6c...   # tip commit touching the pathspec
```

- **Key** is a git *pathspec*, so globs work. Resolved relative to the
  `.dvc`'s directory and written back in the `/`-rooted form, exactly like
  `deps` / `git_deps`.
- **Value** is `git rev-list -1 HEAD -- <pathspec>` — the most recent commit
  touching it — recorded after a successful run.
- **Freshness**: stale iff the current tip differs from the recorded one.
  Recording the *tip* rather than a `since..HEAD` range keeps the check to a
  single comparison and is immune to the ancestry rewrites (rebase, amend)
  that make range arithmetic fragile.
- **Not an ordering edge.** Nothing in the plan produces a commit, so
  `git_log_deps` never adds a node or a level — it's a freshness input only.

### Resume cursor

Before the cmd runs, dvx exports the **recorded** shas (where the last
successful run left off — not the current tips):

- `DVX_GIT_LOG_SINCE` — the sha, when exactly one pathspec is declared.
- `DVX_GIT_LOG_DEPS` — `{pathspec: sha}` as JSON, always.

So the stage walks `$DVX_GIT_LOG_SINCE..HEAD` and processes only what's new,
and stops reading its own output entirely. dvx owns freshness; the stage owns
the resume point. On a from-scratch run there's no recorded sha, the env vars
are absent, and the stage builds from its own floor — which is exactly the
"if something's there use it, else rebuild" behavior that already works.

### Shallow clones

`git rev-list` **truncates silently** at a shallow boundary: in a `--depth 1`
clone it answers "HEAD touched everything", which would make a stale stage
look fresh. `is_shallow_repo()` gates the check, and a shallow repo reports
`git history dep unverifiable (shallow clone): <pathspec>` — it reruns rather
than claiming a freshness it hasn't established.

A `--filter=blob:none` **partial** clone is *not* shallow: full commit graph,
lazy blob fetch over the normal pack protocol. For nj-crashes that's 60 MB /
3.2 s versus 3.71 GiB for a full clone, and it's the right shape for a
container that needs history. Partial-clone is the answer to "how does the
Batch image get history cheaply" — no per-blob GitHub API fallback needed.

## Surface

| | |
|---|---|
| `dvc_files.get_git_log_dep_sha(pathspec)` | tip commit, or None |
| `dvc_files.is_shallow_repo()` | gate for the above |
| `DVCFileInfo.git_log_deps` | `{pathspec: sha}` |
| `Computation.git_log_deps` | `list[Artifact \| str \| Path]` |
| `Computation.get_git_log_dep_shas(recompute=)` | current or recorded shas |
| `write_dvc_file(git_log_deps=)` | emits + merges the block |
| `$DVX_GIT_LOG_SINCE` / `$DVX_GIT_LOG_DEPS` | resume cursor for the cmd |

## Resolution

Implemented as described. Tests in `tests/test_git_log_deps.py` cover the
primitive (tip resolution through a glob, unrelated commits, no match), the
freshness transitions (fresh → new matching commit → stale; a *new* matching
file counts; unmatched pathspec reported missing), the shallow-clone guard,
`.dvc` round-trip including the `/`-rooted spelling, the resume-cursor env
vars (recorded sha before the run, advanced tip after), and that a history
dep adds no level to the plan.

Not done, deliberately: no `$refspec` *range* syntax (`a..b`) in the key. The
tip-commit form answers freshness completely, and a range would need its own
staleness semantics without a demonstrated use case. `dvx status`'s
stale-descendant propagation also ignores `git_log_deps` — a pathspec is
never a stage output, and a glob could not match one.
