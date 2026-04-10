# Stage ordering without data dependencies

## Problem

Some stages must run after others even though there's no file dependency between them. E.g. "refresh summaries" must run after "refresh data" because summaries uses the freshly-fetched XML files. But the XML files are git-tracked (not DVX-tracked), so there's no DVX dep to express this ordering.

Currently DVX puts independent stages in the same level and runs them in parallel, which causes summaries to run before (or alongside) refresh.

## Options

### 1. `after:` field in .dvc metadata

```yaml
meta:
  computation:
    cmd: njsp refresh_summaries
    after:
      - njsp/data/refresh.dvc
```

DVX places this stage after the referenced stage in the execution plan, regardless of data deps.

### 2. Side-effect dep

Allow depending on a side-effect `.dvc` file. The dep hash could be the `last_run` timestamp or a content hash of the `.dvc` file itself.

### 3. git_dep on the side-effect's outputs

If refresh produces `data/FAUQStats*.xml` (git-tracked), summaries can have a `git_dep` on those files. DVX would detect that refresh modifies them and order accordingly.

## Recommendation

Option 1 (`after:`) is the simplest and most explicit. It's a pure ordering constraint, not a data dependency.
