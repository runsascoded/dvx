# Recursive .dvc file discovery for `dvx run`

## Problem

`dvx run` with no targets only discovers `.dvc` files in the current working directory. In projects where `.dvc` files are scattered across subdirectories (e.g. `njsp/data/refresh.dvc`, `www/deploy.dvc`, `api/d1-import.dvc`), `dvx run` finds nothing and reports "No computations to execute."

`dvx status` correctly finds all `.dvc` files recursively, but `dvx run` doesn't.

## Proposed fix

When no targets are specified, `dvx run` should recursively discover all `.dvc` files in the repo (same as `dvx status`), filter to those with `computation.cmd`, and run stale ones.

This makes the CI workflow simply `dvx run` instead of requiring explicit target lists.
