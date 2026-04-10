# Per-stage push should be gated on global push being enabled

## Bug

Config has `push: never` globally but `stages: { foo.dvc: { push: each } }`.
Running `dvx run --commit` (no `--push`) still pushes after `foo.dvc` because
the per-stage override takes effect regardless of the global setting.

## Expected

Per-stage `push: each` should only apply when push is enabled (via `--push`,
`DVX_PUSH`, or global `push: each|end`). When global push is `never` (or
not passed on CLI), no stage should push regardless of per-stage config.

The per-stage config is about *strategy* (when to push within a run that
pushes), not *enablement* (whether to push at all).
