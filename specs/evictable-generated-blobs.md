# Reproducible Generated Blobs

## Problem

DVX tracks both **generated** artifacts (have `meta.computation` with `cmd` + `deps`) and **input** artifacts (no computation, or foreign imports). Currently GC treats them identically — keep or evict based on git scope. But generated artifacts are fundamentally different: they can be **regenerated** from their inputs + code at any commit, making them safe to evict from cache even when referenced.

This matters for projects like jc-taxes where ~450MB of GeoJSON is regenerated from deterministic code + parquet inputs. Every regen creates 16 new cache blobs; old versions accumulate even though they're reproducible from the corresponding code.

## Concepts

### Blob taxonomy

|  | **Has computation** | **No computation** |
|--|--------------------|--------------------|
| **DVX-tracked** (`.dvc` file) | Generated: `dvx run` output | Input: `dvx add`'d raw data |
| **Foreign** (`import-url --no-download`) | n/a | External: tracked by ETag, not cached |

Generated blobs are the only ones that can be safely evicted without data loss, because they satisfy:
1. Their `.dvc` file records the exact `cmd` + `deps` + `git_deps`
2. At any git commit, the code (`git_deps`) and data inputs (`deps`) are pinned
3. Re-running the command with those inputs reproduces the output

### Reproducibility spectrum

Not all generated blobs are equally reproducible. The `reproducible` flag encodes a **confidence level about reproducibility**:

| Level | Example | Reproducible? | Notes |
|-------|---------|--------------|-------|
| **Bit-reproducible** | jc-taxes GeoJSON from deterministic Python | Yes (default) | Same inputs + code → byte-identical output |
| **Semantically reproducible** | ML inference, deterministic but float-sensitive | Mostly | May differ at last bits due to hardware/library versions |
| **Reproducible with seed** | Single-GPU training with fixed seed | Cautiously | Reproduce given same hardware + library versions |
| **Non-reproducible** | Distributed training with weight-update races | No | Even with same inputs, output differs per run |

Note: even "non-reproducible" training is becoming tractable — Google's Marin 8B (JAX + TPU) achieved bit-reproducibility for large-scale training, specifically to enable debugging loss spikes in expensive runs. But this is the exception; most distributed training has inherent nondeterminism from collective ops ordering.

**Key insight**: reproducibility is the positive default. Blobs with `computation` are assumed reproducible unless explicitly marked `meta.reproducible: false`. A `dvx run` output that took 10,000 GPU-hours to produce should be explicitly marked non-reproducible by the user.

### Reproducibility

A generated blob is **reproducible** (and thus safe to evict) when:
- It has a `computation` block in its `.dvc` file
- It is NOT marked `meta.reproducible: false` (opt-out)
- All its `deps` are available (either cached or themselves reproducible)
- All its `git_deps` are available (always true — they're in git)

An input blob is **never evictable** unless backed up to a remote (the existing `--safe` GC behavior).

## Proposed Changes

### 1. `.dvc` file: `reproducible` flag

The `meta.reproducible` field is opt-out — present only when a generated blob is NOT reproducible:

```yaml
outs:
  - md5: abc123
    size: 22851069
    path: model-checkpoint.pt

meta:
  reproducible: false
  computation:
    cmd: python train.py --steps=50000 --seed=42
    deps:
      data/training_set.parquet: def456
    git_deps:
      train.py: aabbcc
```

- Default (absent): blobs with `computation` are assumed reproducible
- `reproducible: false` — blob cannot be reliably regenerated, keep in cache
- Classification is surfaced by `dvx audit` (see blob-audit-lineage spec)

### 2. `dvx gc --evict-reproducible`

New GC mode: evict blobs that are reproducible (have `computation` and not marked `reproducible: false`), keeping only input blobs and non-reproducible outputs.

```bash
# Evict reproducible blobs not used in current workspace
dvx gc -w --evict-reproducible

# Evict reproducible blobs from all commits (keep only inputs in cache)
dvx gc -A --evict-reproducible

# Dry run
dvx gc -w --evict-reproducible --dry
```

Logic:
1. Identify all `.dvc` files in scope
2. For each, check if it has `computation` and is NOT `meta.reproducible: false`
3. If reproducible: remove from cache (local and/or remote per `--cloud`)
4. If not reproducible: keep (standard GC behavior)

### 3. Non-reproducible marking

For expensive or non-deterministic outputs, mark them explicitly:

```yaml
# In the .dvc file, set meta.reproducible: false
meta:
  reproducible: false
  computation:
    cmd: ...
```

This can be done manually in the YAML, or a future `dvx mark --not-reproducible <path>` command.

### 4. `dvx regen` (optional, future)

Regenerate evicted blobs on demand:

```bash
# Regenerate a specific evicted blob
dvx regen www/public/taxes-2025-lots.geojson

# Regenerate all evicted blobs needed for current workspace
dvx regen --workspace
```

Uses `meta.computation.cmd` with the pinned deps. This is essentially `dvx run` but specifically targeting blobs that were evicted.

## Interaction with `dvx push`

When pushing, reproducible blobs could optionally be skipped:

```bash
# Push only non-reproducible (input) blobs
dvx push --skip-reproducible

# Push everything (default, current behavior)
dvx push
```

This saves remote storage for blobs that are reproducible. The tradeoff: re-generating is slower than pulling from cache, but for deterministic pipelines the remote copy is pure redundancy.

## Open Questions

- Should there be a project-level default in `.dvc/config`? E.g., `core.assume_reproducible = true` (already the default behavior) or `false` to require explicit opt-in.
- Should there be a "confidence" level? E.g., `reproducible: true` (fully reproducible) vs `reproducible: expensive` (reproducible but costly)?
- How does this interact with remote storage billing? If reproducible blobs are skipped on push, the remote is smaller but regen requires local compute.
