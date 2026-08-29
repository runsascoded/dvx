# Generated outputs aren't reflinked to cache (only checked-out ones are)

> From the `hccs/crashes` session, chasing why a 52 G (apparent) working tree
> was ~14 G larger on disk than it needed to be. `/read crashes` for context.

## What's correct already

DVC's default `cache.type` is `["reflink", "copy"]` (reflink-first), and it
works on the *checkout* path. Proven on APFS: `njdot/data/crashes.parquet`
(292 MB, materialized by a `dvx pull`/checkout) shares extents with its cache
blob — `rm` of the worktree copy freed **0 bytes**, because the cache blob
still held the extents. Good: that file costs 1× on disk, not 2×.

## The gap

Outputs a stage **generates in place and then commits** are left as *real
copies* that duplicate their own cache blob. Same repo, the `.db` outputs of
`njdot compute db` (written directly to `www/public/njdot/`, then added):

```
rm www/public/njdot/vehicles.db   →  freed 1,120 MB   (independent copy)
```

Reflinking each `.db` from its cache blob by hand —

```
cp --reflink=auto .dvc/cache/files/md5/<ab>/<rest> www/public/njdot/<x>.db
```

— reclaimed **5.18 GB** across 6 files, and `dvx status` stayed `Fresh` for
all of them (the reflink is byte-identical to the cache blob, which is what the
`.dvc` already records). So these were pure 2× duplication: the ingest path
copied the bytes into the cache but left the worktree original in place instead
of replacing it with a link.

`dvx checkout --relink` over the whole repo did **not** fix this — it
reclaimed ~0 and left the duplicates as copies. So there is currently no
supported path (short of hand-cloning) to converge already-committed generated
outputs onto reflinks.

## Asks

1. **Ingest should link, not leave a copy.** After `dvx add` / a `dvx run`
   stage commit moves an output's bytes into the cache, re-materialize the
   worktree path from the cache per `cache.type` (reflink first), the same way
   checkout does — so a generated output costs 1× immediately, not 2× until a
   future re-checkout.
2. **`checkout --relink` should force re-linking** even when the worktree
   content already matches the cache. Its job is to fix link *type*; today it
   no-ops on content match and so can't repair an existing copy-mode tree.
3. **Report link type** somewhere cheap (`dvx status -v`, or a `dvx doctor`).
   Establishing "are my outputs reflinked or duplicated?" today requires a
   free-space probe (`rm` + `df`), because reflinks and copies are
   indistinguishable by `stat` (both are separate inodes, `nlink=1`) and `du`
   counts a clone at full apparent size. A one-line "cache links: reflink
   (N files), copy (M files)" would make the 1×/2× question observable.

## Crashes-side status

Hand-reflinked the 6 duplicated `.db` (5.2 GB reclaimed) plus removed
regenerable build cruft (`www/dist`, `.next`/`out`, a `.db.bak`) — ~14.5 GB
total off a working tree the treemap reported at 52 GB. The treemap counts
apparent size, so it double-counts every reflinked pair; physical footprint is
roughly half. Once (1) lands, generated `.db` won't re-duplicate on the next
pipeline run.

## Resolution

**Ask #1 (ingest should link, not leave a copy) — done.** The fix lives at the
one ingest chokepoint both paths share, `_cache_file` in `src/dvx/cache.py`,
so it covers `dvx run` stage commits, `dvx add`, and a directory output's
inner files alike. After the blob is cached, `_relink_worktree_from_cache`
replaces the worktree file with a copy-on-write clone of the cache blob:

- `_reflink(src, dst)` — `clonefile(2)` on macOS/APFS, the `FICLONE` ioctl on
  Linux (btrfs/XFS). Returns False (never raises) on any filesystem without
  reflink support, so ext4 / tmpfs / NFS / a cross-device dst just keep the
  plain copy. Both are reached through `ctypes` / `fcntl` — no third-party dep.
- Safe by construction: clones into a temp path in the same directory and only
  `os.replace`s it in on success, restoring the original mtime so the freshness
  mtime-cache stays valid. A byte-identical clone is guaranteed because the blob
  is content-addressed — it holds this file's exact bytes.
- Runs whether the blob was just written or already present (a fresh run writes
  an independent worktree copy either way). It only fires on an actual
  (re-)run — a skipped fresh stage never calls `cache_blob`, so there's no
  per-run churn.
- Honors an explicit `cache.type: copy` (opt-out) and a `DVX_NO_REFLINK` kill
  switch.

Tests in `tests/test_reflink.py` prove extent *sharing* on a reflink-capable FS
(via the `F_LOG2PHYS_EXT` physical-block probe — see below), and prove the safe
invariants everywhere: content/md5/mtime unchanged, an independent copy under
the fallback, opt-out honored, missing-blob no-op. Verified end-to-end on APFS
(an 8 MB output reclaimed its second copy).

**Ask #2 (`checkout --relink` should force re-linking) — deferred.** `dvx
checkout` delegates to DVC's own `repo.checkout`, so this is upstream behavior
we'd have to intercept or reimplement. And once #1 stops *creating* duplicates,
it's only a one-time repair tool for trees duplicated before this landed —
which a `dvx add`/re-run of those outputs now also fixes. Left as a follow-up.

**Ask #3 (report link type) — deferred, but more tractable than first thought.**
The spec (and my first read) called on-disk link-type detection impractical
because `stat`/`du` can't see extent sharing. That's true of those tools, but
*not* of userspace generally, and **no kext is needed**: sharing lives at the
physical-extent layer, which ordinary syscalls expose —

- macOS: `fcntl(fd, F_LOG2PHYS_EXT)` maps a file offset to its physical device
  block; two files reporting the same block for offset 0 share extents. (Used
  by the test probe here.)
- Linux: `ioctl(fd, FS_IOC_FIEMAP)` returns each file's physical extent map;
  btrfs additionally flags shared extents directly via `FIEMAP_EXTENT_SHARED`.

So a `dvx status -v` / `dvx doctor` "cache links: reflink (N) / copy (M)" line
is buildable — it's just two platform-specific code paths, not impossible. Not
built yet; the `stat`/`du` blind spot was the only real blocker and it isn't
one. Left as a follow-up now that #1 makes new outputs reflinked by default.
