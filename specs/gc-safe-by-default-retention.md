# `dvx gc`: `--safe` by default on the retention paths

Origin: surfaced by the nj-crashes (`~/c/hccs/crashes`) session's cross-project disk-reclaim pass. User asked, of `dvx gc`: "if non-`-s` is so dangerous, should dvx change the default?" Greenlit in the dvx session.

## Problem

`dvx gc`'s destructive, dvx-specific paths delete local cache blobs regardless of whether a remote can restore them. The retention paths (`--keep N` / `--older-than D`) delete *superseded* artifact versions — and a superseded version whose blobs were **never pushed to any remote** is then gone permanently. That's the "committed-but-unpushed leaf blobs" footgun the reproc work already hit. `-s/--safe` (delete only blobs confirmed present in a remote; report + skip the rest) exists precisely to prevent this, but it's opt-in — so the safe behavior is the one you have to remember, and the data-destroying behavior is the default (guarded only by the generic `click.confirm`).

## Change

Make safe the **default for the dvx-native retention paths** (`--keep` / `--older-than`), with an explicit `-U/--unsafe` opt-out:

- `-s/--safe` stays (now redundant-but-allowed on retention; still the explicit opt-in on the DVC-scope paths).
- `-U/--unsafe` restores the delete-local-only-blobs behavior. `--safe` and `--unsafe` together is an error.
- `use_safe = safe or (retention and not unsafe)` — retention defaults safe; the DVC-scope paths (`-w`/`-a`/`-A`/`-T` without a retention policy) are **unchanged** (still explicit-`--safe`-only, mirroring DVC's own unsafe default; flipping those is a separate DVC-parity call).
- **Fail loud, not destructive**, when safe-by-default can't verify a remote (none configured, or unreachable — e.g. an expired SSO token): error with "push first, or pass `--unsafe`" instead of deleting. An *explicit* `--safe` keeps its existing "remote check failed" message.
- The skip report ("N blob(s) not present in any remote") now points at `--unsafe` rather than "gc without --safe".

Net: the only-copy-is-local footgun becomes opt-in; offline/no-remote degrades to a clear error rather than silent data loss.

## TFFP

`tests/` — a retention gc where one superseded version's blob is present in the remote and another's is not:
- Default (no flag): the remote-backed blob is deleted, the local-only blob is retained + reported. (Pre-change: both deleted.)
- `--unsafe`: both deleted.
- Safe-default with no remote resolvable: raises the actionable error, deletes nothing.
