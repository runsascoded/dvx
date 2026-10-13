# Cross-remote push exclusion and pull fallback

Requested by jc-taxes, 2026-10-13. Implemented 2026-10-13; this completed spec accompanies the cross-remote transfer implementation commit.

## Motivation

jc-taxes has a public canonical R2 content-addressed cache and a private contributor staging bucket. Contributors can read canonical blobs anonymously over HTTPS, but can write/delete only in their own staging bucket. Staging should contain only new blobs, not copies of the canonical dataset. A tracked target can consequently require blobs from both remotes, especially a directory manifest and its children.

Direct pull from staging remains sufficient for a standalone file whose blob is staged. Pulling a directory or several targets from staging alone is not guaranteed to work when shared blobs were deliberately excluded from upload.

## Implemented CLI

```bash
dvx push -r staging --exclude-remote public <targets>
dvx push -n -r staging --exclude-remote public <targets>
dvx pull -r staging --fallback-remote public <targets>
dvx fetch -r staging --fallback-remote public <targets>
```

Use repeatable long options if it fits the existing transfer API: exclusions are a union; fallbacks are ordered. No implicit cross-remote lookup or change to commands without these flags. Resolve named remotes through existing configuration; the public remote is an HTTP cache root, not an authenticated S3 endpoint. Confirm existing HTTP-remote support before implementation rather than embedding R2-specific URL rules.

## Push Semantics

- Resolve the same tracked target closure as normal push, including directory manifests and all referenced children. Do not upload unrelated local cache objects.
- Upload a referenced blob only when absent from destination and every exclusion remote. Probe candidate hashes, not entire remote listings: public HTTP cache endpoints support object reads but not anonymous listing.
- Cover both normal DVC push and DVX's directory-inner-blob backfill. An exclusion must not be undone by the backfill.
- A missing local blob is acceptable if it exists in destination or an exclusion remote. Missing required local blobs are errors. Manifest expansion must use available local/remote manifests; do not silently omit unknown children.
- Only definite absence permits upload. Authentication, permission, timeout, and server errors must fail, not masquerade as absence. Bound concurrency and reuse existing retry/remote abstractions.
- Dry-run uses the same plan as real push. Report upload count/bytes, already-at-destination count, excluded count (with source remote), and errors. No remote writes in dry-run.
- Verification with exclusions validates availability across destination plus exclusions, not destination completeness; explicitly label this union. Preserve destination-only verification without exclusions.

## Pull / Fetch Semantics

- Use a valid local cached blob first; otherwise try primary remote followed by fallbacks in supplied order, independently for every hash.
- Fall back only on definite not-found. Fail on permission/network/server errors instead of hiding a broken configuration. Invalid downloaded content must fail integrity verification, not silently switch sources.
- Fetch a directory manifest from whichever remote has it, then resolve each child with the same lookup order. Cache downloads locally normally; pull performs existing checkout semantics, fetch does not checkout.
- If no remote has a required blob, fail with its hash and attempted remote names. Keep existing partial-transfer behavior explicit; do not report the target complete.
- Never copy blobs between remote buckets, never write to a fallback/exclusion remote, and never modify sidecars or remote configuration as a transfer side effect.
- Deduplicate candidate hashes and cache existence results within the invocation. Avoid stale persistent listing results deciding absence.

## Implementation Pointers

Start in src/dvx/cli/transfer.py. Current push delegates to Repo.push and separately repairs directory-inner blobs; filtering only the latter is insufficient. src/dvx/cli/cache/comm.py already exposes local/remote/reference set comparisons, but its remote-listing approach is not appropriate for anonymous HTTP candidate checks. Prefer one shared transfer plan and existing object-database operations over shelling out to cache comm or building a second cache implementation.

## Tests / Acceptance

Use tiny local fake remotes and an HTTP fixture with object GET/HEAD but no LIST; no cloud credentials needed. Assertions should compare exact plans, hash sets, requests, and destination contents rather than output substrings.

1. Push mixed targets: one local-only blob uploads, one canonical-only blob is excluded, one destination blob is skipped; no unrelated blob uploads.
2. Missing local canonical-only blob succeeds; missing local blob absent everywhere fails.
3. Directory manifest and children split across remotes transfer correctly, including nested paths and the inner-blob backfill. Unknown/unavailable manifests fail clearly.
4. Dry-run and actual push produce identical classifications; dry-run leaves destination unchanged. Union verification succeeds only when the complete closure is available.
5. Pull/fetch resolve primary-only, fallback-only, and locally cached hashes; ordered fallbacks stop at the first valid match. Pull checks out exact content; fetch leaves workspace unchanged.
6. Definite 404 permits fallback/exclusion absence; 403, timeout, 5xx, and corrupted content fail. No fallback silently masks those errors.
7. Duplicate references transfer once; canonical HTTP fixture receives no listing requests and no writes.
8. Existing no-flag push/pull/fetch behavior and supported target-selection flags retain coverage. Document unsupported combinations explicitly rather than ignoring flags.

## Scope Boundary

This is transfer composition, not automatic promotion or provenance trust. Staging is intentionally incomplete alone; document the need for canonical plus staging when consuming it. Canonical publication still requires maintainer verification. No contributor credentials or account identifiers belong in tests or examples.

## Implementation report

- `src/dvx/transfer.py` owns a shared `TransferPlan` for explicit cross-remote operations, using DVC configuration, object databases, target selection, and checkout. The new push path covers manifests and children together, bypassing both legacy push and its separate backfill so exclusions cannot be undone. Objects are deduplicated, existence results are invocation-local, and probes/transfers use a bounded thread pool.
- HTTP uses the existing DVC HTTP backend's strict candidate existence checks, preserving configured authentication/retries and raising on non-404 failures. S3 uses HEAD for the individual object rather than prefix listings. Local remotes use fresh object metadata. No persistent remote index decides absence.
- Directory manifests can be read from local cache, primary, or additional remotes. Planning downloads them into temporary storage under `.dvc/tmp`, validates their MD5 and structure, and expands their children without populating permanent cache or touching workspace data. Unavailable or invalid manifests fail the plan.
- Push plans classify upload/destination/excluded hashes with source names and upload byte counts. Plan errors prevent all uploads; runtime failures report partial completion. `--verify` checks the full destination/exclusion union with fresh probes.
- Pull/fetch prefer valid local content, then primary and ordered fallbacks. Downloads use unique temporary siblings, MD5 validation, and atomic replacement. Corruption and non-not-found errors stop lookup. Pull checks out through DVC only after all required transfers succeed; fetch and `pull --meta-only` do not check out. Sidecars and remote configuration are not rewritten.
- Documented limits: cross-remote flags currently reject history-wide selectors (`-a/-A/-T`, pull `-R`), worktree/versioned remotes, non-MD5 outputs, external outputs, and nested `.dir` references inside manifests. Nested file paths in ordinary DVC manifests are supported. Existing no-flag transfers remain on their original code paths.
- Validation: the isolated staged snapshot passed 440 tests, with 4 opt-in S3 tests skipped, including all 49 cross-remote cases. The index export initially omitted the generated `_version.py` build artifact; the version test passed after supplying it. Ruff and `git diff --check` pass. Tests use local remotes, an object-only loopback HTTP server, and fake S3 HEAD calls; no cloud credentials or production data are used.
