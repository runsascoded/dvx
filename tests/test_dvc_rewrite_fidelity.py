"""Tests for `.dvc` rewrite fidelity: comments, key order, and path
spelling must survive a value-only rewrite (md5/size bump).

Regression the tests guard against: an existing ``.dvc`` file has a YAML
comment block explaining *why* the deps are shaped a certain way; a
subsequent ``write_dvc_file`` call that only needs to bump one dep's md5
used to round-trip through ``yaml.dump`` and lose everything the data
model doesn't carry — comments first, path-spelling nuance second.

See ``specs/dvc-rewrite-fidelity.md`` for the observed nj-crashes case.
"""

from pathlib import Path

from dvx.run.dvc_files import write_dvc_file


def test_rewrite_preserves_comment_block_above_deps(tmp_path):
    """A comment block above ``deps:`` survives a dep-md5 bump.

    Concrete shape from the nj-crashes repro: 3 dep entries, an inline
    comment block above them explaining a non-obvious modeling choice.
    After bumping one dep's md5, the comment block must still be there
    in its original position, and the two untouched dep md5s must be
    unchanged.
    """
    dvc_path = tmp_path / "harmonize.dvc"
    dvc_path.write_text(
        "outs:\n"
        "- md5: abc123def456abc123def456abc123de\n"
        "  size: 100\n"
        "  hash: md5\n"
        "  path: harmonize\n"
        "meta:\n"
        "  computation:\n"
        "    cmd: njsp harmonize_muni_codes\n"
        "    deps:\n"
        "      # The NJSP side reads the raw XMLs (`load_sp_data` -> `get_crashes_df`),\n"
        "      # NOT `crashes.parquet`. This used to dep on `crashes.parquet`, which\n"
        "      # invented a `update_pqts` -> `harmonize` edge that doesn't exist.\n"
        "      /data/FAUQStats2008.xml: 017b63c673518b66eff6dcabf5af4778\n"
        "      /data/FAUQStats2024.xml: 1234567890abcdef1234567890abcdef\n"
        "      /data/FAUQStats2025.xml: 2776168cd1b100f7041dd14dcdc19701\n"
    )

    # Bump just the 2025 dep md5 (the daily update case).
    write_dvc_file(
        tmp_path / "harmonize",
        md5="abc123def456abc123def456abc123de",
        size=100,
        cmd="njsp harmonize_muni_codes",
        deps={
            "/data/FAUQStats2008.xml": "017b63c673518b66eff6dcabf5af4778",
            "/data/FAUQStats2024.xml": "1234567890abcdef1234567890abcdef",
            "/data/FAUQStats2025.xml": "6e96ac604e24fd6310deb4ce26befc21",
        },
    )

    rewritten = dvc_path.read_text().split("\n")

    # Comment block still present at its original position (above the
    # dep entries, under the ``deps:`` key). Assert the exact lines and
    # the surviving unchanged dep md5s.
    assert rewritten == [
        "outs:",
        "- md5: abc123def456abc123def456abc123de",
        "  size: 100",
        "  hash: md5",
        "  path: harmonize",
        "meta:",
        "  computation:",
        "    cmd: njsp harmonize_muni_codes",
        "    deps:",
        "      # The NJSP side reads the raw XMLs (`load_sp_data` -> `get_crashes_df`),",
        "      # NOT `crashes.parquet`. This used to dep on `crashes.parquet`, which",
        "      # invented a `update_pqts` -> `harmonize` edge that doesn't exist.",
        "      /data/FAUQStats2008.xml: 017b63c673518b66eff6dcabf5af4778",
        "      /data/FAUQStats2024.xml: 1234567890abcdef1234567890abcdef",
        "      /data/FAUQStats2025.xml: 6e96ac604e24fd6310deb4ce26befc21",
        "",
    ]


def test_rewrite_preserves_slash_prefixed_dep_paths(tmp_path):
    """``/repo-root``-style dep paths survive an md5 rewrite verbatim.

    Under a deeply-nested ``.dvc`` file, DVX already writes deps outside
    ``dvc_dir`` in ``/repo-root``-prefixed shorthand (avoids ``../../../``
    chains). This test guards against that spelling being lost when a
    prior file recorded the same spelling.
    """
    subdir = tmp_path / "njsp"
    subdir.mkdir()
    dvc_path = subdir / "harmonize.dvc"
    dvc_path.write_text(
        "outs:\n"
        "- md5: abc123def456abc123def456abc123de\n"
        "  size: 50\n"
        "  hash: md5\n"
        "  path: harmonize\n"
        "meta:\n"
        "  computation:\n"
        "    cmd: harmonize\n"
        "    deps:\n"
        "      /data/FAUQStats2025.xml: 2776168cd1b100f7041dd14dcdc19701\n"
    )

    # Same `/data/...` spelling in the new call — should survive.
    write_dvc_file(
        subdir / "harmonize",
        md5="abc123def456abc123def456abc123de",
        size=50,
        cmd="harmonize",
        deps={"data/FAUQStats2025.xml": "6e96ac604e24fd6310deb4ce26befc21"},
    )

    rewritten = dvc_path.read_text().split("\n")

    assert rewritten == [
        "outs:",
        "- md5: abc123def456abc123def456abc123de",
        "  size: 50",
        "  hash: md5",
        "  path: harmonize",
        "meta:",
        "  computation:",
        "    cmd: harmonize",
        "    deps:",
        "      /data/FAUQStats2025.xml: 6e96ac604e24fd6310deb4ce26befc21",
        "",
    ]


def test_rewrite_preserves_key_order_in_outs_entry(tmp_path):
    """Order of fields inside an ``outs`` entry is preserved on rewrite."""
    dvc_path = tmp_path / "out.dvc"
    # Original file uses non-default key order: path first, then hash, md5, size.
    dvc_path.write_text(
        "outs:\n"
        "- path: out\n"
        "  hash: md5\n"
        "  md5: abc123def456abc123def456abc123de\n"
        "  size: 10\n"
    )

    write_dvc_file(
        tmp_path / "out",
        md5="deadbeefdeadbeefdeadbeefdeadbeef",
        size=20,
    )

    rewritten = dvc_path.read_text().split("\n")

    # Only md5 + size updated; ordering intact.
    assert rewritten == [
        "outs:",
        "- path: out",
        "  hash: md5",
        "  md5: deadbeefdeadbeefdeadbeefdeadbeef",
        "  size: 20",
        "",
    ]


def test_rewrite_removes_obsolete_dep(tmp_path):
    """A dep dropped from the new call is removed from the rewritten file.

    In-place merge must not perpetuate stale keys. This is the
    negative-side counterpart to comment/order preservation — the
    canonical field set still wins.
    """
    dvc_path = tmp_path / "harmonize.dvc"
    dvc_path.write_text(
        "outs:\n"
        "- md5: abc123def456abc123def456abc123de\n"
        "  size: 10\n"
        "  hash: md5\n"
        "  path: harmonize\n"
        "meta:\n"
        "  computation:\n"
        "    cmd: cmd\n"
        "    deps:\n"
        "      /data/a: 1111aaaa1111aaaa1111aaaa1111aaaa\n"
        "      /data/b: 2222bbbb2222bbbb2222bbbb2222bbbb\n"
    )

    # New call drops `/data/b`.
    write_dvc_file(
        tmp_path / "harmonize",
        md5="abc123def456abc123def456abc123de",
        size=10,
        cmd="cmd",
        deps={"data/a": "1111aaaa1111aaaa1111aaaa1111aaaa"},
    )

    rewritten = dvc_path.read_text().split("\n")

    assert rewritten == [
        "outs:",
        "- md5: abc123def456abc123def456abc123de",
        "  size: 10",
        "  hash: md5",
        "  path: harmonize",
        "meta:",
        "  computation:",
        "    cmd: cmd",
        "    deps:",
        "      /data/a: 1111aaaa1111aaaa1111aaaa1111aaaa",
        "",
    ]


def test_rewrite_adds_new_dep_at_end(tmp_path):
    """A dep newly added by the call appears at the end (order-stable).

    Note: existing deps keep their original spelling (``/data/a`` here);
    genuinely new deps take the *caller's* spelling. We don't infer a
    "site style" from siblings — the caller decides what the canonical
    spelling of a fresh dep should be, and ``_relativize_dep_paths``
    normally produces the same form the existing sibling uses anyway.
    """
    dvc_path = tmp_path / "harmonize.dvc"
    dvc_path.write_text(
        "outs:\n"
        "- md5: abc123def456abc123def456abc123de\n"
        "  size: 10\n"
        "  hash: md5\n"
        "  path: harmonize\n"
        "meta:\n"
        "  computation:\n"
        "    cmd: cmd\n"
        "    deps:\n"
        "      /data/a: 1111aaaa1111aaaa1111aaaa1111aaaa\n"
    )

    # Caller passes both deps with `/`-prefixed spelling for the new one
    # (mirroring what `_relativize_dep_paths` produces when the .dvc is
    # deeper than repo root — i.e. the realistic add-a-new-dep path).
    write_dvc_file(
        tmp_path / "harmonize",
        md5="abc123def456abc123def456abc123de",
        size=10,
        cmd="cmd",
        deps={
            "data/a": "1111aaaa1111aaaa1111aaaa1111aaaa",
            "/data/b": "2222bbbb2222bbbb2222bbbb2222bbbb",
        },
    )

    rewritten = dvc_path.read_text().split("\n")

    assert rewritten == [
        "outs:",
        "- md5: abc123def456abc123def456abc123de",
        "  size: 10",
        "  hash: md5",
        "  path: harmonize",
        "meta:",
        "  computation:",
        "    cmd: cmd",
        "    deps:",
        "      /data/a: 1111aaaa1111aaaa1111aaaa1111aaaa",
        "      /data/b: 2222bbbb2222bbbb2222bbbb2222bbbb",
        "",
    ]


def test_rewrite_new_file_no_existing(tmp_path):
    """When the file doesn't exist, the writer emits a fresh minimal form."""
    output_path = tmp_path / "new_output"

    dvc_path = write_dvc_file(
        output_path,
        md5="abc123def456abc123def456abc123de",
        size=5,
        cmd="echo",
    )

    # No prior file → freshly emitted content, no comments to preserve.
    assert dvc_path.read_text().split("\n") == [
        "outs:",
        "- md5: abc123def456abc123def456abc123de",
        "  size: 5",
        "  hash: md5",
        "  path: new_output",
        "meta:",
        "  computation:",
        "    cmd: echo",
        "",
    ]


def test_rewrite_preserves_multiline_comment_in_outs(tmp_path):
    """A comment block above the ``outs`` list survives an outs rewrite."""
    dvc_path = tmp_path / "out.dvc"
    dvc_path.write_text(
        "# Top-level comment: this artifact is public-facing.\n"
        "outs:\n"
        "- md5: abc123def456abc123def456abc123de\n"
        "  size: 10\n"
        "  hash: md5\n"
        "  path: out\n"
    )

    write_dvc_file(
        tmp_path / "out",
        md5="deadbeefdeadbeefdeadbeefdeadbeef",
        size=20,
    )

    rewritten = dvc_path.read_text().split("\n")

    assert rewritten == [
        "# Top-level comment: this artifact is public-facing.",
        "outs:",
        "- md5: deadbeefdeadbeefdeadbeefdeadbeef",
        "  size: 20",
        "  hash: md5",
        "  path: out",
        "",
    ]


def test_rewrite_preserves_extra_meta_fields(tmp_path):
    """Custom fields under ``meta:`` (not managed by DVX) survive rewrites.

    A common ruamel idiom: teams stash their own metadata under ``meta:``.
    A DVX rewrite that touches ``meta.computation.deps`` must not delete
    those sibling fields.
    """
    dvc_path = tmp_path / "out.dvc"
    dvc_path.write_text(
        "outs:\n"
        "- md5: abc123def456abc123def456abc123de\n"
        "  size: 10\n"
        "  hash: md5\n"
        "  path: out\n"
        "meta:\n"
        "  owner: data-team\n"
        "  tags:\n"
        "  - public\n"
        "  - daily\n"
        "  computation:\n"
        "    cmd: cmd\n"
        "    deps:\n"
        "      /data/a: 1111aaaa1111aaaa1111aaaa1111aaaa\n"
    )

    write_dvc_file(
        tmp_path / "out",
        md5="abc123def456abc123def456abc123de",
        size=10,
        cmd="cmd",
        deps={"data/a": "2222bbbb2222bbbb2222bbbb2222bbbb"},
    )

    rewritten = dvc_path.read_text().split("\n")

    assert rewritten == [
        "outs:",
        "- md5: abc123def456abc123def456abc123de",
        "  size: 10",
        "  hash: md5",
        "  path: out",
        "meta:",
        "  owner: data-team",
        "  tags:",
        "  - public",
        "  - daily",
        "  computation:",
        "    cmd: cmd",
        "    deps:",
        "      /data/a: 2222bbbb2222bbbb2222bbbb2222bbbb",
        "",
    ]


def test_rewrite_preserves_side_effect_flag(tmp_path):
    """A hand-authored ``side_effect: true`` survives the stage's own rerun.

    ``side_effect`` is author intent, not a value DVX recomputes. The
    executor rewrites a co-output / driver ``.dvc`` after the cmd runs to
    refresh dep hashes, and does so without passing the flag — so a merge
    that stripped it (because ``new_comp`` omits it) silently disarmed the
    co-output wait. nj-crashes hit exactly this: a daily-cron
    ``harmonize_muni_codes`` run dropped ``side_effect: true`` from
    ``njsp/data/harmonize.dvc``, and the next reproc failed
    ``✗ harmonize: co-output not produced``.

    Regression of ``specs/co-output-side-effect-flag-durability.md``.
    """
    dvc_path = tmp_path / "harmonize.dvc"
    dvc_path.write_text(
        "meta:\n"
        "  computation:\n"
        "    cmd: njsp harmonize_muni_codes\n"
        "    # Driver stage: no outs of its own; the four real co-outputs\n"
        "    # carry the data. side_effect keeps the co-output wait from\n"
        "    # expecting an artifact here.\n"
        "    side_effect: true\n"
        "    deps:\n"
        "      /data/FAUQStats2023.xml: 1111aaaa1111aaaa1111aaaa1111aaaa\n"
    )

    # The rewrite the executor's side-effect branch performs: refresh dep
    # hashes, cmd re-supplied, no side_effect passed.
    write_dvc_file(
        tmp_path / "harmonize",
        cmd="njsp harmonize_muni_codes",
        deps={"data/FAUQStats2023.xml": "2222bbbb2222bbbb2222bbbb2222bbbb"},
    )

    rewritten = dvc_path.read_text().split("\n")

    assert rewritten == [
        "meta:",
        "  computation:",
        "    cmd: njsp harmonize_muni_codes",
        "    # Driver stage: no outs of its own; the four real co-outputs",
        "    # carry the data. side_effect keeps the co-output wait from",
        "    # expecting an artifact here.",
        "    side_effect: true",
        "    deps:",
        "      /data/FAUQStats2023.xml: 2222bbbb2222bbbb2222bbbb2222bbbb",
        "",
    ]
