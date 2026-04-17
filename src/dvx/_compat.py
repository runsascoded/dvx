"""Patch DVC/dvc-data to support HTTP `Last-Modified` as `mtime` in `.dvc` files.

Upstream dvc-data's `Meta.from_info()` doesn't capture HTTP `Last-Modified`.
DVC's voluptuous schema doesn't allow `mtime` in deps/outs. And
`Meta.to_dict()` doesn't serialize `mtime` (doing so breaks local files).

This module patches all three: `from_info()` to capture `Last-Modified`,
the schema to allow `mtime`, and `to_dict()` to serialize it as ISO 8601
for HTTP sources (distinguished from local fs mtime by absence of `inode`).
Also patches `Meta.from_dict()` to parse ISO strings back to float timestamps.
"""

from datetime import datetime, timezone


def _mtime_to_iso(ts: float) -> str:
    """Convert Unix timestamp to ISO 8601 string."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _iso_to_mtime(val) -> float:
    """Parse mtime from ISO 8601 string or float."""
    if isinstance(val, str):
        return datetime.fromisoformat(val).timestamp()
    return float(val)


def _patch():
    from dvc.dependency import SCHEMA as DEP_SCHEMA
    from dvc.output import ARTIFACT_SCHEMA, META_SCHEMA
    from dvc_data.hashfile.meta import Meta

    # 1. Allow `mtime` in DVC's .dvc file schema validation
    #    Accept both float (legacy) and str (ISO 8601)
    def _mtime_validator(v):
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            return _iso_to_mtime(v)
        raise ValueError(f"mtime must be float or ISO 8601 string, got {type(v)}")

    for schema in (META_SCHEMA, ARTIFACT_SCHEMA, DEP_SCHEMA):
        schema["mtime"] = _mtime_validator

    # Allow `checksum` and `user_agent` in deps (used by dvx import-url)
    DEP_SCHEMA["checksum"] = str
    DEP_SCHEMA["user_agent"] = str

    # 2. Patch Meta.from_info() to capture HTTP Last-Modified as mtime.
    #    Upstream dvc-data doesn't do this; our fork (9b27dc6) does, but we
    #    can't ship a git+ dep to PyPI, so we monkeypatch instead.
    _orig_from_info = Meta.from_info.__func__

    @classmethod  # type: ignore[misc]
    def _from_info_with_mtime(cls, info, protocol=None):
        result = _orig_from_info(cls, info, protocol=protocol)
        if result.mtime is None and protocol and protocol.startswith("http"):
            last_modified = info.get("Last-Modified")
            if last_modified:
                from email.utils import parsedate_to_datetime

                result.mtime = parsedate_to_datetime(last_modified).timestamp()
        return result

    Meta.from_info = _from_info_with_mtime

    # 3. Extend Meta.to_dict() to serialize mtime as ISO 8601 for HTTP sources.
    #    Local filesystems always have inode set; HTTP sources never do.
    #    Use this to distinguish HTTP mtime (from Last-Modified) from
    #    local filesystem mtime (which DVC uses internally but shouldn't
    #    persist to .dvc files).
    _orig_to_dict = Meta.to_dict

    def _to_dict_with_mtime(self):
        ret = _orig_to_dict(self)
        if self.mtime is not None and self.inode is None:
            ret["mtime"] = _mtime_to_iso(self.mtime)
        return ret

    Meta.to_dict = _to_dict_with_mtime

    # 4. Extend Meta.from_dict() to parse ISO 8601 mtime strings back to float.
    _orig_from_dict = Meta.from_dict

    @classmethod  # type: ignore[misc]
    def _from_dict_with_mtime(cls, d):
        if "mtime" in d and isinstance(d["mtime"], str):
            d = {**d, "mtime": _iso_to_mtime(d["mtime"])}
        return _orig_from_dict.__func__(cls, d)

    Meta.from_dict = _from_dict_with_mtime


_patch()
