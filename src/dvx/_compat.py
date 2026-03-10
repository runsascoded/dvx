"""Patch DVC to support HTTP `Last-Modified` as `mtime` in `.dvc` files.

dvc-data's `Meta.from_info()` captures HTTP `Last-Modified` into `mtime`, but:
1. DVC's voluptuous schema doesn't allow `mtime` in deps/outs
2. `Meta.to_dict()` doesn't serialize `mtime` (doing so breaks local files)

This module patches both: adds `mtime` to the schema, and extends `to_dict()`
to include `mtime` as an ISO 8601 string when the source is HTTP (distinguished
from local fs mtime by absence of `inode`). Also patches `Meta.from_dict()` to
parse ISO strings back to float timestamps.
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

    # 2. Extend Meta.to_dict() to serialize mtime as ISO 8601 for HTTP sources.
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

    # 3. Extend Meta.from_dict() to parse ISO 8601 mtime strings back to float.
    _orig_from_dict = Meta.from_dict

    @classmethod  # type: ignore[misc]
    def _from_dict_with_mtime(cls, d):
        if "mtime" in d and isinstance(d["mtime"], str):
            d = {**d, "mtime": _iso_to_mtime(d["mtime"])}
        return _orig_from_dict.__func__(cls, d)

    Meta.from_dict = _from_dict_with_mtime


_patch()
