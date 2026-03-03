"""Patch DVC to support HTTP `Last-Modified` as `mtime` in `.dvc` files.

dvc-data's `Meta.from_info()` captures HTTP `Last-Modified` into `mtime`, but:
1. DVC's voluptuous schema doesn't allow `mtime` in deps/outs
2. `Meta.to_dict()` doesn't serialize `mtime` (doing so breaks local files)

This module patches both: adds `mtime` to the schema, and extends `to_dict()`
to include `mtime` when `checksum` is set (HTTP sources use `checksum` for
ETag/Content-MD5, while local/S3/GCS use `etag`/`md5`).
"""


def _patch():
    from dvc.dependency import SCHEMA as DEP_SCHEMA
    from dvc.output import ARTIFACT_SCHEMA, META_SCHEMA
    from dvc_data.hashfile.meta import Meta

    # 1. Allow `mtime` in DVC's .dvc file schema validation
    for schema in (META_SCHEMA, ARTIFACT_SCHEMA, DEP_SCHEMA):
        if "mtime" not in schema:
            schema["mtime"] = float

    # 2. Extend Meta.to_dict() to serialize mtime for HTTP sources.
    #    Local filesystems always have inode set; HTTP sources never do.
    #    Use this to distinguish HTTP mtime (from Last-Modified) from
    #    local filesystem mtime (which DVC uses internally but shouldn't
    #    persist to .dvc files).
    _orig_to_dict = Meta.to_dict

    def _to_dict_with_mtime(self):
        ret = _orig_to_dict(self)
        if self.mtime is not None and self.inode is None:
            ret["mtime"] = self.mtime
        return ret

    Meta.to_dict = _to_dict_with_mtime


_patch()
