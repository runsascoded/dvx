import os

from dvx import output
from dvx.exceptions import URLMissingError
from dvx.fs import download, parse_external_url
from dvx.utils import resolve_output


def get_url(url, out=None, *, fs_config=None, jobs=None, force=False, config=None):
    out = resolve_output(url, out, force=force)
    out = os.path.abspath(out)
    (out,) = output.loads_from(None, [out], use_cache=False)

    src_fs, src_path = parse_external_url(url, fs_config, config=config)
    if not src_fs.exists(src_path):
        raise URLMissingError(url)
    download(src_fs, src_path, out.fs_path, jobs=jobs)
