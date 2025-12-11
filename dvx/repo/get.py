import os
from typing import TYPE_CHECKING, Union

from dvx.exceptions import DvcException
from dvx.log import logger
from dvx.utils import resolve_output

if TYPE_CHECKING:
    from dvx.fs.dvc import DVCFileSystem


logger = logger.getChild(__name__)


class GetDVCFileError(DvcException):
    def __init__(self):
        super().__init__(
            "the given path is a DVC file, you must specify a data file or a directory"
        )


def get(
    url,
    path,
    out=None,
    rev=None,
    jobs=None,
    force=False,
    config=None,
    remote=None,
    remote_config=None,
):
    from dvx.config import Config
    from dvx.dvcfile import is_valid_filename
    from dvx.repo import Repo

    out = resolve_output(path, out, force=force)

    if is_valid_filename(out):
        raise GetDVCFileError

    if config and not isinstance(config, dict):
        config = Config.load_file(config)

    with Repo.open(
        url=url,
        rev=rev,
        subrepos=True,
        uninitialized=True,
        config=config,
        remote=remote,
        remote_config=remote_config,
    ) as repo:
        from dvx.fs import download
        from dvx.fs.data import DataFileSystem

        fs: Union[DataFileSystem, DVCFileSystem]
        if os.path.isabs(path):
            fs = DataFileSystem(index=repo.index.data["local"])
            fs_path = fs.from_os_path(path)
        else:
            fs = repo.dvcfs
            fs_path = fs.from_os_path(path)
        download(fs, fs_path, os.path.abspath(out), jobs=jobs)
