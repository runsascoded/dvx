import typing

from dvx.dvcfile import DVC_FILE_SUFFIX
from dvx.log import logger
from dvx.stage.exceptions import (
    StageFileDoesNotExistError,
    StageFileIsNotDvcFileError,
    StageNotFound,
)

from . import locked

if typing.TYPE_CHECKING:
    from dvx.repo import Repo

logger = logger.getChild(__name__)


@locked
def remove(self: "Repo", target: str, outs: bool = False):
    try:
        stages = self.stage.from_target(target, accept_group=False)
    except (StageNotFound, StageFileDoesNotExistError) as e:
        # If the user specified a tracked file as a target instead of a stage,
        # e.g. `data.csv` instead of `data.csv.dvc`,
        # give a more helpful error message.
        if self.fs.exists(target + DVC_FILE_SUFFIX):
            raise StageFileIsNotDvcFileError(target) from e
        raise

    for stage in stages:
        stage.remove(remove_outs=outs, force=outs)

    return stages
