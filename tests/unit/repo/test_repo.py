import os
import shutil

import pytest

from dvc.exceptions import OutputDuplicationError
from dvc.repo import NotDvcRepoError, Repo, locked
from dvc_data.hashfile.hash_info import HashInfo


def test_is_dvc_internal(dvc):
    assert dvc.is_dvc_internal(os.path.join("path", "to", ".dvc", "file"))
    assert not dvc.is_dvc_internal(os.path.join("path", "to-non-.dvc", "file"))


@pytest.mark.parametrize(
    "path",
    [
        os.path.join("dir", "subdir", "file"),
        os.path.join("dir", "subdir"),
        "dir",
    ],
)
def test_find_outs_by_path(tmp_dir, dvc, path):
    (stage,) = tmp_dir.dvc_gen({"dir": {"subdir": {"file": "file"}, "other": "other"}})

    outs = dvc.find_outs_by_path(path, strict=False)
    assert len(outs) == 1
    assert outs[0].fs_path == stage.outs[0].fs_path


def test_find_outs_by_path_does_graph_checks(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    shutil.copyfile("foo.dvc", "foo-2.dvc")

    dvc._reset()
    with pytest.raises(OutputDuplicationError):
        dvc.find_outs_by_path("foo")


@pytest.mark.parametrize(
    "path",
    [os.path.join("dir", "subdir", "file"), os.path.join("dir", "subdir")],
)
def test_used_objs(tmp_dir, dvc, path):
    tmp_dir.dvc_gen({"dir": {"subdir": {"file": "file"}, "other": "other"}})

    expected = {
        HashInfo("md5", "70922d6bf66eb073053a82f77d58c536.dir"),
        HashInfo("md5", "8c7dd922ad47494fc02c388e12c00eac"),
    }

    used = set()
    for _, obj_ids in dvc.used_objs([path]).items():
        used.update(obj_ids)

    assert used == expected


def test_locked(mocker):
    repo = mocker.MagicMock()
    repo._lock_depth = 0
    repo.method = locked(repo.method)

    args = ()
    kwargs = {}
    repo.method(repo, args, kwargs)

    assert repo.method_calls == [
        mocker.call._reset(),
        mocker.call.method(repo, args, kwargs),
        mocker.call._reset(),
    ]


def test_branch_config(tmp_dir, scm):
    tmp_dir.scm_gen("foo", "foo", commit="init")

    # sanity check
    with pytest.raises(NotDvcRepoError):
        Repo().close()

    scm.checkout("branch", create_new=True)
    dvc = Repo.init()
    with dvc.config.edit() as conf:
        conf["remote"]["branch"] = {"url": "/some/path"}
    dvc.close()

    scm.add([os.path.join(".dvc", "config")])
    scm.commit("init dvc")
    scm.checkout("master")

    with pytest.raises(NotDvcRepoError):
        Repo(rev="master").close()

    dvc = Repo(rev="branch")
    try:
        assert dvc.config["remote"]["branch"]["url"] == "/some/path"
    finally:
        dvc.close()


def test_dynamic_cache_initialization(tmp_dir, scm):
    dvc = Repo.init()
    with dvc.config.edit() as conf:
        conf["cache"]["ssh"] = "foo"
        conf["remote"]["foo"] = {"url": "remote://bar/baz"}
    dvc.close()

    Repo(str(tmp_dir)).close()
