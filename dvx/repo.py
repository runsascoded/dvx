"""DVX Repo - wrapper around DVC's Repo class.

Exposes only the core data versioning operations, hiding pipeline/experiment
functionality.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dvc.repo import Repo as DVCRepo


class Repo:
    """DVX repository - a minimal wrapper around DVC.

    This class wraps dvc.repo.Repo and exposes only the data versioning
    operations (add, push, pull, checkout, etc.), intentionally excluding
    pipeline and experiment functionality.

    Example:
        >>> repo = Repo()
        >>> repo.add("data/")
        >>> repo.push()
    """

    def __init__(
        self,
        root_dir: str | None = None,
        rev: str | None = None,
        subrepos: bool = False,
        uninitialized: bool = False,
    ):
        """Initialize DVX repo.

        Args:
            root_dir: Root directory of the repo. If None, searches upward.
            rev: Git revision to operate on (for read-only operations).
            subrepos: Whether to include subrepos.
            uninitialized: Allow operating in uninitialized repo.
        """
        from dvc.repo import Repo as DVCRepo

        self._repo: DVCRepo = DVCRepo(
            root_dir=root_dir,
            rev=rev,
            subrepos=subrepos,
            uninitialized=uninitialized,
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close the repo and release resources."""
        self._repo.close()

    @property
    def root_dir(self) -> str:
        """Root directory of the repository."""
        return self._repo.root_dir

    @property
    def dvc_dir(self) -> str | None:
        """Path to .dvc directory."""
        return self._repo.dvc_dir

    # =========================================================================
    # Core data versioning operations
    # =========================================================================

    def add(
        self,
        targets: str | list[str],
        no_commit: bool = False,
        glob: bool = False,
        **kwargs: Any,
    ):
        """Track file(s) with DVC.

        Args:
            targets: File or directory path(s) to track.
            no_commit: Don't auto-commit to git.
            glob: Enable globbing for targets.
            **kwargs: Additional arguments passed to dvc add.

        Returns:
            List of stages created.
        """
        if isinstance(targets, str):
            targets = [targets]
        return self._repo.add(targets, no_commit=no_commit, glob=glob, **kwargs)

    def push(
        self,
        targets: list[str] | None = None,
        jobs: int | None = None,
        remote: str | None = None,
        all_branches: bool = False,
        all_tags: bool = False,
        all_commits: bool = False,
        glob: bool = False,
        **kwargs: Any,
    ) -> int:
        """Push cached files to remote storage.

        Args:
            targets: Specific .dvc files to push. If None, pushes all.
            jobs: Number of parallel jobs.
            remote: Remote name to push to.
            all_branches: Push cache for all branches.
            all_tags: Push cache for all tags.
            all_commits: Push cache for all commits.
            glob: Enable globbing for targets.

        Returns:
            Number of files pushed.
        """
        return self._repo.push(
            targets=targets,
            jobs=jobs,
            remote=remote,
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
            glob=glob,
            **kwargs,
        )

    def pull(
        self,
        targets: list[str] | None = None,
        jobs: int | None = None,
        remote: str | None = None,
        all_branches: bool = False,
        all_tags: bool = False,
        all_commits: bool = False,
        force: bool = False,
        glob: bool = False,
        **kwargs: Any,
    ) -> int:
        """Pull data files from remote storage.

        Args:
            targets: Specific .dvc files to pull. If None, pulls all.
            jobs: Number of parallel jobs.
            remote: Remote name to pull from.
            all_branches: Pull cache for all branches.
            all_tags: Pull cache for all tags.
            all_commits: Pull cache for all commits.
            force: Force pull even if local files exist.
            glob: Enable globbing for targets.

        Returns:
            Number of files pulled.
        """
        return self._repo.pull(
            targets=targets,
            jobs=jobs,
            remote=remote,
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
            force=force,
            glob=glob,
            **kwargs,
        )

    def fetch(
        self,
        targets: list[str] | None = None,
        jobs: int | None = None,
        remote: str | None = None,
        all_branches: bool = False,
        all_tags: bool = False,
        all_commits: bool = False,
        **kwargs: Any,
    ) -> int:
        """Fetch data files from remote to cache (without checkout).

        Args:
            targets: Specific .dvc files to fetch.
            jobs: Number of parallel jobs.
            remote: Remote name to fetch from.
            all_branches: Fetch for all branches.
            all_tags: Fetch for all tags.
            all_commits: Fetch for all commits.

        Returns:
            Number of files fetched.
        """
        return self._repo.fetch(
            targets=targets,
            jobs=jobs,
            remote=remote,
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
            **kwargs,
        )

    def checkout(
        self,
        targets: list[str] | None = None,
        force: bool = False,
        **kwargs: Any,
    ):
        """Checkout data files from cache to workspace.

        Args:
            targets: Specific files/directories to checkout.
            force: Force checkout even if local changes exist.

        Returns:
            Checkout result info.
        """
        return self._repo.checkout(targets=targets, force=force, **kwargs)

    def status(
        self,
        targets: list[str] | None = None,
        cloud: bool = False,
        remote: str | None = None,
        all_branches: bool = False,
        all_tags: bool = False,
        all_commits: bool = False,
        **kwargs: Any,
    ) -> dict:
        """Show status of DVC files.

        Args:
            targets: Specific files to check status.
            cloud: Check status against remote storage.
            remote: Specific remote to check against.
            all_branches: Check all branches.
            all_tags: Check all tags.
            all_commits: Check all commits.

        Returns:
            Dictionary with status information.
        """
        return self._repo.status(
            targets=targets,
            cloud=cloud,
            remote=remote,
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
            **kwargs,
        )

    def diff(
        self,
        a_rev: str | None = None,
        b_rev: str | None = None,
        targets: list[str] | None = None,
        **kwargs: Any,
    ) -> dict:
        """Show changes between commits, commit and working tree, etc.

        Args:
            a_rev: First revision to compare.
            b_rev: Second revision to compare.
            targets: Specific files to diff.

        Returns:
            Dictionary with diff information.
        """
        return self._repo.diff(a_rev=a_rev, b_rev=b_rev, targets=targets, **kwargs)

    def gc(
        self,
        workspace: bool = False,
        all_branches: bool = False,
        all_tags: bool = False,
        all_commits: bool = False,
        cloud: bool = False,
        remote: str | None = None,
        force: bool = False,
        jobs: int | None = None,
        **kwargs: Any,
    ) -> dict:
        """Garbage collect unused cache files.

        Args:
            workspace: Keep only files used in current workspace.
            all_branches: Keep files used in any branch.
            all_tags: Keep files used in any tag.
            all_commits: Keep files used in any commit.
            cloud: Also garbage collect remote storage.
            remote: Specific remote to gc.
            force: Force gc without confirmation.
            jobs: Number of parallel jobs.

        Returns:
            Dictionary with gc results.
        """
        return self._repo.gc(
            workspace=workspace,
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
            cloud=cloud,
            remote=remote,
            force=force,
            jobs=jobs,
            **kwargs,
        )

    def remove(
        self,
        targets: str | list[str],
        outs: bool = False,
        **kwargs: Any,
    ):
        """Stop tracking file(s) with DVC.

        Args:
            targets: .dvc file(s) to remove.
            outs: Also remove the output files.
        """
        if isinstance(targets, str):
            targets = [targets]
        return self._repo.remove(targets, outs=outs, **kwargs)

    def move(self, src: str, dst: str, **kwargs: Any):
        """Move a DVC-tracked file or directory.

        Args:
            src: Source path.
            dst: Destination path.
        """
        return self._repo.move(src, dst, **kwargs)

    # =========================================================================
    # Import operations
    # =========================================================================

    def imp(
        self,
        url: str,
        path: str,
        out: str | None = None,
        rev: str | None = None,
        **kwargs: Any,
    ):
        """Import a file from another DVC repository.

        Args:
            url: URL of the DVC repository.
            path: Path within that repository.
            out: Output path (default: same as path basename).
            rev: Git revision in the source repo.
        """
        return self._repo.imp(url=url, path=path, out=out, rev=rev, **kwargs)

    def imp_url(
        self,
        url: str,
        out: str | None = None,
        **kwargs: Any,
    ):
        """Import a file from a URL.

        Args:
            url: URL to import from.
            out: Output path.
        """
        return self._repo.imp_url(url=url, out=out, **kwargs)

    # =========================================================================
    # Static/class methods
    # =========================================================================

    @staticmethod
    def init(root_dir: str = ".", no_scm: bool = False, force: bool = False):
        """Initialize a new DVC repository.

        Args:
            root_dir: Directory to initialize.
            no_scm: Initialize without git.
            force: Force init even if already initialized.

        Returns:
            Initialized Repo instance.
        """
        from dvc.repo import Repo as DVCRepo

        dvc_repo = DVCRepo.init(root_dir=root_dir, no_scm=no_scm, force=force)
        repo = Repo.__new__(Repo)
        repo._repo = dvc_repo
        return repo

    @staticmethod
    def get(url: str, path: str, out: str | None = None, rev: str | None = None):
        """Download a file from a DVC repository (without tracking).

        Args:
            url: URL of the DVC repository.
            path: Path within that repository.
            out: Output path.
            rev: Git revision.
        """
        from dvc.repo import Repo as DVCRepo

        return DVCRepo.get(url=url, path=path, out=out, rev=rev)

    @staticmethod
    def get_url(url: str, out: str | None = None):
        """Download a file from a URL (without tracking).

        Args:
            url: URL to download.
            out: Output path.
        """
        from dvc.repo import Repo as DVCRepo

        return DVCRepo.get_url(url=url, out=out)
