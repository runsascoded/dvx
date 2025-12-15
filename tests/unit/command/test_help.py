import logging

import click
import pytest

from dvc.cli import main
from dvc.cli.main import cli


def command_tuples():
    """Get all Click commands and subcommands."""
    commands = [()]  # Root command

    def recurse_group(group, parents=()):
        if isinstance(group, click.Group):
            for name in group.list_commands(None):
                cmd = group.get_command(None, name)
                cmd_path = (*parents, name)
                commands.append(cmd_path)
                if isinstance(cmd, click.Group):
                    recurse_group(cmd, cmd_path)

    recurse_group(cli)

    # DVX has fewer commands than DVC (removed experiments, plots, metrics, params, etc.)
    assert len(commands) >= 20
    return sorted(commands)


def ids(values):
    return "-".join(values) or "dvx"


@pytest.mark.parametrize("command_tuples", command_tuples(), ids=ids)
def test_help(caplog, capsys, command_tuples):
    """Test that --help works for all CLI commands."""
    with caplog.at_level(logging.INFO):
        # DVX uses Click CLI which doesn't raise SystemExit for --help
        # with standalone_mode=False, it just returns 0
        result = main([*command_tuples, "--help"])
    assert result == 0
    assert not caplog.text

    out, err = capsys.readouterr()

    # Note: Click uses uppercase metavars by convention (OPTIONS, ARGS, etc.)
    # which is different from the original argparse CLI. This is acceptable.

    assert not err
    assert out
