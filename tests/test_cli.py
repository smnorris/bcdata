import os
import shutil

from click.testing import CliRunner

from bcdata.scripts.cli import cli

GEOMARK = "gm-3D54AEE61F1847BA881E8BF7DE23BA21"
DATASET = 'bc-airports'


def setup():
    if not os.path.exists('tests/data'):
        os.mkdir('tests/data')


def test_cli_basic():
    runner = CliRunner()
    result = runner.invoke(cli,
                           ['--output', 'tests/data/airports.gdb', DATASET])
    assert result.exit_code == 0
    assert os.path.exists("tests/data/airports.gdb") == 1


def teardown():
    shutil.rmtree("tests/data")
