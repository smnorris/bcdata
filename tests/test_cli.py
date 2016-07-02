from click.testing import CliRunner

from bcdata.scripts.cli import cli

GEOMARK = "gm-3D54AEE61F1847BA881E8BF7DE23BA21"
DATASET = 'bc-airports'


def test_cli_download():
    runner = CliRunner()
    result = runner.invoke(cli, ['--email', '--geomark', GEOMARK, DATASET])
    assert result.exit_code == 0
