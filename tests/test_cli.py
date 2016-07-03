from click.testing import CliRunner

from bcdata.scripts.cli import cli

GEOMARK = "gm-3D54AEE61F1847BA881E8BF7DE23BA21"
DATASET = 'bc-airports'

# test isn't working at the moment
#def test_cli_basic():
#    runner = CliRunner()
#    result = runner.invoke(cli, ['--email', DATASET])
#    assert result.exit_code == 0
