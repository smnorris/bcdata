from click.testing import CliRunner

from bcdata.cli import cli


AIRPORTS_PACKAGE = "bc-airports"
AIRPORTS_TABLE = "WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW"
TERRACE_QUERY = "AIRPORT_NAME='Terrace (Northwest Regional) Airport'"
BBOX_LL = "-123.396104,48.404465,-123.342588,48.425401"

# Note that these tests depend on airport counts.
# If airports are added to or removed from source layer, tests will fail


def test_info_table():
    runner = CliRunner()
    result = runner.invoke(cli, ["info", AIRPORTS_TABLE])
    assert result.exit_code == 0
    assert 'name": "{}"'.format(AIRPORTS_TABLE) in result.output
    assert '"count": 455' in result.output


def test_info_package():
    runner = CliRunner()
    result = runner.invoke(cli, ["info", AIRPORTS_PACKAGE])
    assert result.exit_code == 0
    assert 'name": "{}"'.format(AIRPORTS_TABLE) in result.output
    assert '"count": 455' in result.output


def test_list():
    runner = CliRunner()
    result = runner.invoke(cli, ["list"])
    assert result.exit_code == 0
    assert AIRPORTS_TABLE in result.output


def test_cat():
    runner = CliRunner()
    result = runner.invoke(cli, ["cat", AIRPORTS_TABLE])
    assert result.exit_code == 0
    assert len(result.output.split("\n")) == 456


def test_cat_query():
    runner = CliRunner()
    result = runner.invoke(cli, ["cat", AIRPORTS_TABLE, "--query", TERRACE_QUERY])
    assert result.exit_code == 0
    assert len(result.output.split("\n")) == 2


def test_cat_bounds_ll():
    runner = CliRunner()
    result = runner.invoke(
        cli, ["cat", AIRPORTS_TABLE, "--bounds", BBOX_LL, "--bounds_crs", "EPSG:4326"]
    )
    assert result.exit_code == 0
    assert len(result.output.split("\n")) == 4
