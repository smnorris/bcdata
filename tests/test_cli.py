import os

from click.testing import CliRunner

from bcdata.cli import cli
from bcdata.database import Database

DB_URL = os.environ.get("DATABASE_URL")
DB_CONNECTION = Database(url=DB_URL)
AIRPORTS_PACKAGE = "bc-airports"
AIRPORTS_TABLE = "WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW"
TERRACE_QUERY = "AIRPORT_NAME='Terrace (Northwest Regional) Airport'"
VICTORIA_QUERY = "Victoria Harbour (Camel Point) Heliport"
BBOX_LL = "-123.396104,48.404465,-123.342588,48.425401"
RIVERS_TABLE = "whse_basemapping.fwa_rivers_poly"
ASSESSMENTS_TABLE = "whse_fish.pscis_assessment_svw"

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
        cli,
        [
            "cat",
            AIRPORTS_TABLE,
            "--bounds",
            BBOX_LL,
            "--bounds_crs",
            "EPSG:4326",
        ],
    )
    assert result.exit_code == 0
    assert len(result.output.split("\n")) == 4


def test_bc2pg():
    runner = CliRunner()
    result = runner.invoke(cli, ["bc2pg", AIRPORTS_TABLE, "--db_url", DB_URL])
    assert result.exit_code == 0
    assert AIRPORTS_TABLE.lower() in DB_CONNECTION.tables
    DB_CONNECTION.execute("drop table " + AIRPORTS_TABLE.lower())


def test_bc2pg_refresh():
    runner = CliRunner()
    r1 = runner.invoke(
        cli,
        [
            "bc2pg",
            AIRPORTS_TABLE,
            "--db_url",
            DB_URL,
            "--query",
            "AIRPORT_NAME='Terrace (Northwest Regional) Airport'",
        ],
    )
    r2 = runner.invoke(
        cli,
        [
            "bc2pg",
            AIRPORTS_TABLE,
            "--db_url",
            DB_URL,
            "--refresh",
            "--query",
            "AIRPORT_NAME='Victoria International Airport'",
        ],
    )
    q = DB_CONNECTION.query("select * from whse_imagery_and_base_maps.gsr_airports_svw")
    assert r1.exit_code == 0
    assert r2.exit_code == 0
    assert AIRPORTS_TABLE.lower() in DB_CONNECTION.tables
    assert len(q) == 1
    DB_CONNECTION.execute("drop table " + AIRPORTS_TABLE.lower())


def test_bc2pg_refresh_fails_table_does_not_exist():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "bc2pg",
            AIRPORTS_TABLE,
            "--db_url",
            DB_URL,
            "--count",
            "1",
            "--refresh",
        ],
    )
    assert result.exit_code == 1
