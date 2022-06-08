import pytest
import click
from click.testing import CliRunner

from bcdata.cli import cli
from bcdata.database import Database


DB_URL = "postgresql://postgres@localhost:5432/test_bcdata"
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
        cli, ["cat", AIRPORTS_TABLE, "--bounds", BBOX_LL, "--bounds_crs", "EPSG:4326"]
    )
    assert result.exit_code == 0
    assert len(result.output.split("\n")) == 4


def test_bc2pg():
    runner = CliRunner()
    result = runner.invoke(cli, ["bc2pg", AIRPORTS_TABLE, "--db_url", DB_URL])
    assert result.exit_code == 0
    assert AIRPORTS_TABLE.lower() in DB_CONNECTION.tables
    DB_CONNECTION.execute("drop table " + AIRPORTS_TABLE.lower())


def test_bc2pg_table():
    runner = CliRunner()
    result = runner.invoke(
        cli, ["bc2pg", AIRPORTS_TABLE, "--db_url", DB_URL, "--table", "testtable"]
    )
    assert (
        result.exit_code == 0
        and "whse_imagery_and_base_maps.testtable" in DB_CONNECTION.tables
    )
    DB_CONNECTION.execute("drop table whse_imagery_and_base_maps.testtable")


def test_bc2pg_schema():
    runner = CliRunner()
    result = runner.invoke(
        cli, ["bc2pg", AIRPORTS_TABLE, "--db_url", DB_URL, "--schema", "testschema"]
    )
    assert (
        result.exit_code == 0 and "testschema.gsr_airports_svw" in DB_CONNECTION.tables
    )
    DB_CONNECTION.execute("drop schema testschema cascade")


def test_bc2pg_pagesize():
    runner = CliRunner()
    result = runner.invoke(
        cli, ["bc2pg", AIRPORTS_TABLE, "--db_url", DB_URL, "--pagesize", 100]
    )
    assert result.exit_code == 0 and AIRPORTS_TABLE.lower() in DB_CONNECTION.tables
    DB_CONNECTION.execute("drop table " + AIRPORTS_TABLE.lower())


def test_bc2pg_maxworkers():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "bc2pg",
            RIVERS_TABLE,
            "--db_url",
            DB_URL,
            "--pagesize",
            5000,
            "--max_workers",
            4,
        ],
    )
    assert result.exit_code == 0 and RIVERS_TABLE in DB_CONNECTION.tables
    DB_CONNECTION.execute("drop table " + RIVERS_TABLE)


def test_bc2pg_fid():
    runner = CliRunner()
    result = runner.invoke(
        cli, ["bc2pg", RIVERS_TABLE, "--db_url", DB_URL, "--fid", "waterbody_poly_id"]
    )
    assert result.exit_code == 0 and RIVERS_TABLE in DB_CONNECTION.tables
    DB_CONNECTION.execute("drop table " + RIVERS_TABLE)


def test_bc2pg_append_small():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "bc2pg",
            RIVERS_TABLE,
            "--db_url",
            DB_URL,
            "--fid",
            "waterbody_poly_id",
            "--query",
            "WATERSHED_GROUP_CODE='COWN'",
        ],
    )
    result = runner.invoke(
        cli,
        [
            "bc2pg",
            RIVERS_TABLE,
            "--db_url",
            DB_URL,
            "--fid",
            "waterbody_poly_id",
            "--append",
            "--query",
            "WATERSHED_GROUP_CODE='VICT'",
        ],
    )
    assert result.exit_code == 0 and RIVERS_TABLE in DB_CONNECTION.tables
    query = "select waterbody_poly_id from whse_basemapping.fwa_rivers_poly where watershed_group_code = 'VICT'"
    assert [r[0] for r in DB_CONNECTION.query(query)] == [710022126, 710022158]
    DB_CONNECTION.execute("drop table " + RIVERS_TABLE)


def test_bc2pg_append_large():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "bc2pg",
            ASSESSMENTS_TABLE,
            "--db_url",
            DB_URL,
            "--fid",
            "stream_crossing_id",
            "--query",
            "STREAM_CROSSING_ID=198090",
        ],
    )
    result = runner.invoke(
        cli,
        [
            "bc2pg",
            ASSESSMENTS_TABLE,
            "--db_url",
            DB_URL,
            "--fid",
            "stream_crossing_id",
            "--append",
            "--query",
            "STREAM_CROSSING_ID<>198090",
        ],
    )
    assert result.exit_code == 0 and ASSESSMENTS_TABLE in DB_CONNECTION.tables
    query = "select count(*) from whse_fish.pscis_assessment_svw"
    assert DB_CONNECTION.query(query)[0][0] > 18000
    DB_CONNECTION.execute("drop table " + ASSESSMENTS_TABLE)
