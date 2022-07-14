import pytest
import click
from click.testing import CliRunner

import bcdata
from bcdata.database import Database


DB_URL = "postgresql://postgres@localhost:5432/test_bcdata"
DB_CONNECTION = Database(url=DB_URL)
AIRPORTS_PACKAGE = "bc-airports"
AIRPORTS_TABLE = "WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW"
TERRACE_QUERY = "AIRPORT_NAME='Terrace (Northwest Regional) Airport'"
VICTORIA_QUERY = "Victoria Harbour (Camel Point) Heliport"
ASSESSMENTS_TABLE = "whse_fish.pscis_assessment_svw"

# Note that these tests depend on airport counts.
# If airports are added to or removed from source layer, tests will fail


def test_bc2pg():
    bcdata.bc2pg(AIRPORTS_TABLE, DB_URL)
    assert AIRPORTS_TABLE.lower() in DB_CONNECTION.tables
    DB_CONNECTION.execute("drop table " + AIRPORTS_TABLE.lower())

def test_bc2pg_table():
    bcdata.bc2pg(AIRPORTS_TABLE, DB_URL, table="testtable")
    assert "whse_imagery_and_base_maps.testtable" in DB_CONNECTION.tables
    DB_CONNECTION.execute("drop table whse_imagery_and_base_maps.testtable")

def test_bc2pg_schema():
    bcdata.bc2pg(AIRPORTS_TABLE, DB_URL, schema="testschema")
    assert "testschema.gsr_airports_svw" in DB_CONNECTION.tables
    DB_CONNECTION.execute("drop schema testschema cascade")

def test_bc2pg_primary_key():
    bcdata.bc2pg(ASSESSMENTS_TABLE, DB_URL, primary_key="stream_crossing_id")
    assert ASSESSMENTS_TABLE in DB_CONNECTION.tables
    DB_CONNECTION.execute("drop table " + ASSESSMENTS_TABLE)

