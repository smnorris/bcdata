import pytest
import click
from click.testing import CliRunner

import bcdata
from bcdata.database import Database


DB_URL = "postgresql://postgres@localhost:5432/bcdata_test"
DB_CONNECTION = Database(url=DB_URL)
AIRPORTS_PACKAGE = "bc-airports"
AIRPORTS_TABLE = "whse_imagery_and_base_maps.gsr_airports_svw"
TERRACE_QUERY = "AIRPORT_NAME='Terrace (Northwest Regional) Airport'"
VICTORIA_QUERY = "Victoria Harbour (Camel Point) Heliport"
ASSESSMENTS_TABLE = "whse_fish.pscis_assessment_svw"


def test_bc2pg():
    bcdata.bc2pg(AIRPORTS_TABLE, DB_URL)
    assert AIRPORTS_TABLE in DB_CONNECTION.tables
    DB_CONNECTION.execute("drop table " + AIRPORTS_TABLE)


def test_bc2pg_count():
    bcdata.bc2pg(AIRPORTS_TABLE, DB_URL, count=10)
    assert AIRPORTS_TABLE in DB_CONNECTION.tables
    r = DB_CONNECTION.query(
        "select airport_name from whse_imagery_and_base_maps.gsr_airports_svw"
    )
    assert len(r) == 10
    DB_CONNECTION.execute("drop table " + AIRPORTS_TABLE)


def test_bc2pg_table():
    bcdata.bc2pg(AIRPORTS_TABLE, DB_URL, table="testtable")
    assert "whse_imagery_and_base_maps.testtable" in DB_CONNECTION.tables
    DB_CONNECTION.execute("drop table whse_imagery_and_base_maps.testtable")


def test_bc2pg_schema():
    bcdata.bc2pg(AIRPORTS_TABLE, DB_URL, schema="testschema")
    assert "testschema.gsr_airports_svw" in DB_CONNECTION.tables
    DB_CONNECTION.execute("drop schema testschema cascade")


def test_bc2pg_primary_key():
    bcdata.bc2pg(ASSESSMENTS_TABLE, DB_URL, primary_key="stream_crossing_id", count=100)
    assert ASSESSMENTS_TABLE in DB_CONNECTION.tables
    r = DB_CONNECTION.query("""
        SELECT a.attname FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = any(i.indkey)
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE relname = 'pscis_assessment_svw'
        AND nspname = 'whse_fish'
        AND indisprimary
        """)
    assert r[0][0] == 'stream_crossing_id'
    DB_CONNECTION.execute("drop table " + ASSESSMENTS_TABLE)


def test_bc2pg_filter():
    bcdata.bc2pg(
        AIRPORTS_TABLE,
        DB_URL,
        query="AIRPORT_NAME='Terrace (Northwest Regional) Airport'",
    )
    assert AIRPORTS_TABLE in DB_CONNECTION.tables
    r = DB_CONNECTION.query(
        "select airport_name from whse_imagery_and_base_maps.gsr_airports_svw"
    )
    assert len(r) == 1
    assert r[0][0] == "Terrace (Northwest Regional) Airport"
    DB_CONNECTION.execute("drop table " + AIRPORTS_TABLE)


def test_bc2pg_schema_only():
    bcdata.bc2pg(AIRPORTS_TABLE, DB_URL, schema_only=True)
    assert AIRPORTS_TABLE in DB_CONNECTION.tables
    r = DB_CONNECTION.query("select * from whse_imagery_and_base_maps.gsr_airports_svw")
    assert len(r) == 0
    DB_CONNECTION.execute("drop table " + AIRPORTS_TABLE)


def test_bc2pg_append():
    bcdata.bc2pg(AIRPORTS_TABLE, DB_URL, schema_only=True)
    bcdata.bc2pg(
        AIRPORTS_TABLE,
        DB_URL,
        query="AIRPORT_NAME='Terrace (Northwest Regional) Airport'",
        append=True
    )
    bcdata.bc2pg(
        AIRPORTS_TABLE,
        DB_URL,
        query="AIRPORT_NAME='Victoria International Airport'",
        append=True
    )
    r = DB_CONNECTION.query("select * from whse_imagery_and_base_maps.gsr_airports_svw")
    assert len(r) == 2
    DB_CONNECTION.execute("drop table " + AIRPORTS_TABLE)
