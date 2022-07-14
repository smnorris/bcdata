import os
from urllib.parse import urlparse
from urllib.parse import urlencode
import logging

import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, MetaData, Table, Column
from geoalchemy2 import Geometry
import geopandas as gpd
from shapely.geometry.point import Point
from shapely.geometry.multipoint import MultiPoint
from shapely.geometry.linestring import LineString
from shapely.geometry.multilinestring import MultiLineString
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon

import bcdata

log = logging.getLogger(__name__)


class Database(object):
    """A simple wrapper around a psycopg connection"""

    def __init__(self, url=os.environ.get("DATABASE_URL")):
        self.url = url
        u = urlparse(url)
        db, user, password, host, port = (
            u.path[1:],
            u.username,
            u.password,
            u.hostname,
            u.port,
        )
        self.database = db
        self.user = user
        self.password = password
        self.host = host
        self.port = u.port
        self.conn = psycopg2.connect(url)
        # make sure postgis is available
        try:
            self.query("SELECT postgis_full_version()")
        except psycopg2.errors.UndefinedFunction:
            log.error("Cannot find PostGIS, is extension added to database %s ?", url)
            raise psycopg2.errors.UndefinedFunction

    @property
    def schemas(self):
        """List all non-system schemas in db"""
        sql = """SELECT schema_name FROM information_schema.schemata
                 ORDER BY schema_name"""
        schemas = self.query(sql)
        return [s[0] for s in schemas if s[0][:3] != "pg_"]

    @property
    def tables(self):
        """List all non-system tables in the db"""
        tables = []
        for schema in self.schemas:
            tables = tables + [schema + "." + t for t in self.tables_in_schema(schema)]
        return tables

    def tables_in_schema(self, schema):
        """Get a listing of all tables in given schema"""
        sql = """SELECT table_name
                 FROM information_schema.tables
                 WHERE table_schema = %s"""
        return [t[0] for t in self.query(sql, (schema,))]

    def query(self, sql, params=None):
        """Execute sql and return all results"""
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(sql, params)
                result = curs.fetchall()
        return result

    def execute(self, sql, params=None):
        """Execute sql and return only whether the query was successful"""
        with self.conn:
            with self.conn.cursor() as curs:
                result = curs.execute(sql, params)
        return result

    def execute_many(self, sql, params):
        """Execute many sql"""
        with self.conn:
            with self.conn.cursor() as curs:
                curs.executemany(sql, params)


def bc2pg(
    dataset,
    db_url,
    table=None,
    schema=None,
    query=None,
    sortby=None,
    primary_key=None,
    pagesize=10000,
    timestamp=True,
    schema_only=False,
):
    """Request table definition from bcdc and replicate in postgres"""
    dataset = bcdata.validate_name(dataset)
    schema_name, table_name = dataset.lower().split(".")
    if schema:
        schema_name = schema
    if table:
        table_name = table
    table_comments, table_details = bcdata.get_table_definition(dataset)

    # remove columns of unsupported types
    # (this also strips the geometry column, this is added below)
    table_details = [
        c for c in table_details if c["data_type"] in bcdata.DATABASE_TYPES.keys()
    ]

    # remove redundant columns
    table_details = [
        c
        for c in table_details
        if c["column_name"] not in ["FEATURE_AREA_SQM", "FEATURE_LENGTH_M"]
    ]

    # note resulting column names
    column_names = [c["column_name"].lower() for c in table_details]

    # check if column provided in sortby option is present in dataset
    if sortby:
        if sortby.lower() not in column_names:
            raise ValueError("Specified sortby column {sortby} is not present in {dataset}")
        # column needs to be uppercase in request
        sortby = sortby.upper()


    # guess at geom type by requesting the first record in the collection
    geom_type = bcdata.get_type(dataset)

    # make everything multipart
    # (some datasets have mixed singlepart/multipart geometries)
    if geom_type[:5] != "MULTI":
        geom_type = "MULTI" + geom_type

    # translate the oracle types to sqlalchemy provided postgres types
    columns = []
    for i in range(len(table_details)):
        column_name = table_details[i]["column_name"].lower()
        column_type = bcdata.DATABASE_TYPES[table_details[i]["data_type"]]
        # append precision if varchar or numeric
        if table_details[i]["data_type"] in ["VARCHAR2", "NUMBER"]:
            column_type = column_type(int(table_details[i]["data_precision"]))
        # check that comments are present
        if "column_comments" in table_details[i].keys():
            column_comments = table_details[i]["column_comments"]
        else:
            column_comments = None
        columns.append(
            Column(
                column_name,
                column_type,
                comment=column_comments,
            )
        )

    # add geometry column
    columns.append(Column("geom", Geometry(geom_type, srid=3005)))

    # create psycopg2 connection
    db = Database(db_url)

    # create schema if it does not exist
    if schema_name not in db.schemas:
        logging.info(f"Schema {schema_name} does not exist, creating it")
        dbq = sql.SQL("CREATE SCHEMA {schema}").format(
            schema=sql.Identifier(schema_name)
        )
        db.execute(dbq)

    # drop table if it exists
    if dataset.lower() in db.tables:
        logging.info(f"Dropping existing table {schema_name}.{table_name}")
        dbq = sql.SQL("DROP TABLE {schema}.{table}").format(
            schema=sql.Identifier(schema_name), table=sql.Identifier(table_name)
        )
        db.execute(dbq)

    # create sqlalchemy connection
    pgdb = create_engine(db_url)
    post_meta = MetaData(bind=pgdb.engine)

    # create empty table
    Table(
        table_name.lower(),
        post_meta,
        *columns,
        comment=table_comments,
        schema=schema_name,
    ).create()

    # define requests
    param_dicts = bcdata.define_request(
        dataset,
        query=query,
        sortby=sortby,
        pagesize=pagesize,
        crs="epsg:3005",
    )

    # load the data
    if not schema_only:
        # loop through the requests
        for n, paramdict in enumerate(param_dicts):
            payload = urlencode(paramdict, doseq=True)
            url = bcdata.WFS_URL + "?" + payload

            # download with geopandas, let geopandas handle errors
            log.info(url)
            df = gpd.read_file(url)

            # tidy the result
            df = df.rename_geometry("geom")
            df.columns = df.columns.str.lower()  # lowercasify
            df = df[column_names + ["geom"]]  # retain only specified columns (and geom)

            # cast to everything multipart because responses can have mixed types
            # geopandas does not have a built in function:
            # https://gis.stackexchange.com/questions/311320/casting-geometry-to-multi-using-geopandas
            df["geom"] = [
                MultiPoint([feature]) if isinstance(feature, Point) else feature
                for feature in df["geom"]
            ]
            df["geom"] = [
                MultiLineString([feature]) if isinstance(feature, LineString) else feature
                for feature in df["geom"]
            ]
            df["geom"] = [
                MultiPolygon([feature]) if isinstance(feature, Polygon) else feature
                for feature in df["geom"]
            ]
            log.info(f"Writing {dataset} to database as {schema_name}.{table_name}")
            df.to_postgis(table_name, pgdb, if_exists="append", schema=schema_name)
            # note that geopandas automatically indexes the geometry

        # once load complete, note date/time of load completion in public.bcdata
        if timestamp:
            log.info("Logging download date to public.bcdata")
            db.execute(
                "CREATE TABLE IF NOT EXISTS public.bcdata (table_name text PRIMARY KEY, date_downloaded timestamp WITH TIME ZONE);"
            )
            db.execute(
                """INSERT INTO public.bcdata (table_name, date_downloaded)
                            SELECT %s as table_name, NOW() as date_downloaded
                            ON CONFLICT (table_name) DO UPDATE SET date_downloaded = NOW();
                         """,
                (schema_name + "." + table_name,),
            )
    # optionally, create primary key
    if primary_key:
        log.info(f"Adding primary key {primary_key} to {schema_name}.{table_name}")
        dbq = sql.SQL(
            "ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({primary_key})"
        ).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            primary_key=sql.Identifier(primary_key.lower()),
        )
        db.execute(dbq)