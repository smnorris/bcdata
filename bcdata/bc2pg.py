import os
import logging

from geoalchemy2 import Geometry
import geopandas as gpd
from shapely.geometry.point import Point
from shapely.geometry.multipoint import MultiPoint
from shapely.geometry.linestring import LineString
from shapely.geometry.multilinestring import MultiLineString
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from tenacity import retry
from tenacity.stop import stop_after_delay
from tenacity.wait import wait_random_exponential

import bcdata
from bcdata.database import Database

log = logging.getLogger(__name__)

SUPPORTED_TYPES = [
    "POINT",
    "POINTZ",
    "MULTIPOINT",
    "MULTIPOINTZ",
    "LINESTRING",
    "MULTILINESTRING",
    "MULTILINESTRINGZ",
    "POLYGON",
    "MULTIPOLYGON",
]


@retry(stop=stop_after_delay(120), wait=wait_random_exponential(multiplier=1, max=60))
def _download(url):
    """offload download requests to geopandas, using tenacity to handle unsuccessful requests"""
    try:
        data = gpd.read_file(url)
    except Exception:
        log.error("Data transfer error")
    return data


def bc2pg(
    dataset,
    db_url,
    table=None,
    schema=None,
    geometry_type=None,
    query=None,
    count=None,
    sortby=None,
    primary_key=None,
    pagesize=10000,
    timestamp=True,
    schema_only=False,
    append=False,
):
    """Request table definition from bcdc and replicate in postgres"""
    dataset = bcdata.validate_name(dataset)
    schema_name, table_name = dataset.lower().split(".")
    if schema:
        schema_name = schema.lower()
    if table:
        table_name = table.lower()

    # connect to target db
    db = Database(db_url)

    # if appending, get column names from db
    if append:
        # make sure table actually exists
        if schema_name + "." + table_name not in db.tables:
            raise ValueError(
                f"{schema_name}.{table_name} does not exist, nothing to append to"
            )
        column_names = db.get_columns(schema_name, table_name)

    # if not appending, define and create table
    else:
        # get info about the table from catalouge
        table_comments, table_details = bcdata.get_table_definition(dataset)

        if not table_details:
            raise ValueError(
                "Cannot create table, schema details not found via bcdc api"
            )

        # clean provided geometry type and ensure it is valid
        if geometry_type:
            geometry_type = geometry_type.upper()
            if geometry_type not in SUPPORTED_TYPES:
                raise ValueError("Geometry type {geometry_type} is not supported")

        # if geometry type is not provided, infer from first 10 records in dataset
        if not geometry_type:
            geometry_type = bcdata.get_spatial_types(dataset, 10)[0]

        # build the table definition and create table
        table = db.define_table(
            schema_name,
            table_name,
            table_details,
            geometry_type,
            table_comments,
            primary_key,
            append,
        )
        column_names = [c.name for c in table.columns]

    # check if column provided in sortby option is present in dataset
    if sortby and sortby.lower() not in column_names:
        raise ValueError(
            f"Specified sortby column {sortby} is not present in {dataset}"
        )

    # load the data
    if not schema_only:
        # define requests
        urls = bcdata.define_requests(
            dataset,
            query=query,
            count=count,
            sortby=sortby,
            pagesize=pagesize,
            crs="epsg:3005",
        )
        # loop through the requests
        for n, url in enumerate(urls):
            log.info(url)
            df = _download(url)
            log.info(_download.retry.statistics)  # log the retry stats
            # tidy the resulting dataframe
            df = df.rename_geometry("geom")
            # lowercasify
            df.columns = df.columns.str.lower()
            # retain only columns matched in table definition
            df = df[column_names]
            # extract features with no geometry
            df_nulls = df[df["geom"].isna()]
            # keep this df for loading with pandas
            df_nulls = df_nulls.drop(columns=["geom"])
            # remove rows with null geometry from geodataframe
            df = df[df["geom"].notna()]
            # cast to everything multipart because responses can have mixed types
            # geopandas does not have a built in function:
            # https://gis.stackexchange.com/questions/311320/casting-geometry-to-multi-using-geopandas
            df["geom"] = [
                MultiPoint([feature]) if isinstance(feature, Point) else feature
                for feature in df["geom"]
            ]
            df["geom"] = [
                MultiLineString([feature])
                if isinstance(feature, LineString)
                else feature
                for feature in df["geom"]
            ]
            df["geom"] = [
                MultiPolygon([feature]) if isinstance(feature, Polygon) else feature
                for feature in df["geom"]
            ]

            # run the load in two parts, one with geoms, one with no geoms
            log.info(f"Writing {dataset} to database as {schema_name}.{table_name}")
            df.to_postgis(table_name, db.engine, if_exists="append", schema=schema_name)
            df_nulls.to_sql(
                table_name,
                db.engine,
                if_exists="append",
                schema=schema_name,
                index=False,
            )

        # once load complete, note date/time of load completion in public.bcdata
        if timestamp:
            log.info("Logging download date to bcdata.log")
            db.execute(
                """CREATE SCHEMA IF NOT EXISTS bcdata;
                   CREATE TABLE IF NOT EXISTS bcdata.log (
                     table_name text PRIMARY KEY,
                     latest_download timestamp WITH TIME ZONE
                   );
                """
            )
            db.execute(
                """INSERT INTO bcdata.log (table_name, latest_download)
                   SELECT %s as table_name, NOW() as latest_download
                   ON CONFLICT (table_name) DO UPDATE SET latest_download = NOW();
                """,
                (schema_name + "." + table_name,),
            )

    return schema_name + "." + table_name
