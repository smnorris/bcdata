import os
from urllib.parse import urlparse
from urllib.parse import urlencode
import logging

from psycopg2 import sql
from sqlalchemy import MetaData, Table, Column
from geoalchemy2 import Geometry
import geopandas as gpd
from shapely.geometry.point import Point
from shapely.geometry.multipoint import MultiPoint
from shapely.geometry.linestring import LineString
from shapely.geometry.multilinestring import MultiLineString
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon

import bcdata
from bcdata.database import Database

log = logging.getLogger(__name__)


def bc2pg(
    dataset,
    db_url,
    table=None,
    schema=None,
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
    table_comments, table_details = bcdata.get_table_definition(dataset)
    # find geometry type(s) in first 10 records and take the first one
    geom_type = bcdata.get_types(dataset, 10)[0]
    # make everything multipart
    # (some datasets have mixed singlepart/multipart geometries)
    if geom_type in ["POINT", "LINE", "POLYGON"]:
        geom_type = "MULTI" + geom_type

    # define db connection and connect
    db = Database(db_url)

    if schema_name + "." + table_name not in db.tables and append:
        raise ValueError("Table does not exist, nothing to append to")

    # if schema is available via bcdc and we are not appending to existing table,
    # build the table definition and create table
    if table_details:

        # drop table if it exists
        if schema_name + "." + table_name in db.tables and not append:
            logging.info(f"Dropping existing table {schema_name}.{table_name}")
            dbq = sql.SQL("DROP TABLE {schema}.{table}").format(
                schema=sql.Identifier(schema_name), table=sql.Identifier(table_name)
            )
            db.execute(dbq)

        # remove columns of unsupported types
        # (this also strips the geometry column, this is added below)
        table_details = [
            c for c in table_details if c["data_type"] in db.supported_types.keys()
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
                raise ValueError(
                    "Specified sortby column {sortby} is not present in {dataset}"
                )
            # column needs to be uppercase in request
            sortby = sortby.upper()

        # translate the oracle types to sqlalchemy provided postgres types
        columns = []
        for i in range(len(table_details)):
            column_name = table_details[i]["column_name"].lower()
            column_type = db.supported_types[table_details[i]["data_type"]]
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

        if not append:
            # create schema if it does not exist
            if schema_name not in db.schemas:
                logging.info(f"Schema {schema_name} does not exist, creating it")
                dbq = sql.SQL("CREATE SCHEMA {schema}").format(
                    schema=sql.Identifier(schema_name)
                )
                db.execute(dbq)

            # create empty table
            meta = MetaData(bind=db.engine)
            Table(
                table_name,
                meta,
                *columns,
                comment=table_comments,
                schema=schema_name,
            ).create()

    # note if schema is not available from bcdc
    if not table_details:
        log.info("No table details found via BCDC, guessing types")

    # load the data
    if not schema_only:
        # define requests
        param_dicts = bcdata.define_request(
            dataset,
            query=query,
            count=count,
            sortby=sortby,
            pagesize=pagesize,
            crs="epsg:3005",
        )
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
            # df = df[df["geom"].isna() == False]  # remove rows with null geometry
            # fill rows with null geometry
            # df["geom"] = [
            #    Geometry(geom_type, srid=3005) if feature is None else feature
            #    for feature in df["geom"]
            # ]
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
            log.info(f"Writing {dataset} to database as {schema_name}.{table_name}")
            df.to_postgis(table_name, db.engine, if_exists="append", schema=schema_name)
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
    if primary_key and not append:
        log.info(f"Adding primary key {primary_key} to {schema_name}.{table_name}")
        dbq = sql.SQL(
            "ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({primary_key})"
        ).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            primary_key=sql.Identifier(primary_key.lower()),
        )
        db.execute(dbq)
    return schema_name + "." + table_name
