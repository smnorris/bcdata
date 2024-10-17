import json
import logging
import os

import geopandas as gpd
import numpy
import stamina
from geoalchemy2 import Geometry
import requests
from shapely.geometry.linestring import LineString
from shapely.geometry.multilinestring import MultiLineString
from shapely.geometry.multipoint import MultiPoint
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.point import Point
from shapely.geometry.polygon import Polygon

import bcdata
from bcdata.database import Database
from bcdata.wfs import BCWFS

log = logging.getLogger(__name__)

SUPPORTED_TYPES = [
    "POINT",
    "POINTZ",
    "MULTIPOINT",
    "MULTIPOINTZ",
    "LINESTRING",
    "LINESTRINGZ",
    "MULTILINESTRING",
    "MULTILINESTRINGZ",
    "POLYGON",
    "MULTIPOLYGON",
]


def bc2pg(  # noqa: C901
    dataset,
    db_url,
    table=None,
    schema=None,
    geometry_type=None,
    query=None,
    bounds=None,
    bounds_crs=None,
    count=None,
    sortby=None,
    primary_key=None,
    timestamp=True,
    schema_only=False,
    append=False,
    refresh=False,
):
    """Request table definition from bcdc and replicate in postgres"""
    if schema_only and append:
        raise ValueError("Options schema_only and append are not compatible")

    dataset = bcdata.validate_name(dataset)
    schema_name, table_name = dataset.lower().split(".")
    if schema:
        schema_name = schema.lower()
    if table:
        table_name = table.lower()

    # take note if geometry type is specified as option
    geometry_type_opt = geometry_type

    # do not promote to multi if singlepart specified,
    # otherwise default to always promoting to multipart in case mixed types are returned
    if geometry_type_opt and geometry_type_opt.upper() in ["POINT", "LINE", "POLYGON"]:
        promote_to_multi = False
    else:
        promote_to_multi = True

    # connect to target db
    db = Database(db_url)

    # create wfs service interface instance
    WFS = BCWFS()

    # define requests
    urls = bcdata.define_requests(
        dataset,
        query=query,
        bounds=bounds,
        bounds_crs=bounds_crs,
        count=count,
        sortby=sortby,
        crs="epsg:3005",
    )

    df = None  # just for tracking if first download is done by geometry type check

    # if appending, get column names from db, make sure table exists
    if append:
        if schema_name + "." + table_name not in db.tables:
            raise ValueError(f"{schema_name}.{table_name} does not exist")
        column_names = db.get_columns(schema_name, table_name)

    # if not appending, define and create table
    if not append:
        # get info about the table from catalogue
        table_definition = bcdata.get_table_definition(dataset)

        if not table_definition["schema"]:
            raise ValueError(
                "Cannot create table, schema details not found via bcdc api"
            )

        # if geometry type is not provided, determine type by making the first request
        if not geometry_type:
            df = WFS.make_requests(
                [urls[0]], as_gdf=True, crs="epsg:3005", lowercase=True
            )
            geometry_type = df.geom_type.unique()[0]  # keep only the first type
            if numpy.any(
                df.has_z.unique()[0]
            ):  # geopandas does not include Z in geom_type string
                geometry_type = geometry_type + "Z"

        # if geometry type is still not populated try the last request
        # (in case all entrys with geom are near the bottom)
        if not geometry_type:
            if not urls[-1] == urls[0]:
                df_temp = WFS.make_requests(
                    [urls[-1]],
                    as_gdf=True,
                    crs="epsg:3005",
                    lowercase=True,
                    silent=True,
                )
                geometry_type = df_temp.geom_type.unique()[
                    0
                ]  # keep only the first type
                if numpy.any(
                    df_temp.has_z.unique()[0]
                ):  # geopandas does not include Z in geom_type string
                    geometry_type = geometry_type + "Z"
                # drop the last request dataframe to free up memory
                del df_temp

        # ensure geom type is valid
        geometry_type = geometry_type.upper()
        if geometry_type not in SUPPORTED_TYPES:
            raise ValueError("Geometry type {geometry_type} is not supported")

        # if primary key is not supplied, use default (if present in list)
        if not primary_key and dataset.lower() in bcdata.primary_keys:
            primary_key = bcdata.primary_keys[dataset.lower()]

        # fail if specified primary key is not in the table
        if primary_key and primary_key.upper() not in [
            c["column_name"].upper() for c in table_definition["schema"]
        ]:
            raise ValueError(
                "Column {primary_key} specified as primary_key does not exist in source"
            )

        # build the table definition and create table
        table = db.define_table(
            schema_name,
            table_name,
            table_definition["schema"],
            geometry_type,
            promote_to_multi,
            table_definition["comments"],
            primary_key,
        )
        column_names = [c.name for c in table.columns]

    # check if column provided in sortby option is present in dataset
    if sortby and sortby.lower() not in column_names:
        raise ValueError(
            f"Specified sortby column {sortby} is not present in {dataset}"
        )

    # load the data
    if not schema_only:
        # loop through the requests
        for n, url in enumerate(urls):
            # if first url not downloaded above when checking geom type, do now
            if df is None:
                df = WFS.make_requests(
                    [url], as_gdf=True, crs="epsg:3005", lowercase=True
                )
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
            # (but only cast if geometry_type is not specified to be singlepart)
            if promote_to_multi:
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
            df = None

        # once load complete, note date/time of load completion in bcdata.log
        # do not log refreshes here, they called by cli once loaded to target table
        if timestamp and not refresh:
            db.log(schema_name, table_name)

    return schema_name + "." + table_name
