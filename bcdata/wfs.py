from datetime import datetime
from datetime import timedelta
import json
import logging
import math
import os
from pathlib import Path
from urllib.parse import urlparse
from urllib.parse import urlencode
import sys
import warnings
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor

from owslib.wfs import WebFeatureService
import requests
import geopandas as gpd
from shapely.geometry.point import Point
from shapely.geometry.multipoint import MultiPoint
from shapely.geometry.linestring import LineString
from shapely.geometry.multilinestring import MultiLineString
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from sqlalchemy import create_engine, MetaData, Table, Column
from geoalchemy2 import Geometry
from psycopg2 import sql

import bcdata
from bcdata.database import Database

if not sys.warnoptions:
    warnings.simplefilter("ignore")

log = logging.getLogger(__name__)


def get_sortkey(table):
    """Check data for unique columns available for sorting paged requests"""
    wfs = WebFeatureService(url=bcdata.OWS_URL, version="2.0.0")
    columns = list(wfs.get_schema("pub:" + table)["properties"].keys())
    # use OBJECTID as default sort key, if present
    if "OBJECTID" in columns:
        return "OBJECTID"
    # if OBJECTID is not present (several GSR tables), use SEQUENCE_ID
    elif "SEQUENCE_ID" in columns:
        return "SEQUENCE_ID"
    # otherwise, it should be safe to presume first column is the primary key
    # (WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY's FEATURE_ID appears to be
    # the only public case, and very large veg downloads are likely better
    # accessed via some other channel)
    else:
        return columns[0]


def check_cache(path):
    """Return true if the cache file holding list of all datasets
    does not exist or is more than a day old
    (this is not very long, but checking daily seems to be a good strategy)
    """
    if not os.path.exists(path):
        return True
    else:
        # check the age
        mod_date = datetime.fromtimestamp(os.path.getmtime(path))
        if mod_date < (datetime.now() - timedelta(days=1)):
            return True
        else:
            return False


def validate_name(dataset):
    """Check wfs/cache and the bcdc api to see if dataset name is valid"""
    if dataset.upper() in list_tables():
        return dataset.upper()
    else:
        return bcdata.get_table_name(dataset.upper())


def list_tables(refresh=False, cache_file=None):
    """Return a list of all datasets available via WFS"""
    # default cache listing all objects available is
    # ~/.bcdata
    if not cache_file:
        if "BCDATA_CACHE" in os.environ:
            cache_file = os.environ["BCDATA_CACHE"]
        else:
            cache_file = os.path.join(str(Path.home()), ".bcdata")

    # regenerate the cache if:
    # - the cache file doesn't exist
    # - we force a refresh
    # - the cache is older than 1 day
    if refresh or check_cache(cache_file):
        wfs = WebFeatureService(url=bcdata.OWS_URL, version="2.0.0")
        bcdata_objects = [i.strip("pub:") for i in list(wfs.contents)]
        with open(cache_file, "w") as outfile:
            json.dump(sorted(bcdata_objects), outfile)
    else:
        with open(cache_file, "r") as infile:
            bcdata_objects = json.load(infile)

    return bcdata_objects


def get_count(dataset, query=None):
    """Ask DataBC WFS how many features there are in a table/query"""
    # https://gis.stackexchange.com/questions/45101/only-return-the-numberoffeatures-in-a-wfs-query
    table = validate_name(dataset)
    payload = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": table,
        "resultType": "hits",
        "outputFormat": "json",
    }
    if query:
        payload["CQL_FILTER"] = query
    r = requests.get(bcdata.WFS_URL, params=payload)
    return int(ET.fromstring(r.text).attrib["numberMatched"])


def make_request(parameters):
    """Submit a getfeature request to DataBC WFS and return features"""
    try:
        r = requests.get(bcdata.WFS_URL, params=parameters)
        log.info(r.url)
        r.raise_for_status()  # check status code is 200
    except requests.exceptions.HTTPError as err:  # fail if not 200
        raise SystemExit(err)
    log.debug(r.headers)
    return r.json()["features"]  # return features if status code is 200


def define_request(
    dataset,
    query=None,
    crs="epsg:4326",
    bounds=None,
    bounds_crs="EPSG:3005",
    sortby=None,
    pagesize=10000,
):
    """Define the getfeature request parameters required to download a dataset

    References:
    - http://www.opengeospatial.org/standards/wfs
    - http://docs.geoserver.org/stable/en/user/services/wfs/vendor.html
    - http://docs.geoserver.org/latest/en/user/tutorials/cql/cql_tutorial.html
    """
    # validate the table name and find out how many features it holds
    table = validate_name(dataset)
    n = bcdata.get_count(table, query=query)
    log.info(f"Total features requested: {n}")
    wfs = WebFeatureService(url=bcdata.OWS_URL, version="2.0.0")
    geom_column = wfs.get_schema("pub:" + table)["geometry_column"]

    # DataBC WFS getcapabilities says that it supports paging,
    # and the spec says that responses should include 'next URI'
    # (section 7.7.4.4.1)....
    # But I do not see any next uri in the responses. Instead of following
    # the paged urls, for datasets with >10k records, just generate urls
    # based on number of features in the dataset.
    chunks = math.ceil(n / pagesize)

    # if making several requests, we need to sort by something
    if chunks > 1 and not sortby:
        sortby = get_sortkey(table)

    # build the request parameters for each chunk
    param_dicts = []
    for i in range(chunks):
        request = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": table,
            "outputFormat": "json",
            "SRSNAME": crs,
        }
        if sortby:
            request["sortby"] = sortby
        # build the CQL based on query and bounds
        # (the bbox param shortcut is mutually exclusive with CQL_FILTER)
        if query and not bounds:
            request["CQL_FILTER"] = query
        if bounds:
            b0, b1, b2, b3 = [str(b) for b in bounds]
            bnd_query = f"bbox({geom_column}, {b0}, {b1}, {b2}, {b3}, '{bounds_crs}')"
            if not query:
                request["CQL_FILTER"] = bnd_query
            else:
                request["CQL_FILTER"] = query + " AND " + bnd_query

        if chunks > 1:
            request["startIndex"] = i * pagesize
            request["count"] = pagesize
        param_dicts.append(request)
    return param_dicts


def get_data(
    dataset,
    query=None,
    crs="epsg:4326",
    bounds=None,
    bounds_crs="epsg:3005",
    sortby=None,
    pagesize=10000,
    max_workers=2,
    as_gdf=False,
):
    """Get GeoJSON featurecollection (or geodataframe) from DataBC WFS"""
    param_dicts = define_request(
        dataset,
        query=query,
        crs=crs,
        bounds=bounds,
        bounds_crs=bounds_crs,
        sortby=sortby,
        pagesize=pagesize,
    )
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(make_request, param_dicts)

    outjson = dict(type="FeatureCollection", features=[])
    for result in results:
        outjson["features"] += result
    if not as_gdf:
        # If output crs is specified, include the crs object in the json
        # But as default, we prefer to default to 4326 and RFC7946 (no crs)
        if crs.lower() != "epsg:4326":
            crs_int = crs.split(":")[1]
            outjson[
                "crs"
            ] = f"""{{"type":"name","properties":{{"name":"urn:ogc:def:crs:EPSG::{crs_int}"}}}}"""
        return outjson
    else:
        gdf = gpd.GeoDataFrame.from_features(outjson)
        gdf.crs = {"init": crs}
        return gdf


def get_features(
    dataset,
    query=None,
    crs="epsg:4326",
    bounds=None,
    bounds_crs="epsg:3005",
    sortby=None,
    pagesize=10000,
    max_workers=2,
):
    """Yield features from DataBC WFS"""
    param_dicts = define_request(
        dataset,
        query=query,
        crs=crs,
        bounds=bounds,
        bounds_crs=bounds_crs,
        sortby=sortby,
        pagesize=pagesize,
    )
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(make_request, param_dicts):
            for feature in result:
                yield feature


def get_type(dataset):
    """Request a single feature and return geometry type"""
    # validate the table name
    table = validate_name(dataset)
    parameters = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": table,
        "outputFormat": "json",
        "count": 1,
    }
    r = requests.get(bcdata.WFS_URL, params=parameters)
    log.debug(r.url)
    # return the feature type
    return r.json()["features"][0]["geometry"]["type"]


def bc2pg(
    dataset,
    db_url,
    table=None,
    schema=None,
    query=None,
    sortby=None,
    primary_key=None,
    pagesize=10000,
):
    """Request table definition from bcdc and replicate in postgres"""
    dataset = validate_name(dataset)
    schema_name, table_name = dataset.lower().split(".")
    if schema:
        schema_name = schema
    if table:
        table_name = table
    table_comments, table_details = bcdata.get_table_definition(dataset)

    # remove columns of unsupported types (including geometry, we add this ourselves)
    table_details = [
        c for c in table_details if c["data_type"] in bcdata.DATABASE_TYPES.keys()
    ]

    # remove cruft
    table_details = [
        c
        for c in table_details
        if c["column_name"] not in ["FEATURE_AREA_SQM", "FEATURE_LENGTH_M"]
    ]

    # note column names
    column_names = [c["column_name"].lower() for c in table_details]

    # guess at geom type by requesting the first record in the collection
    geom_type = bcdata.get_type(dataset)

    # make everything multipart because some datasets have mixed singlepart/multipart geometries
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

    # loop through the requests
    for n, paramdict in enumerate(param_dicts):
        payload = urlencode(paramdict, doseq=True)
        url = bcdata.WFS_URL + "?" + payload
        df = gpd.read_file(url)
        df = df.rename_geometry("geom")
        df.columns = df.columns.str.lower()  # lowercasify
        df = df[column_names + ["geom"]]  # retain only specified columns (and geom)

        # cast to everything multipart becasue responses can have mixed types
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

        df.to_postgis(table_name, pgdb, if_exists="append", schema=schema_name)

    # geopandas automatically creates gist index on geometry
    # optionally, create primary key
    if primary_key:
        dbq = sql.SQL(
            "ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({primary_key})"
        ).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            primary_key=sql.Identifier(primary_key.lower()),
        )
        db.execute(dbq)
