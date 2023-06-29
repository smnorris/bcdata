from datetime import datetime
from datetime import timedelta
import json
import logging
import math
from pathlib import Path
import os
from urllib.parse import urlencode
import sys
import warnings
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor

from owslib.wfs import WebFeatureService
import requests
from tenacity import retry
from tenacity.stop import stop_after_delay
from tenacity.wait import wait_random_exponential
from tenacity.retry import retry_if_exception_type
import geopandas as gpd

import bcdata

if not sys.warnoptions:
    warnings.simplefilter("ignore")

log = logging.getLogger(__name__)

WFS_URL = "https://openmaps.gov.bc.ca/geo/pub/wfs"
OWS_URL = "http://openmaps.gov.bc.ca/geo/ows"


def check_cache(path):
    """Return true if the getcapabilities cache does not exist or is more than a day old"""
    if not os.path.exists(path):
        return True
    else:
        # check the age
        mod_date = datetime.fromtimestamp(os.path.getmtime(path))
        if mod_date < (datetime.now() - timedelta(days=1)):
            return True
        else:
            return False


def get_capabilities(refresh=False, cache_file=None):
    """
    Request server capabilities (layer definitions).
    Cache response as file daily, caching to location specified by:
      - cache_file parameter
      - $BCDATA_CACHE environment variable
      - default (~/.bcdata)
    """
    if not cache_file:
        if "BCDATA_CACHE" in os.environ:
            cache_file = os.environ["BCDATA_CACHE"]
        else:
            cache_file = os.path.join(str(Path.home()), ".bcdata")

    # download capabilites xml if file is > 1 day old or refresh is specified
    if check_cache(cache_file) or refresh:
        with open(cache_file, "w") as f:
            f.write(
                ET.tostring(
                    WebFeatureService(OWS_URL, version="2.0.0")._capabilities,
                    encoding="unicode",
                )
            )

    # load cached xml to WFS object
    with open(cache_file, "r") as f:
        return WebFeatureService(OWS_URL, version="2.0.0", xml=f.read())


def get_sortkey(table, schema):
    """Check data for unique columns available for sorting paged requests"""
    columns = list(schema["properties"].keys())
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


def validate_name(dataset):
    """Check wfs/cache and the bcdc api to see if dataset name is valid"""
    if dataset.upper() in list_tables():
        return dataset.upper()
    else:
        return bcdata.get_table_name(dataset.upper())


def list_tables(cache_file=None):
    """Return a list of all tables available via WFS"""
    capabilites = get_capabilities(cache_file)
    return [i.strip("pub:") for i in list(capabilites.contents)]


@retry(
    retry=retry_if_exception_type(requests.exceptions.HTTPError),
    stop=stop_after_delay(10),
    wait=wait_random_exponential(multiplier=1, max=60),
)
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

    r = requests.get(WFS_URL, params=payload)
    log.debug(r.url)
    r.raise_for_status()  # check status code is 200
    return int(ET.fromstring(r.text).attrib["numberMatched"])


def define_requests(
    dataset,
    query=None,
    crs="epsg:4326",
    bounds=None,
    bounds_crs="EPSG:3005",
    count=None,
    sortby=None,
    pagesize=10000,
):
    """Translate provided parameters into a list of WFS request URLs required
    to download the dataset as specified

    References:
    - http://www.opengeospatial.org/standards/wfs
    - http://docs.geoserver.org/stable/en/user/services/wfs/vendor.html
    - http://docs.geoserver.org/latest/en/user/tutorials/cql/cql_tutorial.html
    """
    # validate the table name and find out how many features it holds
    table = validate_name(dataset)
    n = bcdata.get_count(table, query=query)
    # if count not provided or if it is greater than n of total features,
    # set count to number of features
    if not count or count > n:
        count = n
    log.info(f"Total features requested: {count}")
    wfs = get_capabilities()
    schema = wfs.get_schema("pub:" + table)
    geom_column = schema["geometry_column"]

    # for datasets with >10k records, generate a list of urls based on number of features in the dataset.
    chunks = math.ceil(count / pagesize)

    # if making several requests, we need to sort by something
    if chunks > 1 and not sortby:
        sortby = get_sortkey(table, schema)

    # build the request parameters for each chunk
    urls = []
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
            request["sortby"] = sortby.upper()
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
        if chunks == 1:
            request["count"] = count
        if chunks > 1:
            request["startIndex"] = i * pagesize
            if count < (request["startIndex"] + pagesize):
                request["count"] = count - request["startIndex"]
            else:
                request["count"] = pagesize
        urls.append(WFS_URL + "?" + urlencode(request, doseq=True))
    return urls


@retry(
    retry=retry_if_exception_type(requests.exceptions.HTTPError),
    stop=stop_after_delay(10),
    wait=wait_random_exponential(multiplier=1, max=60),
)
def make_request(url):
    """Submit a getfeature request to DataBC WFS and return features"""
    r = requests.get(url)
    log.info(r.url)
    log.debug(r.headers)
    r.raise_for_status()  # check status code is 200, otherwise HTTPError is raised
    return r.json()["features"]


def get_data(
    dataset,
    query=None,
    crs="epsg:4326",
    bounds=None,
    bounds_crs="epsg:3005",
    count=None,
    sortby=None,
    pagesize=10000,
    as_gdf=False,
    lowercase=False,
):
    """Request features from DataBC WFS and return GeoJSON featurecollection or geodataframe"""
    urls = define_requests(
        dataset,
        query=query,
        crs=crs,
        bounds=bounds,
        bounds_crs=bounds_crs,
        count=count,
        sortby=sortby,
        pagesize=pagesize,
    )
    # loop through requests
    results = []
    for url in urls:
        results.append(make_request(url))

    outjson = dict(type="FeatureCollection", features=[])
    for result in results:
        outjson["features"] += result
    # if specified, lowercasify all properties
    if lowercase:
        for feature in outjson["features"]:
            feature["properties"] = {
                k.lower(): v for k, v in feature["properties"].items()
            }
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
        if len(outjson["features"]) > 0:
            gdf = gpd.GeoDataFrame.from_features(outjson)
            gdf.crs = {"init": crs}
        else:
            gdf = gpd.GeoDataFrame()
        return gdf


def get_features(
    dataset,
    query=None,
    crs="epsg:4326",
    bounds=None,
    bounds_crs="epsg:3005",
    count=None,
    sortby=None,
    pagesize=10000,
    lowercase=False,
):
    """Yield features from DataBC WFS"""
    urls = define_requests(
        dataset,
        query=query,
        crs=crs,
        bounds=bounds,
        bounds_crs=bounds_crs,
        count=count,
        sortby=sortby,
        pagesize=pagesize,
    )
    for url in urls:
        for feature in make_request(url):
            if lowercase:
                feature["properties"] = {
                    k.lower(): v for k, v in feature["properties"].items()
                }
            yield feature


def get_types(dataset, count=10):
    """Return distinct types within the first n features"""
    # validate the table name
    table = validate_name(dataset)
    log.info("Getting feature geometry type")
    # get first n features, examine the feature geometry type (where geometry is not empty)
    geom_types = []
    for f in get_features(table, count=count):
        if f["geometry"]:
            geom_type = f["geometry"]["type"].upper()
            # only these geometry types are expected/supported
            if geom_type not in (
                "POINT",
                "LINESTRING",
                "POLYGON",
                "MULTIPOINT",
                "MULTILINESTRING",
                "MULTIPOLYGON",
            ):
                raise ValueError("Geometry type {geomtype} is not supported")
            # look for z dimension, modify type if found
            if (
                (geom_type == "POINT" and len(f["geometry"]["coordinates"]) == 3)
                or (
                    geom_type == "MULTIPOINT"
                    and len(f["geometry"]["coordinates"][0]) == 3
                )
                or (
                    geom_type == "LINESTRING"
                    and len(f["geometry"]["coordinates"][0]) == 3
                )
                or (
                    geom_type == "MULTILINESTRING"
                    and len(f["geometry"]["coordinates"][0][0]) == 3
                )
            ):
                geom_type = geom_type + "Z"
            geom_types.append(geom_type)
    geom_types = list(set(geom_types))
    # issue warning if types are mixed
    if len(geom_types) > 1:
        typestring = ",".join(geom_types)
        log.warning(f"Dataset {dataset} has multiple geometry types: {typestring}")
    return geom_types
