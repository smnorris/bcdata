from datetime import datetime
from datetime import timedelta
import json
import logging
import math
import os
from pathlib import Path
from urllib.parse import urlparse
import sys
import warnings
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor

from owslib.wfs import WebFeatureService
import requests
import geopandas as gpd

import bcdata


if not sys.warnoptions:
    warnings.simplefilter("ignore")

log = logging.getLogger(__name__)


def get_sortkey(table):
    """Check data for unique columns available for sorting paged requests
    """
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


def get_table_name(package):
    """Query DataBC API to find WFS table/layer name for given package
    """
    package = package.lower() # package names are lowercase
    params = {"id": package}
    r = requests.get(bcdata.BCDC_API_URL + "package_show", params=params)
    if r.status_code != 200:
        raise ValueError("{d} is not present in DataBC API list".format(d=package))
    result = r.json()["result"]
    # Because the object_name in the result json is not a 100% reliable key
    # for WFS requests, parse URL in WMS resource(s).
    # Also, some packages may have >1 WFS layer - if this is the case, bail
    # and provide user with a list of layers
    layer_urls = [r["url"] for r in result["resources"] if r["format"] == "wms"]
    layer_names = [urlparse(l).path.split("/")[3] for l in layer_urls]
    if len(layer_names) > 1:
        raise ValueError(
            "Package {} includes more than one WFS resource, specify one of the following: \n{}".format(
                package, "\n".join(layer_names)
            )
        )
    return layer_names[0]


def validate_name(dataset):
    """Check wfs/cache and the bcdc api to see if dataset name is valid
    """
    if dataset.upper() in list_tables():
        return dataset.upper()
    else:
        return get_table_name(dataset.upper())


def list_tables(refresh=False, cache_file=None):
    """Return a list of all datasets available via WFS
    """
    # default cache listing all objects available is
    # ~/.bcdata
    if not cache_file:
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
    """Ask DataBC WFS how many features there are in a table/query
    """
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
    """Submit a getfeature request to DataBC WFS and return features
    """
    r = requests.get(bcdata.WFS_URL, params=parameters)
    log.debug(r.url)
    return r.json()["features"]


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
    """Get GeoJSON featurecollection (or geodataframe) from DataBC WFS
    """
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
    # If output crs is specified, include the crs object in the json
    # But as default, we prefer to default to 4326 and RFC7946 (no crs)
    if crs.lower() != "epsg:4326":
        crs_int = crs.split(":")[1]
        outjson["crs"] = f'''{{"type":"name","properties":{{"name":"urn:ogc:def:crs:EPSG::{crs_int}"}}}}'''
    for result in results:
        outjson["features"] += result
    if not as_gdf:
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
    """Yield features from DataBC WFS
    """
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
