from datetime import datetime
from datetime import timedelta
import json
import logging
import math
import os
from pathlib import Path
import sys
import warnings
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor

from owslib.wfs import WebFeatureService
import requests


import bcdata


if not sys.warnoptions:
    warnings.simplefilter("ignore")

log = logging.getLogger(__name__)


def get_sortkey(table):
    # If we don't know what we want to sort by, just pick the first
    # column in the table in alphabetical order...
    # Ideally we would get the primary key from bcdc api, but it doesn't
    # seem to be available
    wfs = WebFeatureService(url=bcdata.OWS_URL, version="2.0.0")
    return sorted(wfs.get_schema("pub:" + table)["properties"].keys())[0]


def check_cache(path):
    """Return true if the file does not exist or is older than 30 days
    """
    if not os.path.exists(path):
        return True
    else:
        # check the age
        mod_date = datetime.fromtimestamp(os.path.getmtime(path))
        if mod_date < (datetime.now() - timedelta(days=30)):
            return True
        else:
            return False


def bcdc_package_show(package):
    """Query DataBC Catalogue API about given dataset/package
    """
    params = {"id": package}
    r = requests.get(bcdata.BCDC_API_URL + "package_show", params=params)
    if r.status_code != 200:
        raise ValueError("{d} is not present in DataBC API list".format(d=package))
    return r.json()["result"]


def validate_name(dataset):
    if dataset in list_tables():
        return dataset
    else:
        return bcdc_package_show(dataset)["object_name"]


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
    # - the cache is older than 1 month
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
    """Ask DataBC WFS how many features there are in a table
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
    r = requests.get(bcdata.WFS_URL, params=parameters)
    return r.json()["features"]


def define_request(dataset, query=None, crs="epsg:4326", bbox=None, sortby=None, pagesize=10000):
    # references:
    # http://www.opengeospatial.org/standards/wfs
    # http://docs.geoserver.org/stable/en/user/services/wfs/vendor.html
    # http://docs.geoserver.org/latest/en/user/tutorials/cql/cql_tutorial.html
    table = validate_name(dataset)

    # First, can we handle the data with just one request?
    # The server imposes a 10k record limit - how many records are there?
    n = bcdata.get_count(table)

    # DataBC WFS getcapabilities says that it supports paging,
    # and the spec says that responses should include 'next URI'
    # (section 7.7.4.4.1)....
    # But I do not see any next uri in the responses. Instead of following
    # the paged urls, for datasets with >10k records, just generate urls
    # based on number of features in the dataset.
    chunks = math.ceil(n / pagesize)
    if chunks > 1 and not sortby:
        sortby = get_sortkey(table)

    # build the request parameters for each chunk
    payloads = []
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
        if query:
            request["CQL_FILTER"] = query
        if bbox:
            request["bbox"] = bbox
        if chunks > 1:
            request["startIndex"] = i * pagesize
            request["count"] = pagesize
        payloads.append(request)
    return payloads


def get_data(dataset, query=None, crs="epsg:4326", bbox=None, sortby=None, pagesize=10000):
    """Get GeoJSON from DataBC WFS
    """
    param_dicts = define_request(dataset, query, crs, bbox, sortby, 10000)

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = executor.map(make_request, param_dicts)

    outjson = dict(type="FeatureCollection", features=[])
    for result in results:
        outjson["features"] += result
    return outjson


def get_features(dataset, query=None, crs="epsg:4326", bbox=None, sortby=None, pagesize=10000):
    """Yield features from DataBC WFS
    """
    param_dicts = define_request(dataset, query, crs, bbox, sortby, 10000)

    with ThreadPoolExecutor(max_workers=5) as executor:
        for result in executor.map(make_request, param_dicts):
            for feature in result:
                yield feature
