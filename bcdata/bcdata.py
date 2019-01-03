import os
import xml.etree.ElementTree as ET
import sys
import warnings

from owslib.wfs import WebFeatureService
import requests

import bcdata


if not sys.warnoptions:
        warnings.simplefilter("ignore")


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


def list_tables():
    """Return a list of all datasets available via WFS
    """
    # todo: it might be helpful to cache this list for speed
    wfs = WebFeatureService(url=bcdata.OWS_URL, version="2.0.0")
    return [i.strip("pub:") for i in list(wfs.contents)]


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


def get_data(dataset, query=None, number=None, crs="epsg:3005"):
    """Get GeoJSON from DataBC WFS
    """
    # references:
    # http://www.opengeospatial.org/standards/wfs
    # http://docs.geoserver.org/stable/en/user/services/wfs/vendor.html
    # http://docs.geoserver.org/latest/en/user/tutorials/cql/cql_tutorial.html
    table = validate_name(dataset)
    payload = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": table,
        "outputFormat": "json",
        "SRSNAME": crs,
    }
    if number:
        payload["count"] = str(number)
    if query:
        payload["CQL_FILTER"] = query

    r = requests.get(bcdata.WFS_URL, params=payload)

    if r.status_code != 200:
        ValueError("WFS error {} - check your CQL_FILTER".format(str(r.status_code)))
    else:
        return r.json()
