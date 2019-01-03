import os
import xml.etree.ElementTree as ET

import requests


BCDC_API_URL = "https://catalogue.data.gov.bc.ca/api/3/action/"
WFS_URL = "https://openmaps.gov.bc.ca/geo/pub/"


def list_tables():
    """Get WFS capabilities and extract a list of all datasets available
    """
    url = "http://openmaps.gov.bc.ca/geo/ows"
    payload = {"service": "WFS",
               "version": "2.0.0",
               "request": "GetCapabilities"}
    r = requests.get(url, params=payload)
    root = ET.fromstring(r.text)
    # get a list of all tables
    tables = [e[0].text.split(":")[1] for e in root[3]]
    return tables


def package_show(package):
    """Query DataBC Catalogue API about given dataset/package
    """
    params = {"id": package}
    r = requests.get(BCDC_API_URL + "package_show", params=params)
    if r.status_code != 200:
        raise ValueError("{d} is not present in DataBC API list".format(d=package))
    return r.json()["result"]


def get_count(object_name):
    """Ask DataBC WFS how many features there are in a table
    """
    #https://gis.stackexchange.com/questions/45101/only-return-the-numberoffeatures-in-a-wfs-query
    url = os.path.join(WFS_URL, object_name, "wfs")
    payload = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": object_name,
        "resultType": "hits",
        "outputFormat": "json"
    }
    r = requests.get(url, params=payload)
    return int(ET.fromstring(r.text).attrib["numberMatched"])


def get_data(dataset, query=None, number=None):
    """Get GeoJSON from DataBC WFS (EPSG:3005 only)
    """
    # references:
    # http://www.opengeospatial.org/standards/wfs
    # http://docs.geoserver.org/stable/en/user/services/wfs/vendor.html
    # http://docs.geoserver.org/latest/en/user/tutorials/cql/cql_tutorial.html
    if dataset in list_tables():
        table = dataset
    else:
        table = package_show(dataset)["object_name"]

    url = os.path.join(WFS_URL, table, "wfs")
    payload = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": table,
        "outputFormat": "json",
        "SRSNAME": "epsg:3005"
    }
    if number:
        payload["count"] = str(number)
    if query:
        payload["CQL_FILTER"] = query

    r = requests.get(url, params=payload)
    if r.status_code != 200:
        ValueError("WFS error {} - check your CQL_FILTER".format(str(r.status_code)))
    else:
        return r.json()
