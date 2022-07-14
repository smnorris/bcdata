import json
import logging
import os
from pathlib import Path
from urllib.parse import urlparse

import requests
import geopandas as gpd

import bcdata


log = logging.getLogger(__name__)


def get_table_name(package):
    """Query DataBC API to find WFS table/layer name for given package"""
    package = package.lower()  # package names are lowercase
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


def get_table_definition(table_name):
    """
    Given a table/object name, search BCDC for the first package/resource with a
    matching "object_name", returns tuple (table comments, table schema)
    """
    # only allow searching for tables present in WFS list
    if table_name not in bcdata.list_tables():
        raise ValueError(
            "Only tables available via WFS are supported, {table_name} not found".format(
                d=package
            )
        )
    # search the api for the provided table
    r = requests.get(bcdata.BCDC_API_URL + "package_search", params={"q": table_name})
    # catch general api errors
    status_code = r.status_code
    if status_code != 200:
        raise ValueError(f"Error searching BC Data Catalogue API: {status_code}")
    # if there are no matching results, let the user know
    if r.json()["result"]["count"] == 0:
        log.warning(
            f"BC Data Catalouge API search provides no results for: {table_name}"
        )
        return []
    else:
        matches = []
        # iterate through results of search (packages)
        for result in r.json()["result"]["results"]:
            # iterate through resources associated with each package
            for resource in result["resources"]:
                # if resource is wms and url matches provided table name,
                # extract the metadata
                if resource["format"] == "wms":
                    if urlparse(resource["url"]).path.split("/")[3] == table_name:
                        if (
                            "object_table_comments" in resource.keys()
                            and "details" in resource.keys()
                        ):
                            matches.append(
                                (resource["object_table_comments"], resource["details"])
                            )
        # uniquify the result
        # (presuming there is only one unique result, but this seems safe as we are
        # matching on the oracle table name)
        matched = list(set(matches))[0]
        return (matched[0], json.loads(matched[1]))
