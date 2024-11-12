import json
import logging
from urllib.parse import urlparse

import requests
import stamina

import bcdata

log = logging.getLogger(__name__)

BCDC_API_URL = "https://catalogue.data.gov.bc.ca/api/3/action/"


class ServiceException(Exception):
    pass


@stamina.retry(on=requests.HTTPError, timeout=60)
def _package_show(package):
    r = requests.get(BCDC_API_URL + "package_show", params={"id": package})
    if r.status_code in [400, 404]:
        log.error(f"HTTP error {r.status_code}")
        log.error(f"Response headers: {r.headers}")
        log.error(f"Response text: {r.text}")
        raise ValueError(f"Dataset {package} not found in DataBC API list")
    if r.status_code in [500, 502, 503, 504]:  # presumed serivce error, retry
        log.warning(f"HTTP error: {r.status_code}")
        log.warning(f"Response headers: {r.headers}")
        log.warning(f"Response text: {r.text}")
        r.raise_for_status()
    else:
        log.debug(r.text)
    return r


@stamina.retry(on=requests.HTTPError, timeout=60)
def _table_definition(table_name):
    r = requests.get(
        BCDC_API_URL + "package_search",
        params={"q": "res_extras_object_name:" + table_name},
    )
    if r.status_code != 200:
        log.warning(r.headers)
    if r.status_code in [400, 401, 404]:
        raise ServiceException(r.text)  # presumed request error
    if r.status_code in [500, 502, 503, 504]:  # presumed serivce error, retry
        r.raise_for_status()
    return r


def get_table_name(package):
    """Query DataBC API to find WFS table/layer name for given package"""
    package = package.lower()  # package names are lowercase
    r = _package_show(package)
    result = r.json()["result"]
    # Because the object_name in the result json is not a 100% reliable key
    # for WFS requests, parse URL in WMS resource(s).
    # Also, some packages may have >1 WFS layer - if this is the case, bail
    # and provide user with a list of layers
    layer_urls = [r["url"] for r in result["resources"] if r["format"] == "wms"]
    layer_names = [urlparse(url).path.split("/")[3] for url in layer_urls]
    if len(layer_names) > 1:
        raise ValueError(
            "Package {} includes more than one WFS resource, specify one of the following: \n{}".format(
                package, "\n".join(layer_names)
            )
        )
    return layer_names[0]


def get_table_definition(table_name):
    """
    Given a table/object name, search BCDC for the first package/resource with a matching "object_name",
    returns dict: {"comments": <>, "notes": <>, "schema": {<schema dict>} }
    """
    # only allow searching for tables present in WFS list
    table_name = table_name.upper()
    if table_name not in bcdata.list_tables():
        raise ValueError(
            f"Only tables available via WFS are supported, {table_name} not found"
        )

    # search the api for the provided table
    r = _table_definition(table_name)

    # start with an empty table definition dict
    table_definition = {
        "description": None,
        "comments": None,
        "schema": [],
        "primary_key": None,
    }

    # if there are no matching results, let the user know
    if r.json()["result"]["count"] == 0:
        log.warning(
            f"BC Data Catalogue API search provides no results for: {table_name}"
        )
    else:
        # iterate through results of search (packages)
        for result in r.json()["result"]["results"]:
            # description is at top level, same for all resources
            table_definition["description"] = result["notes"]
            # iterate through resources associated with each package
            for resource in result["resources"]:
                # only examine geographic resources with object name key
                if (
                    "object_name" in resource.keys()
                    and resource["bcdc_type"] == "geographic"
                ):
                    # confirm that object name matches table name and schema is present
                    if (
                        (
                            table_name == resource["object_name"]
                            # hack to handle object name / table name mismatch for NR Districts
                            or (
                                table_name
                                == "WHSE_ADMIN_BOUNDARIES.ADM_NR_DISTRICTS_SPG"
                                and resource["object_name"]
                                == "WHSE_ADMIN_BOUNDARIES.ADM_NR_DISTRICTS_SP"
                            )
                        )
                        and "details" in resource.keys()
                        and resource["details"] != ""
                    ):
                        table_definition["schema"] = json.loads(resource["details"])
                        # look for comments only if details/schema was found
                        if "object_table_comments" in resource.keys():
                            table_definition["comments"] = resource[
                                "object_table_comments"
                            ]

    if not table_definition["schema"]:
        log.warning(
            f"BC Data Catalouge API search provides no schema for: {table_name}"
        )

    # add primary key if present in bcdata.primary_keys
    if table_name.lower() in bcdata.primary_keys:
        table_definition["primary_key"] = bcdata.primary_keys[
            table_name.lower()
        ].upper()

    return table_definition
