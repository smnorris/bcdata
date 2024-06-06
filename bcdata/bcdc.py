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
    r = requests.get(BCDC_API_URL + "package_search", params={"q": table_name})
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
            notes = result["notes"]
            # iterate through resources associated with each package
            for resource in result["resources"]:
                # wms format resource
                if resource["format"] == "wms":
                    # if wms, check for table name match in this location
                    if urlparse(resource["url"]).path.split("/")[3] == table_name:
                        if "object_table_comments" in resource.keys():
                            table_comments = resource["object_table_comments"]
                        else:
                            table_comments = None
                        # only add to matches if schema details found
                        if "details" in resource.keys() and resource["details"] != "":
                            table_details = resource["details"]
                            matches.append((notes, table_comments, table_details))
                            log.debug(resource)

                # multiple format resource
                elif resource["format"] == "multiple":
                    # if multiple format, check for table name match in this location
                    if resource["preview_info"]:
                        # check that layer_name key is present
                        if "layer_name" in json.loads(resource["preview_info"]):
                            # then check if it matches the table name
                            if (
                                json.loads(resource["preview_info"])["layer_name"]
                                == table_name
                            ):
                                if "object_table_comments" in resource.keys():
                                    table_comments = resource["object_table_comments"]
                                else:
                                    table_comments = None
                                # only add to matches if schema details found
                                if (
                                    "details" in resource.keys()
                                    and resource["details"] != ""
                                ):
                                    table_details = resource["details"]
                                    matches.append(
                                        (notes, table_comments, table_details)
                                    )
                                    log.debug(resource)

        # uniquify the result
        if len(matches) > 0:
            matched = list(set(matches))[0]
            return {
                "description": matched[0],  # notes=description
                "comments": matched[1],
                "schema": json.loads(matched[2]),
            }
        else:
            raise ValueError(
                f"BCDC search for {table_name} does not return a table schema"
            )
