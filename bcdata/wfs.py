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


class BCWFS(object):
    """Wrapper around owslib WebFeatureService to manage cached resources"""

    def __init__(self, cache_path=None, refresh=False):
        self.wfs_url = "https://openmaps.gov.bc.ca/geo/pub/wfs"
        self.ows_url = "http://openmaps.gov.bc.ca/geo/ows"

        # point to cache path
        if cache_path:
            self.cache_path = cache_path
        else:
            if "BCDATA_CACHE" in os.environ:
                self.cache_path = os.environ["BCDATA_CACHE"]
            else:
                self.cache_path = os.path.join(str(Path.home()), ".bcdata")
        # if a file exists in the path provided AND the file name is .bcdata, delete it
        p = Path(self.cache_path)
        if p.is_file():
            if self.cache_path[-7:] == ".bcdata":
                p.unlink()
            # if the file is named something else, prompt user to delete it
            else:
                raise RuntimeError(
                    f"Cache file exists, delete before using bcdata: {self.cache_path}"
                )
        # create cache folder if it does not exist
        p.mkdir(parents=True, exist_ok=True)
        # load capabilities
        self.capabilities = self.get_capabilities(refresh)

    def check_cached_file(self, cache_file, days=1):
        """Return true if the file is empty / does not exist / is more than n days old"""
        cache_file = os.path.join(self.cache_path, cache_file)
        if not os.path.exists(os.path.join(cache_file)):
            return True
        else:
            mod_date = datetime.fromtimestamp(os.path.getmtime(cache_file))
            # if file older than specified days or empty, return true
            if (
                mod_date < (datetime.now() - timedelta(days=days))
                or os.stat(cache_file).st_size == 0
            ):
                return True
            else:
                return False

    @retry(
        stop=stop_after_delay(120), wait=wait_random_exponential(multiplier=1, max=60)
    )
    def _request_capabilities(self):
        try:
            capabilities = ET.tostring(
                WebFeatureService(self.ows_url, version="2.0.0")._capabilities,
                encoding="unicode",
            )
        except Exception:
            log.error("WFS Error")
        return capabilities

    @retry(
        stop=stop_after_delay(120), wait=wait_random_exponential(multiplier=1, max=60)
    )
    def _request_schema(self, table):
        try:
            # self.capabilities attribute is owslib.wfs.WebFeatureService instance
            schema = self.capabilities.get_schema("pub:" + table)
        except Exception:
            log.error("WFS Error")
        return schema

    @retry(
        retry=retry_if_exception_type(requests.exceptions.HTTPError),
        stop=stop_after_delay(120),
        wait=wait_random_exponential(multiplier=1, max=60),
    )
    def _request_count(self, table, query=None):
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
        try:
            r = requests.get(self.wfs_url, params=payload)
            log.debug(r.url)
            log.debug(r.headers)
            r.raise_for_status()  # check status code is 200
            return int(ET.fromstring(r.text).attrib["numberMatched"])
        except Exception:
            log.debug("WFS error")

    @retry(
        retry=retry_if_exception_type(requests.exceptions.HTTPError),
        stop=stop_after_delay(120),
        wait=wait_random_exponential(multiplier=1, max=60),
    )
    def _request_features(self, url):
        """Submit a getfeature request to DataBC WFS and return features"""
        try:
            r = requests.get(url)
            log.info(r.url)
            log.debug(r.headers)
            r.raise_for_status()  # check status code is 200, otherwise HTTPError is raised
            return r.json()["features"]
        except Exception:
            log.debug("WFS error")

    def get_capabilities(self, refresh=False):
        """
        Request server capabilities (layer definitions).
        Cache response as file daily, caching to one of:
          - $BCDATA_CACHE environment variable
          - default (~/.bcdata)
        """
        # request capabilities if cached capabilities file is > 1 day old or refresh is specified
        if self.check_cached_file("capabilities.xml", days=1) or refresh:
            with open(os.path.join(self.cache_path, "capabilities.xml"), "w") as f:
                f.write(self._request_capabilities())
        # load cached xml from file
        with open(os.path.join(self.cache_path, "capabilities.xml"), "r") as f:
            return WebFeatureService(self.ows_url, version="2.0.0", xml=f.read())

    def get_count(self, dataset, query=None):
        """Ask DataBC WFS how many features there are in a table/query"""
        # https://gis.stackexchange.com/questions/45101/only-return-the-numberoffeatures-in-a-wfs-query
        table = self.validate_name(dataset)
        count = self._request_count(table, query)
        log.info(self._request_count.retry.statistics)
        return count

    def get_schema(self, table, refresh=False):
        # download table definition if file is > 30 days old, empty, or refresh is specified
        if self.check_cached_file(table, days=30) or refresh:
            with open(os.path.join(self.cache_path, table), "w") as f:
                schema = self._request_schema(table)
                f.write(json.dumps(schema, indent=4))
        # load cached schema
        with open(os.path.join(self.cache_path, table), "r") as f:
            return json.loads(f.read())

    def get_sortkey(self, table):
        """Check data for unique columns available for sorting paged requests"""
        columns = list(self.get_schema(table)["properties"].keys())
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

    def list_tables(self):
        """Return a list of all tables available via WFS"""
        return [i.strip("pub:") for i in list(self.capabilities.contents)]

    def validate_name(self, dataset):
        """Check wfs/cache and the bcdc api to see if dataset name is valid"""
        if dataset.upper() in self.list_tables():
            return dataset.upper()
        else:
            return bcdata.get_table_name(dataset.upper())

    def define_requests(
        self,
        dataset,
        query=None,
        crs="epsg:4326",
        bounds=None,
        bounds_crs="EPSG:3005",
        count=None,
        sortby=None,
        pagesize=10000,
        check_count=True,
    ):
        """Translate provided parameters into a list of WFS request URLs required
        to download the dataset as specified

        References:
        - http://www.opengeospatial.org/standards/wfs
        - http://docs.geoserver.org/stable/en/user/services/wfs/vendor.html
        - http://docs.geoserver.org/latest/en/user/tutorials/cql/cql_tutorial.html
        """
        # validate the table name
        table = self.validate_name(dataset)
        # find out how many records are in the table
        if not count and check_count is False:
            raise ValueError(
                "{count: Null, check_count=False} is invalid, either provide record count or let bcdata request it"
            )
        elif (
            not count and check_count is True
        ):  # if not provided a count, get one if not told otherwise
            count = self.get_count(table, query=query)
        elif (
            count and check_count is True
        ):  # if provided a count that is bigger than actual number of records, automatically correct count
            n = self.get_count(table, query=query)
            if count > n:
                count = n

        log.info(f"Total features requested: {count}")
        schema = self.get_schema(table)
        geom_column = schema["geometry_column"]

        # for datasets with >10k records, generate a list of urls based on number of features in the dataset.
        chunks = math.ceil(count / pagesize)

        # if making several requests, we need to sort by something
        if chunks > 1 and not sortby:
            sortby = self.get_sortkey(table)

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
                bnd_query = (
                    f"bbox({geom_column}, {b0}, {b1}, {b2}, {b3}, '{bounds_crs}')"
                )
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
            urls.append(self.wfs_url + "?" + urlencode(request, doseq=True))
        return urls

    def get_data(
        self,
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
        urls = self.define_requests(
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
            results.append(self._request_features(url))
            log.info(self._request_features.retry.statistics)

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
        self,
        dataset,
        query=None,
        crs="epsg:4326",
        bounds=None,
        bounds_crs="epsg:3005",
        count=None,
        sortby=None,
        pagesize=10000,
        lowercase=False,
        check_count=True,
    ):
        """Yield features from DataBC WFS"""
        urls = self.define_requests(
            dataset,
            query=query,
            crs=crs,
            bounds=bounds,
            bounds_crs=bounds_crs,
            count=count,
            sortby=sortby,
            pagesize=pagesize,
            check_count=check_count,
        )
        for url in urls:
            for feature in self._request_features(url):
                if lowercase:
                    feature["properties"] = {
                        k.lower(): v for k, v in feature["properties"].items()
                    }
                yield feature
            log.info(self._request_features.retry.statistics)

    def get_spatial_types(self, dataset, count=10):
        """Return distinct types within the first n features"""
        # validate the table name
        table = self.validate_name(dataset)
        log.info("Getting feature geometry type")
        # get first n features, examine the feature geometry type (where geometry is not empty)
        geom_types = []
        for f in self.get_features(
            table, count=count, check_count=False
        ):  # to minimize network traffic, do not check record count for this requests
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


# abstract away the WFS object


def define_requests(
    dataset,
    query=None,
    crs="epsg:4326",
    bounds=None,
    bounds_crs="EPSG:3005",
    count=None,
    sortby=None,
    pagesize=10000,
    check_count=True,
):
    WFS = BCWFS()
    return WFS.define_requests(
        dataset,
        query=query,
        crs=crs,
        bounds=bounds,
        count=count,
        sortby=sortby,
        pagesize=pagesize,
        check_count=check_count,
    )


def get_count(dataset, query=None):
    WFS = BCWFS()
    return WFS.get_count(dataset, query=query)


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
    WFS = BCWFS()
    return WFS.get_data(
        dataset,
        query=query,
        crs=crs,
        bounds=bounds,
        bounds_crs=bounds_crs,
        count=count,
        sortby=sortby,
        pagesize=pagesize,
        as_gdf=as_gdf,
        lowercase=lowercase,
    )


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
    check_count=True,
):
    WFS = BCWFS()
    return WFS.get_features(
        dataset,
        query=query,
        crs=crs,
        bounds=bounds,
        bounds_crs=bounds_crs,
        count=count,
        sortby=sortby,
        pagesize=pagesize,
        lowercase=lowercase,
        check_count=check_count,
    )


def get_spatial_types(dataset, count=10):
    WFS = BCWFS()
    return WFS.get_spatial_types(dataset, count=count)


def list_tables(refresh=False):
    WFS = BCWFS(refresh=refresh)
    return WFS.list_tables()


def validate_name(dataset):
    WFS = BCWFS()
    return WFS.validate_name(dataset)
