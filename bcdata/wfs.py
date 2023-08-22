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

from owslib.feature import schema as wfs_schema
from owslib.feature import wfs200
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
    """Wrapper around web feature service"""

    def __init__(self):
        self.wfs_url = "https://openmaps.gov.bc.ca/geo/pub/wfs"
        self.ows_url = (
            "http://openmaps.gov.bc.ca/geo/pub/ows?service=WFS&request=Getcapabilities"
        )

        # point to cache path
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
    def _request_schema(self, table):
        try:
            schema = wfs_schema.get_schema(
                "https://openmaps.gov.bc.ca/geo/pub/ows",
                typename=table,
                version="2.0.0",
            )
        except Exception:
            log.debug("WFS/network error")
        return schema

    @retry(
        stop=stop_after_delay(120), wait=wait_random_exponential(multiplier=1, max=60)
    )
    def _list_tables(self):
        try:
            wfs = wfs200.WebFeatureService_2_0_0(self.ows_url, "2.0.0", None, False)
        except Exception:
            log.debug("WFS/network error")
        return [i.strip("pub:") for i in list(wfs.contents)]

    @retry(
        stop=stop_after_delay(120), wait=wait_random_exponential(multiplier=1, max=60)
    )
    def _describe_feature_type(self, table):
        """get table schema via DescribeFeatureType request"""
        payload = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "DescribeFeatureType",
            "typeName": table,
        }
        try:
            r = requests.get("https://openmaps.gov.bc.ca/geo/pub/ows", params=payload)
            log.debug(r.url)
            log.debug(r.headers)
            r.raise_for_status()  # check status code is 200
            schema = ET.fromstring(r.text)
        except Exception:
            log.debug("WFS/network error")
        return schema

    @retry(
        stop=stop_after_delay(120),
        wait=wait_random_exponential(multiplier=1, max=60),
    )
    def _request_count(
        self, table, query=None, bounds=None, bounds_crs=None, geom_column=None
    ):
        payload = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": table,
            "resultType": "hits",
            "outputFormat": "json",
        }
        if query or bounds:
            payload["CQL_FILTER"] = self.build_bounds_filter(
                query=query,
                bounds=bounds,
                bounds_crs=bounds_crs,
                geom_column=geom_column,
            )
        try:
            r = requests.get(self.wfs_url, params=payload)
            log.debug(r.url)
            log.debug(r.headers)
            r.raise_for_status()  # check status code is 200
            count = int(ET.fromstring(r.text).attrib["numberMatched"])
            # because table name has been validated, a count should always be returned
            # if empty count returned, presume network/service error and retry
            if not count:
                raise ValueError("No count returned")
        except Exception:
            log.debug("WFS/network error")
        return count

    @retry(
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
            features = r.json()["features"]
            # because table name has been validated, features should always be returned
            # if features element is empty, presume network/service error and retry
            if not features:
                raise ValueError("No features returned")
        except Exception:
            log.debug("WFS/network error")
        return features

    def build_bounds_filter(self, query, bounds, bounds_crs, geom_column):
        """The bbox param shortcut is mutually exclusive with CQL_FILTER,
        combine query and bounds into a single CQL_FILTER expression
        """
        # return query untouched if no bounds provided
        if not bounds:
            if query:
                cql_filter = query
            else:
                cql_filter = None
        # parse the bounds into a bbox
        elif bounds:
            b0, b1, b2, b3 = [str(b) for b in bounds]
            bnd_query = f"bbox({geom_column}, {b0}, {b1}, {b2}, {b3}, '{bounds_crs}')"
            if query:
                cql_filter = query + " AND " + bnd_query
            else:
                cql_filter = bnd_query
        return cql_filter

    def get_count(
        self, dataset, query=None, bounds=None, bounds_crs="EPSG:3005", geom_column=None
    ):
        """Ask DataBC WFS how many features there are in a table/query/bounds"""
        table = self.validate_name(dataset)
        geom_column = self.get_schema(table)["geometry_column"]
        count = self._request_count(
            table,
            query=query,
            bounds=bounds,
            bounds_crs=bounds_crs,
            geom_column=geom_column,
        )
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

    def list_tables(self, refresh=False):
        """Make a GetCapabilites request and return a list of all tables available via WFS"""
        if self.check_cached_file("tables.txt", days=1) or refresh:
            with open(os.path.join(self.cache_path, "tables.txt"), "w") as f:
                f.write("\n".join(self._list_tables()))
        # load cached table list from file
        with open(os.path.join(self.cache_path, "tables.txt"), "r") as f:
            return f.read().splitlines()

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

        # get name of the geometry column
        schema = self.get_schema(table)
        geom_column = schema["geometry_column"]

        # find out how many records are in the table
        if not count and check_count is False:
            raise ValueError(
                "{count: Null, check_count=False} is invalid, either provide record count or let bcdata request it"
            )
        elif (
            not count and check_count is True
        ):  # if not provided a count, get one if not told otherwise
            count = self.get_count(
                table,
                query=query,
                bounds=bounds,
                bounds_crs=bounds_crs,
                geom_column=geom_column,
            )
        elif (
            count and check_count is True
        ):  # if provided a count that is bigger than actual number of records, automatically correct count
            n = self.get_count(
                table,
                query=query,
                bounds=bounds,
                bounds_crs=bounds_crs,
                geom_column=geom_column,
            )
            if count > n:
                count = n

        log.info(f"Total features requested: {count}")

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
            if query or bounds:
                request["CQL_FILTER"] = self.build_bounds_filter(
                    query=query,
                    bounds=bounds,
                    bounds_crs=bounds_crs,
                    geom_column=geom_column,
                )
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


def get_count(dataset, query=None, bounds=None, bounds_crs="EPSG:3005"):
    WFS = BCWFS()
    table = WFS.validate_name(dataset)
    geom_column = WFS.get_schema(table)["geometry_column"]
    return WFS.get_count(
        dataset,
        query=query,
        bounds=bounds,
        bounds_crs=bounds_crs,
        geom_column=geom_column,
    )


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


def list_tables(refresh=False):
    WFS = BCWFS()
    return WFS.list_tables(refresh)


def validate_name(dataset):
    WFS = BCWFS()
    return WFS.validate_name(dataset)
