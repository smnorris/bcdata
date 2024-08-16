import json
import logging
import math
import os
import sys
import warnings
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode

import geopandas as gpd
import requests
import stamina
from owslib.feature import schema as wfs_schema
from owslib.feature import wfs200
from owslib.wfs import WebFeatureService

import bcdata

if not sys.warnoptions:
    warnings.simplefilter("ignore")

log = logging.getLogger(__name__)


class ServiceException(Exception):
    pass


class BCWFS(object):
    """Wrapper around web feature service"""

    def __init__(self, refresh=False):
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
        self.refresh = refresh
        self.cache_refresh_days = 30
        self.capabilities = self.get_capabilities()
        # get pagesize from xml using the xpath from https://github.com/bcgov/bcdata/
        countdefault = ET.fromstring(self.capabilities).findall(
            ".//{http://www.opengis.net/ows/1.1}Constraint[@name='CountDefault']"
        )[0]
        self.pagesize = int(
            countdefault.find(
                "ows:DefaultValue", {"ows": "http://www.opengis.net/ows/1.1"}
            ).text
        )

        self.request_headers = {"User-Agent": "bcdata.py ({bcdata.__version__})"}

    def check_cached_file(self, cache_file):
        """Return true if the file is empty / does not exist / is more than n days old"""
        cache_file = os.path.join(self.cache_path, cache_file)
        if not os.path.exists(os.path.join(cache_file)):
            return True
        else:
            mod_date = datetime.fromtimestamp(os.path.getmtime(cache_file))
            # if file older than specified days or empty, return true
            if (
                mod_date < (datetime.now() - timedelta(days=self.cache_refresh_days))
                or os.stat(cache_file).st_size == 0
            ):
                return True
            else:
                return False

    @stamina.retry(on=requests.HTTPError, timeout=60)
    def _request_schema(self, table):
        schema = wfs_schema.get_schema(
            "https://openmaps.gov.bc.ca/geo/pub/ows",
            typename=table,
            version="2.0.0",
        )
        return schema

    @stamina.retry(on=requests.HTTPError, timeout=60)
    def _request_capabilities(self):
        capabilities = ET.tostring(
            wfs200.WebFeatureService_2_0_0(
                self.ows_url, "2.0.0", None, False
            )._capabilities,
            encoding="unicode",
        )
        return capabilities

    @stamina.retry(on=requests.HTTPError, timeout=60)
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

        r = requests.get(self.wfs_url, params=payload, headers=self.request_headers)
        log.debug(r.url)
        if r.status_code in [400, 401, 404]:
            log.error(f"HTTP error {r.status_code}")
            log.error(f"Response headers: {r.headers}")
            log.error(f"Response text: {r.text}")
            raise ServiceException(r.text)  # presumed request error
        elif r.status_code in [500, 502, 503, 504]:  # presumed serivce error, retry
            log.warning(f"HTTP error: {r.status_code}, retrying")
            log.warning(f"Response headers: {r.headers}")
            log.warning(f"Response text: {r.text}")
            r.raise_for_status()
        return int(ET.fromstring(r.text).attrib["numberMatched"])

    @stamina.retry(on=requests.HTTPError, timeout=60)
    def _request_features(self, url, silent=False):
        """Submit a getfeature request to DataBC WFS and return features"""
        r = requests.get(url, headers=self.request_headers)
        if not silent:
            log.info(r.url)
        else:
            log.debug(r.url)
        if r.status_code in [400, 401, 404]:
            log.error(f"HTTP error {r.status_code}")
            log.error(f"Response headers: {r.headers}")
            log.error(f"Response text: {r.text}")
            raise ServiceException(r.text)  # presumed request error
        elif r.status_code in [500, 502, 503, 504]:  # presumed serivce error, retry
            log.warning(f"HTTP error: {r.status_code}")
            log.warning(f"Response headers: {r.headers}")
            log.warning(f"Response text: {r.text}")
            r.raise_for_status()
        return r.json()["features"]

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

    def get_capabilities(self):
        """
        Request server capabilities (layer definitions).
        Cache response as file daily, caching to one of:
          - $BCDATA_CACHE environment variable
          - default (~/.bcdata)
        """
        # request capabilities if cached file is old or refresh is specified
        if self.check_cached_file("capabilities.xml") or self.refresh:
            with open(os.path.join(self.cache_path, "capabilities.xml"), "w") as f:
                f.write(self._request_capabilities())
        # load cached xml from file
        with open(os.path.join(self.cache_path, "capabilities.xml"), "r") as f:
            return f.read()

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
        return count

    def get_schema(self, table):
        # download table definition if file is > 30 days old, empty, or refresh is specified
        if self.check_cached_file(table) or self.refresh:
            with open(os.path.join(self.cache_path, table), "w") as f:
                schema = self._request_schema(table)
                f.write(json.dumps(schema, indent=4))
        # load cached schema
        with open(os.path.join(self.cache_path, table), "r") as f:
            return json.loads(f.read())

    def get_sortkey(self, table):
        """Check data for unique columns available for sorting paged requests"""
        columns = list(self.get_schema(table)["properties"].keys())
        # use known primary key if it is present in the bcdata repository
        if table.lower() in bcdata.primary_keys:
            return bcdata.primary_keys[table.lower()].upper()
        # if pk not known, use OBJECTID as default sort key when present
        elif "OBJECTID" in columns:
            return "OBJECTID"
        # if OBJECTID is not present (several GSR tables), use SEQUENCE_ID
        elif "SEQUENCE_ID" in columns:
            return "SEQUENCE_ID"
        # otherwise, presume first column is best value to sort by
        # (in some cases this will be incorrect)
        else:
            log.warning(
                f"Reliable sort key for {table} cannot be determined, defaulting to first column {columns[0]}"
            )
            return columns[0]

    def list_tables(self):
        """read and parse capabilities xml, which lists all tables available"""
        return [
            i.strip("pub:")
            for i in list(
                WebFeatureService(
                    self.ows_url, version="2.0.0", xml=self.capabilities
                ).contents
            )
        ]

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
        chunks = math.ceil(count / self.pagesize)

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
                request["startIndex"] = i * self.pagesize
                if count < (request["startIndex"] + self.pagesize):
                    request["count"] = count - request["startIndex"]
                else:
                    request["count"] = self.pagesize
            urls.append(self.wfs_url + "?" + urlencode(request, doseq=True))
        return urls

    def make_requests(
        self, urls, as_gdf=False, crs="epsg4326", lowercase=False, silent=False
    ):
        """turn urls into data"""
        # loop through urls
        results = []
        for url in urls:
            results.append(self._request_features(url, silent))
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
                gdf.crs = crs
            else:
                gdf = gpd.GeoDataFrame()
            return gdf

    def get_data(
        self,
        dataset,
        query=None,
        crs="epsg:4326",
        bounds=None,
        bounds_crs="epsg:3005",
        count=None,
        sortby=None,
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
        )
        return self.make_requests(urls, as_gdf, crs, lowercase)

    def get_features(
        self,
        dataset,
        query=None,
        crs="epsg:4326",
        bounds=None,
        bounds_crs="epsg:3005",
        count=None,
        sortby=None,
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
            check_count=check_count,
        )
        for url in urls:
            for feature in self._request_features(url):
                if lowercase:
                    feature["properties"] = {
                        k.lower(): v for k, v in feature["properties"].items()
                    }
                yield feature


# abstract away the WFS object


def define_requests(
    dataset,
    query=None,
    crs="epsg:4326",
    bounds=None,
    bounds_crs="EPSG:3005",
    count=None,
    sortby=None,
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
        lowercase=lowercase,
        check_count=check_count,
    )


def get_sortkey(dataset):
    WFS = BCWFS()
    table = WFS.validate_name(dataset)
    return WFS.get_sortkey(table)


def list_tables(refresh=False):
    WFS = BCWFS(refresh)
    return WFS.list_tables()


def validate_name(dataset):
    WFS = BCWFS()
    return WFS.validate_name(dataset)
