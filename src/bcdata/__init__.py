import requests

from .bc2pg import bc2pg as bc2pg
from .bcdc import get_table_definition as get_table_definition
from .bcdc import get_table_name as get_table_name
from .wcs import get_dem as get_dem
from .wfs import BCWFS

PRIMARY_KEY_DB_URL = "https://raw.githubusercontent.com/smnorris/bcdata/main/data/primary_keys.json"

# BCDC does not indicate which column in the schema is the primary key.
# In this absence, bcdata maintains its own dictionary of {table: primary_key},
# served via github. Retrieve the dict with this function"""
response = requests.get(PRIMARY_KEY_DB_URL)
if response.status_code == 200:
    primary_keys = response.json()
else:
    raise Exception(f"Failed to download primary key database at {PRIMARY_KEY_DB_URL}")
    primary_keys = {}

__version__ = "0.14.0dev0"

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
        bounds_crs=bounds_crs,
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
    clean=True,
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
        clean=clean,
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
