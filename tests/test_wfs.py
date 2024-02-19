import os

import pytest
import requests
import requests_mock
import stamina
from geopandas.geodataframe import GeoDataFrame

import bcdata
from bcdata.wfs import ServiceException

AIRPORTS_PACKAGE = "bc-airports"
AIRPORTS_TABLE = "WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW"
UTMZONES_KEY = "utm-zones-of-british-columbia"
BEC_KEY = "biogeoclimatic-ecosystem-classification-bec-map"
ASSESSMENTS_TABLE = "whse_fish.pscis_assessment_svw"
GLACIERS_TABLE = "whse_basemapping.fwa_glaciers_poly"
STREAMS_TABLE = "whse_basemapping.fwa_stream_networks_sp"
WELLS_TABLE = "whse_water_management.gw_water_wells_wrbc_svw"


@pytest.fixture(autouse=True, scope="session")
def deactivate_retries():
    stamina.set_active(False)


def test_http_error_502():
    with requests_mock.mock() as m:
        m.get(requests_mock.ANY, status_code=502)
        with pytest.raises(requests.HTTPError):
            bcdata.get_data(AIRPORTS_TABLE)


def test_http_error_404():
    with requests_mock.mock() as m:
        m.get(requests_mock.ANY, status_code=404)
        with pytest.raises(bcdata.wfs.ServiceException):
            bcdata.get_data(AIRPORTS_TABLE)


def test_validate_table_lowercase():
    table = bcdata.validate_name(AIRPORTS_TABLE.lower())
    assert table == AIRPORTS_TABLE


def test_get_table_name_urlparse():
    table = bcdata.get_table_name("natural-resource-nr-district")
    assert table == "WHSE_ADMIN_BOUNDARIES.ADM_NR_DISTRICTS_SPG"


def test_get_count():
    table = bcdata.get_table_name(UTMZONES_KEY)
    assert bcdata.get_count(table) == 6


def test_get_count_filtered():
    assert bcdata.get_count(UTMZONES_KEY, query="UTM_ZONE=10") == 1


def test_get_count_bounds():
    assert (
        bcdata.get_count(AIRPORTS_TABLE, bounds=[1188000, 377051, 1207437, 390361]) == 8
    )


def test_get_sortkey_known():
    assert bcdata.get_sortkey(ASSESSMENTS_TABLE) == "STREAM_CROSSING_ID"


def test_get_sortkey_unknown():
    assert bcdata.get_sortkey(AIRPORTS_TABLE) == "SEQUENCE_ID"


def test_get_data_asgdf():
    gdf = bcdata.get_data(UTMZONES_KEY, query="UTM_ZONE=10", as_gdf=True)
    assert type(gdf) is GeoDataFrame


def test_get_data_asgdf_crs():
    gdf = bcdata.get_data(
        UTMZONES_KEY, query="UTM_ZONE=10", as_gdf=True, crs="EPSG:3005"
    )
    assert gdf.crs == "EPSG:3005"


def test_get_null_gdf():
    gdf = bcdata.get_data(UTMZONES_KEY, query="UTM_ZONE=9999", as_gdf=True)
    assert type(gdf) is GeoDataFrame


def test_get_data_small():
    data = bcdata.get_data(AIRPORTS_TABLE)
    assert data["type"] == "FeatureCollection"


def test_get_data_lowercase():
    data = bcdata.get_data(AIRPORTS_TABLE, lowercase=True)
    assert "airport_name" in data["features"][0]["properties"].keys()


def test_get_data_crs():
    data = bcdata.get_data(AIRPORTS_TABLE, crs="EPSG:3005")
    assert (
        data["crs"]
        == """{"type":"name","properties":{"name":"urn:ogc:def:crs:EPSG::3005"}}"""
    )


def test_get_features():
    data = [f for f in bcdata.get_features(AIRPORTS_TABLE)]
    assert len(data) == 455


def test_get_data_count():
    data = bcdata.get_data(AIRPORTS_TABLE, count=100)
    assert len(data["features"]) == 100


# this presumes the page size will always be less than the total number of wells
def test_get_data_paged_count():
    wfs = bcdata.wfs.BCWFS()
    count = wfs.pagesize + 100
    data = bcdata.get_data(WELLS_TABLE, count=count)
    assert len(data["features"]) == count


def test_get_data_sortby():
    data = bcdata.get_data(AIRPORTS_TABLE, count=1, sortby="AIRPORT_NAME")
    assert data["features"][0]["properties"]["AIRPORT_NAME"] == "100 Mile House Airport"


def test_cql_filter():
    data = bcdata.get_data(
        AIRPORTS_TABLE,
        query="AIRPORT_NAME='Terrace (Northwest Regional) Airport'",
    )
    assert len(data["features"]) == 1
    assert (
        data["features"][0]["properties"]["AIRPORT_NAME"]
        == "Terrace (Northwest Regional) Airport"
    )


def test_bounds_filter():
    data = bcdata.get_data(AIRPORTS_TABLE, bounds=[1188000, 377051, 1207437, 390361])
    assert len(data["features"]) == 8


def test_cql_bounds_filter():
    data = bcdata.get_data(
        AIRPORTS_TABLE,
        query="AIRPORT_NAME='Victoria International Airport'",
        bounds=[1167680.0, 367958.0, 1205720.0, 432374.0],
        bounds_crs="EPSG:3005",
    )
    assert len(data["features"]) == 1
    assert (
        data["features"][0]["properties"]["AIRPORT_NAME"]
        == "Victoria International Airport"
    )
