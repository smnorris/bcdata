import os

import rasterio
import pytest

import bcdata


AIRPORTS_KEY = 'bc-airports'
UTMZONES_KEY = 'utm-zones-of-british-columbia'
BEC_KEY = 'biogeoclimatic-ecosystem-classification-bec-map'


def test_bcdc_package_show():
    package_info = bcdata.bcdc_package_show(AIRPORTS_KEY)
    assert package_info['object_name'] == 'WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW'


def test_get_count():
    table = bcdata.bcdc_package_show(UTMZONES_KEY)['object_name']
    assert bcdata.get_count(table) == 6


def test_get_count_filtered():
    assert bcdata.get_count(UTMZONES_KEY, query="UTM_ZONE=10") == 1


def test_get_data_small():
    data = bcdata.get_data(AIRPORTS_KEY)
    assert data['type'] == 'FeatureCollection'


def test_get_features():
    data = [f for f in bcdata.get_features(AIRPORTS_KEY)]
    assert len(data) == 455


def test_get_data_paged():
    data = bcdata.get_data(AIRPORTS_KEY, pagesize=250)
    assert len(data['features']) == 455


def test_cql_filter():
    data = bcdata.get_data(AIRPORTS_KEY, query="AIRPORT_NAME='Terrace (Northwest Regional) Airport'")
    assert len(data['features']) == 1
    assert data['features'][0]['properties']['AIRPORT_NAME'] == 'Terrace (Northwest Regional) Airport'


def test_bounds_filter():
    data = bcdata.get_data(AIRPORTS_KEY, bounds=[1188000, 377051, 1207437, 390361])
    assert len(data['features']) == 8


def test_cql_bounds_filter():
    data = bcdata.get_data(AIRPORTS_KEY, query="AIRPORT_NAME='Victoria International Airport'", bounds=[1167680.0, 367958.0, 1205720.0, 432374.0], bounds_crs="EPSG:3005")
    assert len(data['features']) == 1
    assert data['features'][0]['properties']['AIRPORT_NAME'] == "Victoria International Airport"


def test_dem(tmpdir):
    bounds = [1046891, 704778, 1055345, 709629]
    out_file = bcdata.get_dem(bounds, os.path.join(tmpdir, "test_dem.tif"))
    assert os.path.exists(out_file)
    with rasterio.open(out_file) as src:
        stats = [{'min': float(b.min()),
                  'max': float(b.max()),
                  'mean': float(b.mean())
                  } for b in src.read()]
    assert stats[0]['max'] == 3982


# interpolation takes a while to run, comment out for now
#def test_dem_resample(tmpdir):
#    bounds = [1046891, 704778, 1055345, 709629]
#    out_file = bcdata.get_dem(bounds, os.path.join(tmpdir, "test_dem.tif"), interpolation="bilinear", resolution=50)
#    assert os.path.exists(out_file)
#    with rasterio.open(out_file) as src:
#        stats = [{'min': float(b.min()),
#                  'max': float(b.max()),
#                  'mean': float(b.mean())
#                  } for b in src.read()]
#    assert stats[0]['max'] == 3956.0


def test_dem_invalid_resample1():
    with pytest.raises(ValueError):
        bounds = [1046891, 704778, 1055345, 709629]
        bcdata.get_dem(bounds, "test_dem.tif", interpolation="cubic", resolution=50)


def test_dem_invalid_resample2():
    with pytest.raises(ValueError):
        bounds = [1046891, 704778, 1055345, 709629]
        bcdata.get_dem(bounds, "test_dem.tif", interpolation="bilinear")
