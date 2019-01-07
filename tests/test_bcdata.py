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


def test_bbox_filter():
    data = bcdata.get_data(AIRPORTS_KEY, bbox="1188000,377051,1207437,390361")
    assert len(data['features']) == 8
