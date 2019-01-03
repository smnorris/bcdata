from bcdata import package_show
from bcdata import get_data
from bcdata import get_count

AIRPORTS_KEY = 'bc-airports'
UTMZONES_KEY = 'utm-zones-of-british-columbia'
BEC_KEY = 'biogeoclimatic-ecosystem-classification-bec-map'


def test_package_show():
    package_info = package_show(AIRPORTS_KEY)
    assert package_info['object_name'] == 'WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW'


def test_get_count():
    table = package_show(UTMZONES_KEY)['object_name']
    assert get_count(table) == 6


def test_get_data_small():
    table = package_show(AIRPORTS_KEY)['object_name']
    data = get_data(table)
    assert data['type'] == 'FeatureCollection'


def test_get_one():
    table = package_show(UTMZONES_KEY)['object_name']
    data = get_data(table, count=1)
    assert len(data['features']) == 1

#def test_get_data_medium():
#    table = package_show(BEC_KEY)['object_name']
#    data = get_data(table)
#    assert data['type'] == 'FeatureCollection'


def test_cql_filter():
    table = package_show(AIRPORTS_KEY)['object_name']
    data = get_data(table, cql_filter="AIRPORT_NAME='Terrace (Northwest Regional) Airport'")
    assert len(data['features']) == 1
    assert data['features'][0]['properties']['AIRPORT_NAME'] == 'Terrace (Northwest Regional) Airport'


