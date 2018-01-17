import os
import unittest
import shutil

import bcdata
import fiona

EMAIL = os.environ["BCDATA_EMAIL"]


def test_info():
    info = bcdata.info('bc-airports')
    assert info == {'name': u'gsr_airports_svw',
                    'schema': u'whse_imagery_and_base_maps'}


def test_shapefile():
    out_wksp = bcdata.download('bc-airports',
                               EMAIL,
                               driver="ESRI Shapefile")
    # open and check downloaded data
    # the data is not static, just check that there are 400+ features
    with fiona.drivers():
        layers = fiona.listlayers(out_wksp)
        assert len(layers) == 1
        with fiona.open(out_wksp, layer=0) as src:
            assert src.driver == 'ESRI Shapefile'
            assert len(src) > 400
    shutil.rmtree(out_wksp)


def test_download_path():
    out_wksp = bcdata.download('bc-airports',
                               EMAIL,
                               driver="ESRI Shapefile",
                               download_path='dl_test')
    # open and check downloaded data
    # the data is not static, just check that there are 400+ features
    with fiona.drivers():
        layers = fiona.listlayers(out_wksp)
        assert len(layers) == 1
        with fiona.open(out_wksp, layer=0) as src:
            assert src.driver == 'ESRI Shapefile'
            assert len(src) > 400
    shutil.rmtree(out_wksp)


def test_gdb():
    out_wksp = bcdata.download('bc-airports',
                               EMAIL)
    with fiona.drivers():
        layers = fiona.listlayers(out_wksp)
        assert len(layers) == 1
        with fiona.open(out_wksp, layer=0) as src:
            assert src.driver == 'OpenFileGDB'
            assert len(src) > 400
    shutil.rmtree(out_wksp)


class URLTest(unittest.TestCase):
    def test_bad_url(self):
        self.assertRaises(ValueError, bcdata.download,
                          ('bad-url'), email_address=EMAIL)
