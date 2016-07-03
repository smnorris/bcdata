import os
import unittest

import bcdata
import fiona

GEOMARK = "gm-3D54AEE61F1847BA881E8BF7DE23BA21"
EMAIL = os.environ["BCDATA_EMAIL"]
NAD83 = {'init': 'epsg:4269'}


def test_simple_success():
    order_id = bcdata.create_order('bc-airports',
                                   EMAIL,
                                   file_format="ESRI Shapefile",
                                   crs="EPSG:4269",
                                   geomark=GEOMARK)
    out_folder = bcdata.download_order(order_id)
    # open and check downloaded data
    with fiona.drivers():
        layers = fiona.listlayers(out_folder)
        assert len(layers) == 1
        with fiona.open(out_folder, layer=0) as src:
            assert src.driver == 'ESRI Shapefile'
            assert src.crs == NAD83
            assert len(src) == 13


def test_empty_download():
    order_id = bcdata.create_order('pscis-design-proposal',
                                   EMAIL,
                                   file_format="ESRI Shapefile",
                                   geomark='gm-C8E70532E717470CA1EC06EE1F2C67B7')
    out_folder = bcdata.download_order(order_id)
    assert out_folder is None


class URLTest(unittest.TestCase):
    def test_bad_url(self):
        self.assertRaises(ValueError, bcdata.create_order,
                          ('bad-url'), email_address=EMAIL)

    def test_bad_orderid(self):
        self.assertRaises(RuntimeError,
                          bcdata.download_order,
                          ('9999'), timeout=10)
