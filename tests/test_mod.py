import os

import bcdata
import fiona

GEOMARK = "gm-3D54AEE61F1847BA881E8BF7DE23BA21"
DATASET = 'bc-airports'
EMAIL = os.environ["BCDATA_EMAIL"]


def test_all_options():
    order_id = bcdata.create_order(DATASET, EMAIL,
                                   file_format="ESRI Shapefile",
                                   crs="EPSG:4326",
                                   geomark=GEOMARK)
    out_folder = bcdata.download_order(order_id)
    # open and check downloaded data
    with fiona.drivers():
        layers = fiona.listlayers(out_folder)
        assert len(layers) == 1
        with fiona.open(out_folder, layer=0) as src:
            assert len(src) == 13
