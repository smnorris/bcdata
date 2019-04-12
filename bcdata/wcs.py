import logging

import requests

import bcdata

log = logging.getLogger(__name__)


def get_dem(bounds, out_file="dem.tif", src_crs="EPSG:3005", dst_crs="EPSG:3005", resolution=25):
    """Get 25m DEM for provided bounds, write to GeoTIFF
    """
    bbox = ",".join([str(b) for b in bounds])
    # todo: validate resolution units are equivalent to src_crs units
    # build request
    payload = {
        "service": "WCS",
        "version": "1.0.0",
        "request": "GetCoverage",
        "coverage": "pub:bc_elevation_25m_bcalb",
        "Format": "GeoTIFF",
        "bbox": bbox,
        "CRS": src_crs,
        "RESPONSE_CRS": dst_crs,
        "resx": str(resolution),
        "resy": str(resolution),
    }
    # request data from WCS
    r = requests.get(bcdata.WCS_URL, params=payload)
    # save to tiff
    if r.status_code == 200:
        with open(out_file, "wb") as file:
            file.write(r.content)
        return out_file
    else:
        raise RuntimeError(
            "WCS request failed with status code {}".format(str(r.status_code))
        )
