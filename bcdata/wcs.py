import logging
from math import trunc

import requests
import rasterio

import bcdata

log = logging.getLogger(__name__)

WCS_URL = "https://openmaps.gov.bc.ca/om/wcs"


def align_bounds(bounds):
    """
    Adjust input bounds to align with Hectares BC raster
    (round bounds to nearest 100m, then shift by 12.5m)
    """
    ll = [((trunc(b / 100) * 100) - 12.5) for b in bounds[:2]]
    ur = [(((trunc(b / 100) + 1) * 100) + 87.5) for b in bounds[2:]]
    return (ll[0], ll[1], ur[0], ur[1])


def get_dem(
    bounds,
    out_file="dem.tif",
    src_crs="EPSG:3005",
    dst_crs="EPSG:3005",
    resolution=25,
    align=False,
    interpolation=None,
    as_rasterio=False,
):
    """Get TRIM DEM for provided bounds, write to GeoTIFF."""
    # align bounds if specified (and bounds are BC Albers CRS)
    if align:
        if src_crs.upper() == "EPSG:3005" and dst_crs.upper() == "EPSG:3005":
            bounds = align_bounds(bounds)
        else:
            raise ValueError(
                f"Target CRS is {dst_crs}, align is only valid for BC Albers based bounds and outputs"
            )

    bbox = ",".join([str(b) for b in bounds])

    # do not upsample
    if resolution < 25:
        raise ValueError("Resolution requested must be 25m or greater")

    # if downsampling, default to bilinear (the server defaults to nearest)
    if resolution > 25 and not interpolation:
        log.info("Interpolation not specified, defaulting to bilinear")
        interpolation = "bilinear"

    # if specifying interpolation method, there has to actually be a
    # resampling requested - resolution can't be the native 25m
    if interpolation and resolution == 25:
        raise ValueError(
            "Requested coverage at native resolution, no resampling required, interpolation {} invalid"
        )

    # make sure interpolation is valid
    if interpolation:
        valid_interpolations = ["nearest", "bilinear", "bicubic"]
        if interpolation not in valid_interpolations:
            raise ValueError(
                "Interpolation {} invalid. Valid keys are: {}".format(
                    interpolation, ",".join(valid_interpolations)
                )
            )

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

    if interpolation:
        payload["INTERPOLATION"] = interpolation

    # request data from WCS
    log.debug(payload)
    r = requests.get(WCS_URL, params=payload)
    log.debug(r.headers)
    if r.status_code == 200:
        if r.headers["Content-Type"] == "image/tiff":
            with open(out_file, "wb") as file:
                file.write(r.content)
        elif r.headers["Content-Type"] == "application/vnd.ogc.se_xml;charset=UTF-8":
            raise RuntimeError(
                "WCS request {} failed with error {}".format(
                    r.url, str(r.content.decode("utf-8"))
                )
            )
        else:
            raise RuntimeError(
                "WCS request {} failed, content type {}".format(
                    r.url, str(r.headers["Content-Type"])
                )
            )
    else:
        raise RuntimeError(
            "WCS request {} failed with status code {}".format(
                r.url, str(r.status_code)
            )
        )

    if as_rasterio:
        return rasterio.open(out_file, "r")
    else:
        return out_file
