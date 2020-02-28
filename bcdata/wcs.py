import logging

import requests
import rasterio

import bcdata

log = logging.getLogger(__name__)


def get_dem(
    bounds,
    out_file="dem.tif",
    src_crs="EPSG:3005",
    dst_crs="EPSG:3005",
    resolution=25,
    interpolation=None,
    as_rasterio=False,
):
    """Get TRIM DEM for provided bounds, write to GeoTIFF.
    """
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
    r = requests.get(bcdata.WCS_URL, params=payload)

    # save to tiff
    if r.status_code == 200:
        with open(out_file, "wb") as file:
            file.write(r.content)
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
