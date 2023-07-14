import os

import rasterio
import pytest
from rasterio.coords import BoundingBox

import bcdata


def test_dem(tmpdir):
    bounds = [1046891, 704778, 1055345, 709629]
    out_file = bcdata.get_dem(bounds, os.path.join(tmpdir, "test_dem.tif"))
    assert os.path.exists(out_file)
    with rasterio.open(out_file) as src:
        stats = [
            {
                "min": float(b.min()),
                "max": float(b.max()),
                "mean": float(b.mean()),
            }
            for b in src.read()
        ]
    assert stats[0]["max"] == 3982


def test_dem_align(tmpdir):
    bounds = [1046891, 704778, 1055345, 709629]
    out_file = bcdata.get_dem(
        bounds, os.path.join(tmpdir, "test_dem_align.tif"), align=True
    )
    assert os.path.exists(out_file)
    with rasterio.open(out_file) as src:
        bounds = src.bounds
    bbox = BoundingBox(1046787.5, 704687.5, 1055487.5, 709787.5)
    assert bounds == bbox


def test_dem_rasterio(tmpdir):
    bounds = [1046891, 704778, 1055345, 709629]
    src = bcdata.get_dem(bounds, as_rasterio=True)
    stats = [
        {"min": float(b.min()), "max": float(b.max()), "mean": float(b.mean())}
        for b in src.read()
    ]
    assert stats[0]["max"] == 3982


# interpolation takes a while to run, comment out for for faster tests
# def test_dem_resample(tmpdir):
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
