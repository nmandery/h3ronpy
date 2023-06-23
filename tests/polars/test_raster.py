try:
    import rasterio

    HAS_RASTERIO = True
except ImportError:
    # rasterio is an optional dependency
    HAS_RASTERIO = False

import numpy as np
import pytest
import polars as pl

from h3ronpy.polars.raster import raster_to_dataframe
from h3ronpy import DEFAULT_CELL_COLUMN_NAME

from tests import TESTDATA_PATH


@pytest.mark.skipif(not HAS_RASTERIO, reason="requires rasterio")
def test_r_tiff():
    dataset = rasterio.open(TESTDATA_PATH / "r.tiff")
    band = dataset.read(1)
    df = raster_to_dataframe(band, dataset.transform, 8, nodata_value=0, compact=True)
    assert len(df) > 100
    assert df[DEFAULT_CELL_COLUMN_NAME].dtype == pl.UInt64
    assert df["value"].dtype == pl.UInt8


@pytest.mark.skipif(not HAS_RASTERIO, reason="requires rasterio")
def test_r_tiff_float32():
    dataset = rasterio.open(TESTDATA_PATH / "r.tiff")
    band = dataset.read(1).astype(np.float32)
    df = raster_to_dataframe(band, dataset.transform, 8, nodata_value=0.0, compact=True)
    assert len(df) > 100
    assert df[DEFAULT_CELL_COLUMN_NAME].dtype == pl.UInt64
    assert df["value"].dtype == pl.Float32
