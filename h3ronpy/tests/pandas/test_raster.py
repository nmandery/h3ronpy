try:
    import rasterio

    HAS_RASTERIO = True
except ImportError:
    # rasterio is an optional dependency
    HAS_RASTERIO = False

import numpy as np
import pytest

from h3ronpy.pandas.raster import raster_to_dataframe
from h3ronpy import DEFAULT_CELL_COLUMN_NAME

from tests import TESTDATA_PATH


@pytest.mark.skipif(not HAS_RASTERIO, reason="requires rasterio")
def test_r_tiff():
    dataset = rasterio.open(TESTDATA_PATH / "r.tiff")
    band = dataset.read(1)
    df = raster_to_dataframe(band, dataset.transform, 8, nodata_value=0, compact=True, geo=False)
    assert len(df) > 100
    assert df.dtypes[DEFAULT_CELL_COLUMN_NAME] == "uint64"
    assert df.dtypes["value"] == "uint8"


@pytest.mark.skipif(not HAS_RASTERIO, reason="requires rasterio")
def test_r_tiff_float32():
    dataset = rasterio.open(TESTDATA_PATH / "r.tiff")
    band = dataset.read(1).astype(np.float32)
    df = raster_to_dataframe(band, dataset.transform, 8, nodata_value=0.0, compact=True, geo=False)
    assert len(df) > 100
    assert df.dtypes[DEFAULT_CELL_COLUMN_NAME] == "uint64"
    assert df.dtypes["value"] == "float32"


def test_preserve_nan_without_nodata_value():
    arr = np.array([[np.nan, 1.0], [np.nan, 1.0]], dtype=np.float32)
    df = raster_to_dataframe(arr, [11.0, 1.0, 0.0, 10.0, 1.2, 0.2], 7, nodata_value=None)
    assert df["value"].value_counts(dropna=False)[np.nan] > 100
    assert df["value"].value_counts(dropna=False)[1.0] > 100


def test_preserve_nan_with_nodata_value():
    arr = np.array([[np.nan, 1.0], [np.nan, 1.0]], dtype=np.float32)
    df = raster_to_dataframe(arr, [11.0, 1.0, 0.0, 10.0, 1.2, 0.2], 7, nodata_value=1.0)
    assert df["value"].value_counts(dropna=False)[np.nan] > 100
