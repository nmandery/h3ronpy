try:
    import rasterio

    HAS_RASTERIO = True
except ImportError:
    # rasterio is an optional dependency
    HAS_RASTERIO = False

import numpy as np
import pytest
import polars as pl

from h3ronpy.polars.raster import raster_to_dataframe, rasterize_cells
from h3ronpy import DEFAULT_CELL_COLUMN_NAME, H3_CRS

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
    df = raster_to_dataframe(band, dataset.transform, 8, nodata_value=np.NAN, compact=True)
    assert len(df) > 100
    assert df[DEFAULT_CELL_COLUMN_NAME].dtype == pl.UInt64
    assert df["value"].dtype == pl.Float32


def write_gtiff(filename, array, transform, nodata_value):
    with rasterio.open(
        filename,
        mode="w",
        driver="GTiff",
        compress="lzw",
        height=array.shape[0],
        width=array.shape[1],
        count=1,
        dtype=array.dtype,
        crs=H3_CRS,
        transform=transform,
        nodata_value=nodata_value,
    ) as ds:
        ds.write(array, 1)


@pytest.mark.skipif(not HAS_RASTERIO, reason="requires rasterio")
def test_rasterize_cells():
    df = pl.read_parquet(TESTDATA_PATH / "population-841fa8bffffffff.parquet")
    size = (1000, 1000)
    nodata_value = -1
    array, transform = rasterize_cells(df["h3index"], df["pop_general"].cast(pl.Int32), size, nodata_value=nodata_value)

    assert array.shape == size
    assert np.int32 == array.dtype.type
    assert np.any(array > 0)

    # for inspection during debugging
    if False:
        write_gtiff("/tmp/rasterized.tif", array, transform, nodata_value)


@pytest.mark.skipif(not HAS_RASTERIO, reason="requires rasterio")
def test_rasterize_cells_auto_aspect():
    df = pl.read_parquet(TESTDATA_PATH / "population-841fa8bffffffff.parquet")
    size = 1000
    nodata_value = -1
    array, transform = rasterize_cells(df["h3index"], df["pop_general"].cast(pl.Int32), size, nodata_value=nodata_value)

    assert array.shape[0] == size
    # print(array.shape)
    assert np.int32 == array.dtype.type
    assert np.any(array > 0)

    # for inspection during debugging
    if False:
        write_gtiff("/tmp/rasterized_auto_aspect.tif", array, transform, nodata_value)
