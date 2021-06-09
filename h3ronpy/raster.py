"""
Conversion of raster numpy arrays to h3.

Resolution search modes
-----------------------

* "min_diff": chose the h3 resolution where the difference in the area of a pixel and the h3index is as small as possible.
* "smaller_than_pixel":  chose the h3 resolution where the area of the h3index is smaller than the area of a pixel.
"""

import geopandas as gp
import h3.api.numpy_int as h3
import numpy as np
from shapely.geometry import Polygon

import pandas as pd
from .h3ronpy import raster
from .util import _array_nditer
from . import H3_CRS

try:
    # affine library is used by rasterio
    import affine

    __HAS_AFFINE_LIB = True
except ImportError:
    __HAS_AFFINE_LIB = False


def _get_transform(t):
    if isinstance(t, raster.Transform):
        return t
    if __HAS_AFFINE_LIB:
        if isinstance(t, affine.Affine):
            return raster.Transform.from_rasterio([t.a, t.b, t.c, t.d, t.e, t.f])
    if type(t) in (list, tuple) and len(t) == 6:
        # probably native gdal
        return raster.Transform.from_gdal(t)
    raise ValueError("unsupported object for transform")


def nearest_h3_resolution(shape, transform, axis_order="yx", search_mode="min_diff"):
    """
    find the h3 resolution closed to the size of a pixel in an array
    of the given shape with the given transform

    :param shape: dimensions of the 2d array
    :param transform: the affine transformation
    :param axis_order: axis order of the 2d array. Either "xy" or "yx"
    :param search_mode: resolution search mode (see documentation of this module)
    :return:
    """
    return raster.nearest_h3_resolution(shape, _get_transform(transform), axis_order, search_mode)


def raster_to_dataframe(in_raster: np.array, transform, h3_resolution: int, nodata_value=None, axis_order: str = "yx",
                        compacted: bool = True, geo: bool = False):
    """
    convert a raster/array to a pandas dataframe containing H3 indexes

    This function is parallelized and uses the available CPUs by distributing tiles to a thread pool.

    The input geometry must be in WGS84.

    :param in_raster: input 2-d array
    :param transform:  the affine transformation
    :param nodata_value: the nodata value. For these cells of the array there will be no h3 indexes generated
    :param axis_order: axis order of the 2d array. Either "xy" or "yx"
    :param h3_resolution: target h3 resolution
    :param compacted: return compacted h3 indexes (see H3 docs)
    :param geo: return a geopandas geodataframe with geometries. increases the memory usage.
    :return: pandas dataframe or geodataframe
    """

    dtype = in_raster.dtype
    func = None
    if dtype == np.uint8:
        func = raster.raster_to_h3_u8
    elif dtype == np.int8:
        func = raster.raster_to_h3_i8
    elif dtype == np.uint16:
        func = raster.raster_to_h3_u16
    elif dtype == np.int16:
        func = raster.raster_to_h3_i16
    elif dtype == np.uint32:
        func = raster.raster_to_h3_u32
    elif dtype == np.int32:
        func = raster.raster_to_h3_i32
    elif dtype == np.uint64:
        func = raster.raster_to_h3_u64
    elif dtype == np.int64:
        func = raster.raster_to_h3_i64
    elif dtype == np.float32:
        func = raster.raster_to_h3_f32
    elif dtype == np.float64:
        func = raster.raster_to_h3_f64
    else:
        raise NotImplementedError(f"no raster_to_h3 implementation for dtype {dtype.name}")

    values, indexes = func(in_raster, _get_transform(transform), nodata_value, h3_resolution, axis_order, compacted)
    if geo:
        return gp.GeoDataFrame({
            "h3index": indexes,
            "value": values,
            "geometry": [Polygon(h3.h3_to_geo_boundary(h, geo_json=True)) for h in _array_nditer(indexes)],
        }, crs=H3_CRS)
    else:
        return pd.DataFrame({
            "h3index": indexes,
            "value": values
        })


def raster_to_geodataframe(*a, **kw):
    """
    convert to a geodataframe

    Uses the same parameters as array_to_dataframe
    """
    kw["geo"] = True
    return raster_to_dataframe(*a, **kw)
