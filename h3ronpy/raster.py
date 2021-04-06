"""
Conversion of raster numpy arrays to h3.

Resolution search modes
-----------------------

* "min_diff": chose the h3 resolution where the difference in the area of a pixel and the h3index is as small as possible.
* "smaller_than_pixel":  chose the h3 resolution where the area of the h3index is smaller than the area of a pixel.
"""

from . import h3ronpy as lib
from .h3ronpy import Transform

import numpy as np
import h3.api.numpy_int as h3
import pandas as pd
import geopandas as gp
from shapely.geometry import Polygon

try:
    # affine library is used by rasterio
    import affine
    __HAS_AFFINE_LIB = True
except:
    __HAS_AFFINE_LIB = False

def _get_transform(t):
    if isinstance(t, Transform):
        return t
    if __HAS_AFFINE_LIB:
        if isinstance(t, affine.Affine):
            return Transform.from_rasterio([t.a, t.b, t.c, t.d, t.e, t.f])
    if type(t) in (list, tuple) and len(t) == 6:
        # proptably native gdal
        return Transform.from_gdal(t)
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
    return lib.nearest_h3_resolution(shape, _get_transform(transform), axis_order, search_mode)


def raster_to_dataframe(in_raster: np.array, transform, h3_resolution: int, nodata_value=None, axis_order:str= "yx", compacted:bool=True, geo:bool=False):
    """
    convert a raster/array to a pandas dataframe containing H3 indexes

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
        func = lib.raster_to_h3_u8
    elif dtype == np.int8:
        func = lib.raster_to_h3_i8
    elif dtype == np.uint16:
        func = lib.raster_to_h3_u16
    elif dtype == np.int16:
        func = lib.raster_to_h3_i16
    elif dtype == np.uint32:
        func = lib.raster_to_h3_u32
    elif dtype == np.int32:
        func = lib.raster_to_h3_i32
    elif dtype == np.uint64:
        func = lib.raster_to_h3_u64
    elif dtype == np.int64:
        func = lib.raster_to_h3_i64
    else:
        raise NotImplementedError(f"no raster_to_h3 implementation for dtype {dtype.name}")

    #print(func.__name__)
    values, indexes = func(in_raster, _get_transform(transform), nodata_value, h3_resolution, axis_order, compacted)
    if geo:
        return gp.GeoDataFrame({
            "h3index": indexes,
            "value": values,
            "geometry": [Polygon(h3.h3_to_geo_boundary(h, geo_json=True)) for h in np.nditer(indexes)],
        })
    else:
        return pd.DataFrame({
            "h3index": indexes,
            "value": values
        })

def array_to_geodataframe(*a, **kw):
    """
    convert to as geodataframe

    uses the same parameters as array_to_dataframe
    """
    kw["geo"] = True
    return raster_to_dataframe(*a, **kw)
