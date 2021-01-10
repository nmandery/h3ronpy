"""
Conversion of numpy arrays to h3.

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
    HAS_AFFINE_LIB = True
except:
    HAS_AFFINE_LIB = False

def _get_transform(t):
    if isinstance(t, Transform):
        return t
    if HAS_AFFINE_LIB:
        if isinstance(t, affine.Affine):
            return Transform.from_rasterio([t.a, t.b, t.c, t.d, t.e, t.f])
    # TODO: native gdal
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


def array_to_h3(in_array, transform, h3_resolution, nodata_value=None, axis_order="yx"):
    """
    convert a raster/array to a pandas dataframe containing H3 indexes

    :param in_array: input 2-d array
    :param transform:  the affine transformation
    :param h3_resolution: target h3 resolution
    :param nodata_value: the nodata value. For these cells of the array there will be no h3 indexes generated
    :param axis_order: axis order of the 2d array. Either "xy" or "yx"
    :return: dict with the former array values as keys, mapping to compactedvec instances containing the h3 indexes.
    """
    dtype = in_array.dtype
    func = None
    if dtype == np.uint8:
        func = lib.array_to_h3_u8
    elif dtype == np.int8:
        func = lib.array_to_h3_i8
    elif dtype == np.uint16:
        func = lib.array_to_h3_u16
    elif dtype == np.int16:
        func = lib.array_to_h3_i16
    elif dtype == np.uint32:
        func = lib.array_to_h3_u32
    elif dtype == np.int32:
        func = lib.array_to_h3_i32
    elif dtype == np.uint64:
        func = lib.array_to_h3_u64
    elif dtype == np.int64:
        func = lib.array_to_h3_i64
    else:
        raise NotImplementedError(f"no array_to_h3 implementation for dtype {dtype.name}")

    #print(func.__name__)
    return func(in_array, _get_transform(transform), nodata_value, h3_resolution, axis_order)


def array_to_dataframe(in_array, transform, h3_resolution, nodata_value=None, axis_order="yx", compacted=True, geo=False):
    """
    convert a raster/array to a pandas dataframe containing H3 indexes

    :param in_array: input 2-d array
    :param transform:  the affine transformation
    :param nodata_value: the nodata value. For these cells of the array there will be no h3 indexes generated
    :param axis_order: axis order of the 2d array. Either "xy" or "yx"
    :param h3_resolution: target h3 resolution
    :param compacted: return compacted h3 indexes (see H3 docs)
    :param geo: return a geopandas geodataframe with geometries. increases the memory usage.
    :return: pandas dataframe or geodataframe
    """
    frames = []
    for value, compacted_vec in array_to_h3(in_array, transform, h3_resolution, nodata_value=nodata_value, axis_order=axis_order).items():
        indexes =  None
        if compacted:
            indexes = compacted_vec.compacted_indexes()
        else:
            indexes = compacted_vec.uncompacted_indexes_at_resolution(h3_resolution)
        values = np.repeat(value, len(indexes))
        frame = None
        if geo:
            frame = gp.GeoDataFrame({
                "h3index": indexes,
                "value": values,
                "geometry": [Polygon(h3.h3_to_geo_boundary(h, geo_json=True)) for h in np.nditer(indexes)],
            })
        else:
            frame = pd.DataFrame({
                "h3index": indexes,
                "value": values
            })
        frames.append(frame)
    return pd.concat(frames)

def array_to_geodataframe(*a, **kw):
    """
    convert to as geodataframe

    uses the same parameters as array_to_dataframe
    """
    kw["geo"] = True
    df = array_to_dataframe(*a, **kw)
