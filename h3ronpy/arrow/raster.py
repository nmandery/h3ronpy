"""
Conversion of 2D `numpy` arrays to H3 cells.

The geo-context is passed to this library using a coordinate transformation matrix - this can be either
a `GDAL-like array <https://gdal.org/tutorials/geotransforms_tut.html>`_ of six float values, or a
`Affine <https://pypi.org/project/affine/>`_-object as used by `rasterio`.

.. note::

    As H3 itself used WGS84 (EPSG:4326) Lat/Lon coordinates, the coordinate transformation matrix used in this module
    must be based on WGS84 as well. Raster data using other coordinate systems need to be reprojected accordingly.


While H3 cells are hexagons and pentagons, this raster conversion process only takes the raster value under the centroid
of the cell into account. When the data shall be aggregated, use any of these methods:

1. Make use the `nearest_h3_resolution` function to convert to the H3 resolution nearest to the pixel size of the raster.
   After that the cell resolution can be changed using the `change_resolution` function and dataframe libraries can be used to
   perform the desired aggregations. This can be a rather memory-intensive process.

2. Scale the raster down using an interpolation algorithm. After that use method 1. This can save a lot of memory, but may
   not be applicable to all datasets - for example dataset with absolute values per pixel like population counts.

Resolution search modes of `nearest_h3_resolution`:

* "min_diff": chose the H3 resolution where the difference in the area of a pixel and the h3index is as small as possible.
* "smaller_than_pixel":  chose the H3 resolution where the area of the h3index is smaller than the area of a pixel.

"""

from ..h3ronpy import raster
from .. import DEFAULT_CELL_COLUMN_NAME
import numpy as np
import pyarrow as pa

try:
    # affine library is used by rasterio
    import affine

    __HAS_AFFINE_LIB = True
except ImportError:
    __HAS_AFFINE_LIB = False

Transform = raster.Transform


def _get_transform(t):
    if isinstance(t, Transform):
        return t
    if __HAS_AFFINE_LIB:
        if isinstance(t, affine.Affine):
            return Transform.from_rasterio([t.a, t.b, t.c, t.d, t.e, t.f])
    if type(t) in (list, tuple) and len(t) == 6:
        # probably native gdal
        return Transform.from_gdal(t)
    raise ValueError("unsupported object for transform")


def nearest_h3_resolution(shape, transform, axis_order="yx", search_mode="min_diff") -> int:
    """
    Find the H3 resolution closest to the size of a pixel in an array
    of the given shape with the given transform

    :param shape: dimensions of the 2d array
    :param transform: the affine transformation
    :param axis_order: axis order of the 2d array. Either "xy" or "yx"
    :param search_mode: resolution search mode (see documentation of this module)
    :return:
    """
    return raster.nearest_h3_resolution(shape, _get_transform(transform), axis_order, search_mode)


def raster_to_dataframe(
    in_raster: np.array,
    transform,
    h3_resolution: int,
    nodata_value=None,
    axis_order: str = "yx",
    compact: bool = True,
) -> pa.Table:
    """
    Convert a raster/array to a pandas `DataFrame` containing H3 cell indexes

    This function is parallelized and uses the available CPUs by distributing tiles to a thread pool.

    The input geometry must be in WGS84.

    :param in_raster: Input 2D array
    :param transform:  The affine transformation
    :param nodata_value: The nodata value. For these cells of the array there will be no h3 indexes generated
    :param axis_order: Axis order of the 2d array. Either "xy" or "yx"
    :param h3_resolution: Target h3 resolution
    :param compact: Return compacted h3 indexes (see H3 docs). This results in mixed H3 resolutions, but also can
            reduce the amount of required memory.
    :return: Tuple of arrow arrays
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

    return pa.Table.from_arrays(
        arrays=func(in_raster, _get_transform(transform), h3_resolution, axis_order, compact, nodata_value),
        names=["value", DEFAULT_CELL_COLUMN_NAME],
    )
