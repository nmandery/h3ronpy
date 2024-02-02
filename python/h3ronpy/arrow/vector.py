from h3ronpy.h3ronpyrs import vector
from .. import ContainmentMode
from . import _to_uint64_array, _HAS_POLARS, _to_arrow_array
from typing import Optional, Tuple, Union
import pyarrow as pa


def cells_to_coordinates(arr, radians: bool = False) -> pa.Table:
    """
    convert to point coordinates in degrees
    """
    return vector.cells_to_coordinates(_to_uint64_array(arr), radians=radians)


def coordinates_to_cells(latarray, lngarray, resarray, radians: bool = False) -> pa.Array:
    """
    Convert coordinates arrays to cells.

    :param latarray: array of lat values
    :param lngarray: array of lng values
    :param resarray: Either an array of resolutions or a single resolution as an integer to apply to all coordinates.
    :param radians: Set to True to pass `lat` and `lng` in radians
    :return: cell array
    """
    if type(resarray) in (int, float):
        res = int(resarray)
    else:
        res = _to_arrow_array(resarray, pa.uint8())
    return vector.coordinates_to_cells(
        _to_arrow_array(latarray, pa.float64()), _to_arrow_array(lngarray, pa.float64()), res, radians=radians
    )


def cells_bounds(arr) -> Optional[Tuple]:
    """
    Bounds of the complete array as a tuple `(minx, miny, maxx, maxy)`.
    """
    return vector.cells_bounds(_to_uint64_array(arr))


def cells_bounds_arrays(arr) -> pa.Table:
    """
    Build a table/dataframe with the columns `minx`, `miny`, `maxx` and `maxy` containing the bounds of the individual
    cells from the input array.
    """
    return vector.cells_bounds_arrays(_to_uint64_array(arr))


def cells_to_wkb_polygons(arr, radians: bool = False, link_cells: bool = False) -> pa.Array:
    """
    Convert cells to polygons.

    The returned geometries in the output array will match the order of the input array - unless ``link_cells``
    is set to True.

    :param: arr: The cell array
    :param radians: Generate geometries using radians instead of degrees
    :param link_cells: Combine neighboring cells into a single polygon geometry.
    """
    return vector.cells_to_wkb_polygons(_to_uint64_array(arr), radians=radians, link_cells=link_cells)


def cells_to_wkb_points(arr, radians: bool = False) -> pa.Array:
    """
    Convert cells to points using their centroids.

    The returned geometries in the output array will match the order of the input array.

    :param: arr: The cell array
    :param radians: Generate geometries using radians instead of degrees
    """
    return vector.cells_to_wkb_points(_to_uint64_array(arr), radians=radians)


def vertexes_to_wkb_points(arr, radians: bool = False) -> pa.Array:
    """
    Convert vertexes to points.

    The returned geometries in the output array will match the order of the input array.

    :param: arr: The vertex array
    :param radians: Generate geometries using radians instead of degrees
    """
    return vector.vertexes_to_wkb_points(_to_uint64_array(arr), radians=radians)


def directededges_to_wkb_linestrings(arr, radians: bool = False) -> pa.Array:
    """
    Convert directed edges to linestrings.

    The returned geometries in the output array will match the order of the input array.

    :param: arr: The directed edge array
    :param radians: Generate geometries using radians instead of degrees
    """
    return vector.directededges_to_wkb_linestrings(_to_uint64_array(arr), radians=radians)


def wkb_to_cells(
    arr,
    resolution: int,
    containment_mode: ContainmentMode = ContainmentMode.ContainsCentroid,
    compact: bool = False,
    flatten: bool = False,
) -> Union[pa.Array, pa.ListArray]:
    """
    Convert a Series/Array/List of WKB values to H3 cells.

    Unless ``flatten`` is set to True a list array will be returned, with the cells generated from a geometry being
    located at the same position as the geometry in the input array.

    :param arr: The input array.
    :param resolution: H3 resolution
    :param containment_mode: Containment mode used to decide if a cell is contained in a polygon or not.
            See the ContainmentMode class.
    :param compact: Compact the returned cells by replacing cells with their parent cells when all children
            of that cell are part of the set.
    :param flatten: Return a non-nested cell array instead of a list array.
    """
    if _HAS_POLARS:
        import polars as pl

        if isinstance(arr, pl.Series):
            arr = arr.to_arrow()

    if not isinstance(arr, pa.LargeBinaryArray):
        arr = pa.array(arr, type=pa.large_binary())

    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()
    return vector.wkb_to_cells(
        arr,
        resolution,
        containment_mode=containment_mode,
        compact=compact,
        flatten=flatten,
    )


def geometry_to_cells(
    geom,
    resolution: int,
    containment_mode: ContainmentMode = ContainmentMode.ContainsCentroid,
    compact: bool = False,
) -> pa.Array:
    """
    Convert a single object which supports the python `__geo_interface__` protocol to H3 cells

    :param geom: geometry
    :param resolution: H3 resolution
    :param containment_mode: Containment mode used to decide if a cell is contained in a polygon or not.
            See the ContainmentMode class.
    :param compact: Compact the returned cells by replacing cells with their parent cells when all children
            of that cell are part of the set.
    """
    return vector.geometry_to_cells(geom, resolution, containment_mode=containment_mode, compact=compact)


__all__ = [
    cells_to_coordinates.__name__,
    coordinates_to_cells.__name__,
    cells_bounds.__name__,
    cells_bounds_arrays.__name__,
    cells_to_wkb_polygons.__name__,
    cells_to_wkb_points.__name__,
    vertexes_to_wkb_points.__name__,
    directededges_to_wkb_linestrings.__name__,
    wkb_to_cells.__name__,
    geometry_to_cells.__name__,
]
