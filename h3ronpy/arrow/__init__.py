from typing import Union

import pyarrow as pa
from ..h3ronpy import op

try:
    import polars as pl

    _HAS_POLARS = True
except ImportError:
    _HAS_POLARS = False


def _to_arrow_array(arr, dtype) -> pa.Array:
    converted = None
    if _HAS_POLARS:
        if isinstance(arr, pl.Series):
            converted = arr.to_arrow()

    if converted is None:
        converted = pa.array(arr, type=dtype)

    if isinstance(arr, pa.ChunkedArray):
        converted = converted.combine_chunks()
    return converted


def _to_uint64_array(arr) -> pa.Array:
    return _to_arrow_array(arr, pa.uint64())


def change_resolution(arr, resolution: int) -> pa.Array:
    return op.change_resolution(_to_uint64_array(arr), resolution)


def change_resolution_paired(arr, resolution: int) -> pa.Table:
    """
    Returns a table/dataframe with two columns: `cell_before` and `cell_after`
    with the cells h3index before and after the resolution change.

    This can be helpful when joining data in different resolutions via
    dataframe libraries
    """
    return op.change_resolution_paired(_to_uint64_array(arr), resolution)


def cells_resolution(arr) -> pa.Array:
    """
    Generates a new array containing the resolution of each cell of the
    input array.

    :param arr:
    :return:
    """
    return op.cells_resolution(_to_uint64_array(arr))


def cells_parse(arr, set_failing_to_invalid: bool = False) -> pa.Array:
    """
    Parse H3 cells from string arrays

    Setting `set_failing_to_invalid` to true will trigger setting the validity bitmap according
    the successful parsing of an individual element. Having this set to false will cause the
    method to fail upon encountering the first unparsable value.
    """
    return op.cells_parse(_to_arrow_array(arr, pa.utf8()), set_failing_to_invalid=set_failing_to_invalid)


def compact(arr, mixed_resolutions: bool = False) -> pa.Array:
    """
    Compact the given cells

    The cells are expected to be of the same resolution, otherwise this operation will fail unless
    `mixed_resolutions` is set to True. Setting this may lead to slight slow-downs.
    """
    return op.compact(_to_uint64_array(arr), mixed_resolutions=mixed_resolutions)


def uncompact(arr, target_resolution: int) -> pa.Array:
    """
    Uncompact the given cells to the resolution `target_resolution`.

    All higher resolution cells contained in the input array than the given `target_resolution` will
    be omitted from the output.
    """
    return op.uncompact(_to_uint64_array(arr), target_resolution)


def _make_h3index_valid_wrapper(fn, h3index_name, wrapper_name):
    def valid_wrapper(arr, booleanarray: bool = False) -> pa.Array:
        return fn(_to_uint64_array(arr), booleanarray=booleanarray)

    valid_wrapper.__doc__ = f"""
    Validate an array of potentially invalid {h3index_name} values by returning a new
    UInt64 array with the validity mask set accordingly.
    
    If `booleanarray` is set to True, a boolean array describing the validity will be
    returned instead.
    """
    valid_wrapper.__name__ = wrapper_name
    return valid_wrapper


cells_valid = _make_h3index_valid_wrapper(op.cells_valid, "cell", "cells_valid")
vertexes_valid = _make_h3index_valid_wrapper(op.cells_valid, "vertex", "vertexes_valid")
directededges_valid = _make_h3index_valid_wrapper(op.cells_valid, "directed edge", "directededges_valid")


def grid_disk(cellarray, k: int, flatten: bool = False) -> Union[pa.ListArray, pa.Array]:
    return op.grid_disk(_to_uint64_array(cellarray), k, flatten=flatten)


def grid_disk_distances(cellarray, k: int, flatten: bool = False) -> pa.Table:
    return op.grid_disk_distances(_to_uint64_array(cellarray), k, flatten=flatten)


def grid_disk_aggregate_k(cellarray, k: int, aggregation_method: str) -> pa.Table:
    """
    Valid values for `aggregation_method` are `"min"` and `"max"`.
    """
    return op.grid_disk_aggregate_k(_to_uint64_array(cellarray), k, aggregation_method)


def grid_ring_distances(cellarray, k_min: int, k_max: int, flatten: bool = False) -> pa.Table:
    return op.grid_ring_distances(_to_uint64_array(cellarray), k_min, k_max, flatten=flatten)


def cells_area_m2(cellarray) -> pa.Array:
    return op.cells_area_m2(_to_uint64_array(cellarray))


def cells_area_km2(cellarray) -> pa.Array:
    return op.cells_area_km2(_to_uint64_array(cellarray))


def cells_area_rads2(cellarray) -> pa.Array:
    return op.cells_area_rads2(_to_uint64_array(cellarray))


def cells_to_string(cellarray) -> pa.Array:
    return op.cells_to_string(_to_uint64_array(cellarray))


def vertexes_to_string(vertexesarray) -> pa.Array:
    return op.vertexes_to_string(_to_uint64_array(vertexesarray))


def directededges_to_string(directededgearray) -> pa.Array:
    return op.directededges_to_string(_to_uint64_array(directededgearray))


__all__ = [
    change_resolution.__name__,
    change_resolution_paired.__name__,
    cells_resolution.__name__,
    cells_parse.__name__,
    compact.__name__,
    uncompact.__name__,
    cells_valid.__name__,
    vertexes_valid.__name__,
    directededges_valid.__name__,
    grid_disk.__name__,
    grid_disk_distances.__name__,
    grid_ring_distances.__name__,
    grid_disk_aggregate_k.__name__,
    cells_area_m2.__name__,
    cells_area_km2.__name__,
    cells_area_rads2.__name__,
    cells_to_string.__name__,
    vertexes_to_string.__name__,
    directededges_to_string.__name__,
]
