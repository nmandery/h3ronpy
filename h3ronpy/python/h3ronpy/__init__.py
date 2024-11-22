from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Sequence, Union, cast

from arro3.core import Array, ChunkedArray, DataType, RecordBatch
from arro3.core.types import (
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
)

from . import h3ronpyrs as _native
from .h3ronpyrs import (  # noqa: F401
    DEFAULT_CELL_COLUMN_NAME,
    ContainmentMode,
    op,
    version,
)

if TYPE_CHECKING:
    import polars as pl


__version__ = version()

H3_CRS = "EPSG:4326"

if not _native.is_release_build():
    import warnings

    warnings.warn(
        "h3ronpy has not been compiled in release mode. Performance will be degraded.",
        RuntimeWarning,
    )


def _to_arrow_array(
    arr: Union[ArrowArrayExportable, ArrowStreamExportable, pl.Series, Sequence[Any]],
    dtype: Optional[ArrowSchemaExportable] = None,
) -> Array:
    if hasattr(arr, "__arrow_c_array__"):
        array = Array.from_arrow(cast(ArrowArrayExportable, arr))
    elif hasattr(arr, "__arrow_c_stream__"):
        ca = ChunkedArray.from_arrow(cast(ArrowStreamExportable, arr))
        array = ca.combine_chunks()
    elif hasattr(arr, "to_arrow"):
        ca = ChunkedArray.from_arrow(arr.to_arrow())  # type: ignore
        array = ca.combine_chunks()
    elif dtype is not None:
        # From arbitrary non-arrow input
        array = Array(cast(Sequence[Any], arr), type=dtype)
    else:
        raise ValueError("Unsupported input to _to_arrow_array. Expected array-like or series-like.")

    # Cast if dtype was provided
    if dtype is not None:
        array = array.cast(dtype)

    return array


def _to_uint64_array(arr) -> Array:
    return _to_arrow_array(arr, DataType.uint64())


def change_resolution(arr, resolution: int) -> Array:
    """
    Change the H3 resolutions of all contained values to `resolution`.

    In case of resolution increases all child indexes will be added, so the returned
    value may contain more indexes than the input.

    Invalid/empty values are omitted.
    """
    return op.change_resolution(_to_uint64_array(arr), resolution)


def change_resolution_list(arr, resolution: int) -> Array:
    """
    Change the H3 resolutions of all contained values to `resolution`.

    The output list array has the same length as the input array, positions of the elements
    in input and output are corresponding to each other.

    Invalid/empty values are preserved as such.
    """
    return op.change_resolution_list(_to_uint64_array(arr), resolution)


def change_resolution_paired(arr, resolution: int) -> RecordBatch:
    """
    Returns a table/dataframe with two columns: `cell_before` and `cell_after`
    with the cells h3index before and after the resolution change.

    This can be helpful when joining data in different resolutions via
    dataframe libraries
    """
    return op.change_resolution_paired(_to_uint64_array(arr), resolution)


def cells_resolution(arr) -> Array:
    """
    Generates a new array containing the resolution of each cell of the
    input array.

    :param arr:
    :return:
    """
    return op.cells_resolution(_to_uint64_array(arr))


def cells_parse(arr, set_failing_to_invalid: bool = False) -> Array:
    """
    Parse H3 cells from string arrays.

    Setting `set_failing_to_invalid` to true will trigger setting the validity bitmap according
    the successful parsing of an individual element. Having this set to false will cause the
    method to fail upon encountering the first unparsable value.

    This function is able to parse multiple representations of H3 cells:

        * hexadecimal (Example: ``8552dc63fffffff``)
        * numeric integer strings (Example: ``600436454824345599``)
        * strings like ``[x], [y], [resolution]`` or  ``[x]; [y]; [resolution]``. (Example: ``10.2,45.5,5``)
    """
    return op.cells_parse(
        _to_arrow_array(arr, DataType.utf8()),
        set_failing_to_invalid=set_failing_to_invalid,
    )


def vertexes_parse(arr, set_failing_to_invalid: bool = False) -> Array:
    """
    Parse H3 vertexes from string arrays.

    Setting `set_failing_to_invalid` to true will trigger setting the validity bitmap according
    the successful parsing of an individual element. Having this set to false will cause the
    method to fail upon encountering the first unparsable value.
    """
    return op.vertexes_parse(
        _to_arrow_array(arr, DataType.utf8()),
        set_failing_to_invalid=set_failing_to_invalid,
    )


def directededges_parse(arr, set_failing_to_invalid: bool = False) -> Array:
    """
    Parse H3 directed edges from string arrays.

    Setting `set_failing_to_invalid` to true will trigger setting the validity bitmap according
    the successful parsing of an individual element. Having this set to false will cause the
    method to fail upon encountering the first unparsable value.
    """
    return op.directededges_parse(
        _to_arrow_array(arr, DataType.utf8()),
        set_failing_to_invalid=set_failing_to_invalid,
    )


def compact(arr, mixed_resolutions: bool = False) -> Array:
    """
    Compact the given cells

    The cells are expected to be of the same resolution, otherwise this operation will fail unless
    `mixed_resolutions` is set to True. Setting this may lead to slight slow-downs.
    """
    return op.compact(_to_uint64_array(arr), mixed_resolutions=mixed_resolutions)


def uncompact(arr, target_resolution: int) -> Array:
    """
    Uncompact the given cells to the resolution `target_resolution`.

    All higher resolution cells contained in the input array than the given `target_resolution` will
    be omitted from the output.
    """
    return op.uncompact(_to_uint64_array(arr), target_resolution)


def _make_h3index_valid_wrapper(fn, h3index_name, wrapper_name):
    def valid_wrapper(arr, booleanarray: bool = False) -> Array:
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


def grid_disk(cellarray, k: int, flatten: bool = False) -> Array:
    return op.grid_disk(_to_uint64_array(cellarray), k, flatten=flatten)


def grid_disk_distances(cellarray, k: int, flatten: bool = False) -> RecordBatch:
    return op.grid_disk_distances(_to_uint64_array(cellarray), k, flatten=flatten)


def grid_disk_aggregate_k(cellarray, k: int, aggregation_method: str) -> RecordBatch:
    """
    Valid values for `aggregation_method` are `"min"` and `"max"`.
    """
    return op.grid_disk_aggregate_k(_to_uint64_array(cellarray), k, aggregation_method)


def grid_ring_distances(cellarray, k_min: int, k_max: int, flatten: bool = False) -> RecordBatch:
    return op.grid_ring_distances(_to_uint64_array(cellarray), k_min, k_max, flatten=flatten)


def cells_area_m2(cellarray) -> Array:
    return op.cells_area_m2(_to_uint64_array(cellarray))


def cells_area_km2(cellarray) -> Array:
    return op.cells_area_km2(_to_uint64_array(cellarray))


def cells_area_rads2(cellarray) -> Array:
    return op.cells_area_rads2(_to_uint64_array(cellarray))


def cells_to_string(cellarray) -> Array:
    return op.cells_to_string(_to_uint64_array(cellarray))


def vertexes_to_string(vertexesarray) -> Array:
    return op.vertexes_to_string(_to_uint64_array(vertexesarray))


def directededges_to_string(directededgearray) -> Array:
    return op.directededges_to_string(_to_uint64_array(directededgearray))


def cells_to_localij(cellarray, anchor, set_failing_to_invalid: bool = False) -> RecordBatch:
    """
    Produces IJ coordinates for an index anchored by an origin `anchor`.

    The coordinate space used by this function may have deleted regions or warping due to pentagonal distortion.

    Coordinates are only comparable if they come from the same origin index.

    The parameter `anchor` can be a single cell or an array of cells which serve as anchor for the
    cells of `cellarray`. In case it is an array, the length must match the length of the cell
    array.

    The default behavior is for this function to fail when a single transformation can not be completed
    successfully. When `set_failing_to_invalid` is set to True, only the failing positions
    of the output arrays will be set to null.
    """
    if type(anchor) is not int:
        anchor = _to_uint64_array(anchor)
    return op.cells_to_localij(
        _to_uint64_array(cellarray),
        anchor,
        set_failing_to_invalid=set_failing_to_invalid,
    )


def localij_to_cells(anchor, i, j, set_failing_to_invalid: bool = False) -> Array:
    """
    Produces cells from `i` and `j` coordinates and an `anchor` cell.

    The default behavior is for this function to fail when a single transformation can not be completed
    successfully. When `set_failing_to_invalid` is set to True, only the failing positions
    of the output arrays will be set to null.
    """
    if type(anchor) is not int:
        anchor = _to_uint64_array(anchor)
    return op.localij_to_cells(
        anchor,
        _to_arrow_array(i, DataType.int32()),
        _to_arrow_array(j, DataType.int32()),
        set_failing_to_invalid=set_failing_to_invalid,
    )


__all__ = [
    "H3_CRS",
    "DEFAULT_CELL_COLUMN_NAME",
    ContainmentMode.__name__,
    version.__name__,
    change_resolution.__name__,
    change_resolution_list.__name__,
    change_resolution_paired.__name__,
    cells_resolution.__name__,
    cells_parse.__name__,
    vertexes_parse.__name__,
    directededges_parse.__name__,
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
    cells_to_localij.__name__,
    localij_to_cells.__name__,
]
