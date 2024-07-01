"""
API to use `h3ronpy` with the `polars dataframe library <https://www.pola.rs/>`_.

.. warning::

    To avoid pulling in unused dependencies, `h3ronpy` does not declare a dependency to `polars`. This
    package needs to be installed separately.

"""

from functools import wraps
import typing
import polars as pl
import pyarrow as pa
from .. import arrow as _arrow


def _wrap(func, ret_type=None):
    @wraps(func, updated=())
    def wrapper(*args, **kw):
        result = func(*args, **kw)
        if isinstance(result, pa.Table) or isinstance(result, pa.Array):
            return pl.from_arrow(result)
        return result

    if ret_type:
        # create a copy to avoid modifying the dict of the wrapped function
        wrapper.__annotations__ = dict(**wrapper.__annotations__)
        wrapper.__annotations__["return"] = ret_type
    return wrapper


change_resolution = _wrap(_arrow.change_resolution, ret_type=pl.Series)
change_resolution_list = _wrap(_arrow.change_resolution, ret_type=pl.Series)
change_resolution_paired = _wrap(_arrow.change_resolution_paired, ret_type=pl.DataFrame)
cells_resolution = _wrap(_arrow.cells_resolution, ret_type=pl.Series)
cells_parse = _wrap(_arrow.cells_parse, ret_type=pl.Series)
vertexes_parse = _wrap(_arrow.vertexes_parse, ret_type=pl.Series)
directededges_parse = _wrap(_arrow.directededges_parse, ret_type=pl.Series)
compact = _wrap(_arrow.compact, ret_type=pl.Series)
uncompact = _wrap(_arrow.uncompact, ret_type=pl.Series)
cells_valid = _wrap(_arrow.cells_valid, ret_type=pl.Series)
vertexes_valid = _wrap(_arrow.vertexes_valid, ret_type=pl.Series)
directededges_valid = _wrap(_arrow.directededges_valid, ret_type=pl.Series)
grid_disk = _wrap(_arrow.grid_disk, ret_type=pl.Series)
grid_disk_distances = _wrap(_arrow.grid_disk_distances, ret_type=pl.DataFrame)
grid_ring_distances = _wrap(_arrow.grid_ring_distances, ret_type=pl.DataFrame)
grid_disk_aggregate_k = _wrap(_arrow.grid_disk_aggregate_k, ret_type=pl.DataFrame)
cells_area_m2 = _wrap(_arrow.cells_area_m2, ret_type=pl.Series)
cells_area_km2 = _wrap(_arrow.cells_area_km2, ret_type=pl.Series)
cells_area_rads2 = _wrap(_arrow.cells_area_rads2, ret_type=pl.Series)
cells_to_string = _wrap(_arrow.cells_to_string, ret_type=pl.Series)
vertexes_to_string = _wrap(_arrow.vertexes_to_string, ret_type=pl.Series)
directededges_to_string = _wrap(_arrow.directededges_to_string, ret_type=pl.Series)
cells_to_localij = _wrap(_arrow.cells_to_localij, ret_type=pl.DataFrame)
localij_to_cells = _wrap(_arrow.localij_to_cells, ret_type=pl.Series)


@pl.api.register_expr_namespace("h3")
class H3Expr:
    """
    Registers H3 functionality with polars Expr expressions.

    The methods of this class mirror the functionality provided by the functions of this
    module. Please refer to the module functions for more documentation.
    """

    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def __expr_map_series(self, func: typing.Callable[[pl.Series], pl.Series]) -> pl.Expr:
        if hasattr(self._expr, "map"):
            # polars < 1.0
            return self._expr.map(func)
        return self._expr.map_batches(func)

    def cells_resolution(self) -> pl.Expr:
        return self.__expr_map_series(lambda s: cells_resolution(s)).alias("resolution")

    def change_resolution(self, resolution: int) -> pl.Expr:
        return self.__expr_map_series(lambda s: change_resolution(s, resolution))

    def change_resolution_list(self, resolution: int) -> pl.Expr:
        return self.__expr_map_series(lambda s: change_resolution_list(s, resolution))

    def cells_parse(self, set_failing_to_invalid: bool = False) -> pl.Expr:
        return self.__expr_map_series(lambda s: cells_parse(s, set_failing_to_invalid=set_failing_to_invalid)).alias(
            "cell"
        )

    def vertexes_parse(self, set_failing_to_invalid: bool = False) -> pl.Expr:
        return self.__expr_map_series(lambda s: vertexes_parse(s, set_failing_to_invalid=set_failing_to_invalid)).alias(
            "vertex"
        )

    def directededges_parse(self, set_failing_to_invalid: bool = False) -> pl.Expr:
        return self.__expr_map_series(
            lambda s: directededges_parse(s, set_failing_to_invalid=set_failing_to_invalid)
        ).alias("directededge")

    def grid_disk(self, k: int, flatten: bool = False) -> pl.Expr:
        return self.__expr_map_series(lambda s: grid_disk(s, k, flatten=flatten))

    def compact(self, mixed_resolutions: bool = False) -> pl.Expr:
        return self.__expr_map_series(lambda s: compact(s, mixed_resolutions=mixed_resolutions))

    def uncompact(self, target_resolution: int) -> pl.Expr:
        return self.__expr_map_series(lambda s: uncompact(s, target_resolution))

    def cells_area_m2(self) -> pl.Expr:
        return self.__expr_map_series(lambda s: cells_area_m2(s)).alias("area_m2")

    def cells_area_km2(self) -> pl.Expr:
        return self.__expr_map_series(lambda s: cells_area_km2(s)).alias("area_km2")

    def cells_area_rads2(self) -> pl.Expr:
        return self.__expr_map_series(lambda s: cells_area_rads2(s)).alias("area_rads2")

    def cells_valid(self) -> pl.Expr:
        return self.__expr_map_series(lambda s: cells_valid(s)).alias("cells_valid")

    def vertexes_valid(self) -> pl.Expr:
        return self.__expr_map_series(lambda s: vertexes_valid(s)).alias("vertexes_valid")

    def directededges_valid(self) -> pl.Expr:
        return self.__expr_map_series(lambda s: directededges_valid(s)).alias("directededges_valid")

    def cells_to_string(self) -> pl.Expr:
        return self.__expr_map_series(lambda s: cells_to_string(s))

    def vertexes_to_string(self) -> pl.Expr:
        return self.__expr_map_series(lambda s: vertexes_to_string(s))

    def directededges_to_string(self) -> pl.Expr:
        return self.__expr_map_series(lambda s: directededges_to_string(s))


@pl.api.register_series_namespace("h3")
class H3SeriesShortcuts:
    """
    Registers H3 functionality with polars Series.

    The methods of this class mirror the functionality provided by the functions of this
    module. Please refer to the module functions for more documentation.
    """

    def __init__(self, s: pl.Series):
        self._s = s

    def cells_resolution(self) -> pl.Series:
        return cells_resolution(self._s)

    def change_resolution(self, resolution: int) -> pl.Series:
        return change_resolution(self._s, resolution)

    def change_resolution_list(self, resolution: int) -> pl.Series:
        return change_resolution_list(self._s, resolution)

    def cells_parse(self, set_failing_to_invalid: bool = False) -> pl.Series:
        return cells_parse(self._s, set_failing_to_invalid=set_failing_to_invalid)

    def vertexes_parse(self, set_failing_to_invalid: bool = False) -> pl.Series:
        return vertexes_parse(self._s, set_failing_to_invalid=set_failing_to_invalid)

    def directededges_parse(self, set_failing_to_invalid: bool = False) -> pl.Series:
        return directededges_parse(self._s, set_failing_to_invalid=set_failing_to_invalid)

    def grid_disk(self, k: int, flatten: bool = False) -> pl.Series:
        return grid_disk(self._s, k, flatten=flatten)

    def compact(self, mixed_resolutions: bool = False) -> pl.Series:
        return compact(self._s, mixed_resolutions=mixed_resolutions)

    def uncompact(self, target_resolution: int) -> pl.Series:
        return uncompact(self._s, target_resolution)

    def cells_area_m2(self) -> pl.Series:
        return cells_area_m2(self._s)

    def cells_area_km2(self) -> pl.Series:
        return cells_area_km2(self._s)

    def cells_area_rads2(self) -> pl.Series:
        return cells_area_rads2(self._s)

    def cells_valid(self) -> pl.Series:
        return cells_valid(self._s)

    def vertexes_valid(self) -> pl.Series:
        return vertexes_valid(self._s)

    def directededges_valid(self) -> pl.Series:
        return directededges_valid(self._s)

    def cells_to_string(self) -> pl.Series:
        return cells_to_string(self._s)

    def vertexes_to_string(self) -> pl.Series:
        return vertexes_to_string(self._s)

    def directededges_to_string(self) -> pl.Series:
        return directededges_to_string(self._s)


__all__ = [
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
    H3Expr.__name__,
    H3SeriesShortcuts.__name__,
]
