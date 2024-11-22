"""
API to use `h3ronpy` with the `polars dataframe library <https://www.pola.rs/>`_.

.. warning::

    To avoid pulling in unused dependencies, `h3ronpy` does not declare a dependency to `polars`. This
    package needs to be installed separately.

"""

from __future__ import annotations

import typing
from functools import wraps

import polars as pl
from arro3.core import ChunkedArray
from arro3.core.types import ArrowArrayExportable

import h3ronpy


# Wrapper for calling arrow-based operations on polars Series.
def _wrap(func: typing.Callable[..., ArrowArrayExportable]):
    @wraps(func, updated=())
    def wrapper(*args, **kw):
        # This _should_ always be a contiguous single-chunk Series already, because
        # we're inside map_batches. So combine_chunks should be free.
        ca = ChunkedArray.from_arrow(args[0])
        array = ca.combine_chunks()
        new_args = list(args)
        new_args[0] = array
        result = func(*new_args, **kw)
        return pl.Series(result)

    return wrapper


@pl.api.register_expr_namespace("h3")
class H3Expr:
    """
    Registers H3 functionality with polars Expr expressions.

    The methods of this class mirror the functionality provided by the functions of this
    module. Please refer to the module functions for more documentation.
    """

    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def __expr_map_series(self, func: typing.Callable[..., ArrowArrayExportable]) -> pl.Expr:
        wrapped_func = _wrap(func)

        if hasattr(self._expr, "map"):
            # polars < 1.0
            return self._expr.map(wrapped_func)

        return self._expr.map_batches(wrapped_func)

    def cells_resolution(self) -> pl.Expr:
        return self.__expr_map_series(h3ronpy.cells_resolution).alias("resolution")

    def change_resolution(self, resolution: int) -> pl.Expr:
        return self.__expr_map_series(lambda s: h3ronpy.change_resolution(s, resolution))

    def change_resolution_list(self, resolution: int) -> pl.Expr:
        return self.__expr_map_series(lambda s: h3ronpy.change_resolution_list(s, resolution))

    def cells_parse(self, set_failing_to_invalid: bool = False) -> pl.Expr:
        return self.__expr_map_series(
            lambda s: h3ronpy.cells_parse(s, set_failing_to_invalid=set_failing_to_invalid)
        ).alias("cell")

    def vertexes_parse(self, set_failing_to_invalid: bool = False) -> pl.Expr:
        return self.__expr_map_series(
            lambda s: h3ronpy.vertexes_parse(s, set_failing_to_invalid=set_failing_to_invalid)
        ).alias("vertex")

    def directededges_parse(self, set_failing_to_invalid: bool = False) -> pl.Expr:
        return self.__expr_map_series(
            lambda s: h3ronpy.directededges_parse(s, set_failing_to_invalid=set_failing_to_invalid)
        ).alias("directededge")

    def grid_disk(self, k: int, flatten: bool = False) -> pl.Expr:
        return self.__expr_map_series(lambda s: h3ronpy.grid_disk(s, k, flatten=flatten))

    def compact(self, mixed_resolutions: bool = False) -> pl.Expr:
        return self.__expr_map_series(lambda s: h3ronpy.compact(s, mixed_resolutions=mixed_resolutions))

    def uncompact(self, target_resolution: int) -> pl.Expr:
        return self.__expr_map_series(lambda s: h3ronpy.uncompact(s, target_resolution))

    def cells_area_m2(self) -> pl.Expr:
        return self.__expr_map_series(h3ronpy.cells_area_m2).alias("area_m2")

    def cells_area_km2(self) -> pl.Expr:
        return self.__expr_map_series(h3ronpy.cells_area_km2).alias("area_km2")

    def cells_area_rads2(self) -> pl.Expr:
        return self.__expr_map_series(h3ronpy.cells_area_rads2).alias("area_rads2")

    def cells_valid(self) -> pl.Expr:
        return self.__expr_map_series(h3ronpy.cells_valid).alias("cells_valid")

    def vertexes_valid(self) -> pl.Expr:
        return self.__expr_map_series(h3ronpy.vertexes_valid).alias("vertexes_valid")

    def directededges_valid(self) -> pl.Expr:
        return self.__expr_map_series(h3ronpy.directededges_valid).alias("directededges_valid")

    def cells_to_string(self) -> pl.Expr:
        return self.__expr_map_series(h3ronpy.cells_to_string)

    def vertexes_to_string(self) -> pl.Expr:
        return self.__expr_map_series(h3ronpy.vertexes_to_string)

    def directededges_to_string(self) -> pl.Expr:
        return self.__expr_map_series(h3ronpy.directededges_to_string)


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
        return _wrap(h3ronpy.cells_resolution)(self._s)

    def change_resolution(self, resolution: int) -> pl.Series:
        return _wrap(h3ronpy.change_resolution)(self._s, resolution)

    def change_resolution_list(self, resolution: int) -> pl.Series:
        return _wrap(h3ronpy.change_resolution_list)(self._s, resolution)

    def cells_parse(self, set_failing_to_invalid: bool = False) -> pl.Series:
        return _wrap(h3ronpy.cells_parse)(self._s, set_failing_to_invalid=set_failing_to_invalid)

    def vertexes_parse(self, set_failing_to_invalid: bool = False) -> pl.Series:
        return _wrap(h3ronpy.vertexes_parse)(self._s, set_failing_to_invalid=set_failing_to_invalid)

    def directededges_parse(self, set_failing_to_invalid: bool = False) -> pl.Series:
        return _wrap(h3ronpy.directededges_parse)(self._s, set_failing_to_invalid=set_failing_to_invalid)

    def grid_disk(self, k: int, flatten: bool = False) -> pl.Series:
        return _wrap(h3ronpy.grid_disk)(self._s, k, flatten=flatten)

    def compact(self, mixed_resolutions: bool = False) -> pl.Series:
        return _wrap(h3ronpy.compact)(self._s, mixed_resolutions=mixed_resolutions)

    def uncompact(self, target_resolution: int) -> pl.Series:
        return _wrap(h3ronpy.uncompact)(self._s, target_resolution)

    def cells_area_m2(self) -> pl.Series:
        return _wrap(h3ronpy.cells_area_m2)(self._s)

    def cells_area_km2(self) -> pl.Series:
        return _wrap(h3ronpy.cells_area_km2)(self._s)

    def cells_area_rads2(self) -> pl.Series:
        return _wrap(h3ronpy.cells_area_rads2)(self._s)

    def cells_valid(self) -> pl.Series:
        return _wrap(h3ronpy.cells_valid)(self._s)

    def vertexes_valid(self) -> pl.Series:
        return _wrap(h3ronpy.vertexes_valid)(self._s)

    def directededges_valid(self) -> pl.Series:
        return _wrap(h3ronpy.directededges_valid)(self._s)

    def cells_to_string(self) -> pl.Series:
        return _wrap(h3ronpy.cells_to_string)(self._s)

    def vertexes_to_string(self) -> pl.Series:
        return _wrap(h3ronpy.vertexes_to_string)(self._s)

    def directededges_to_string(self) -> pl.Series:
        return _wrap(h3ronpy.directededges_to_string)(self._s)


__all__ = [
    H3Expr.__name__,
    H3SeriesShortcuts.__name__,
]
