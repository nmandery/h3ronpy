"""
API to use `h3ronpy` with the `pandas dataframe library <https://pandas.pydata.org>`_ including `geopandas <https://geopandas.org>`_.

.. warning::

    To avoid pulling in unused dependencies, `h3ronpy` does not declare a dependency to `pandas` and `geopandas`. These
    packages need to be installed separately.

"""
from .. import arrow as _arrow
import pyarrow as pa
from functools import wraps
import pandas as pd


def _wrap(func, ret_type=None):
    @wraps(func)
    def wrapper(*args, **kw):
        result = func(*args, **kw)
        if isinstance(result, pa.Table):
            return result.to_pandas(split_blocks=True, self_destruct=True)
        elif isinstance(result, pa.Array):
            return result.to_pandas()
        return result

    if ret_type:
        # create a copy to avoid modifying the dict of the wrapped function
        wrapper.__annotations__ = dict(**wrapper.__annotations__)
        wrapper.__annotations__["return"] = ret_type
    return wrapper


change_resolution = _wrap(_arrow.change_resolution, ret_type=pd.Series)
change_resolution.__annotations__["return"] = pd.Series
change_resolution_paired = _wrap(_arrow.change_resolution_paired, ret_type=pd.DataFrame)
cells_resolution = _wrap(_arrow.cells_resolution, ret_type=pd.Series)
cells_parse = _wrap(_arrow.cells_parse, ret_type=pd.Series)
compact = _wrap(_arrow.compact, ret_type=pd.Series)
uncompact = _wrap(_arrow.uncompact, ret_type=pd.Series)
cells_valid = _wrap(_arrow.cells_valid, ret_type=pd.Series)
vertexes_valid = _wrap(_arrow.vertexes_valid, ret_type=pd.Series)
directededges_valid = _wrap(_arrow.directededges_valid, ret_type=pd.Series)
grid_disk = _wrap(_arrow.grid_disk, ret_type=pd.Series)
grid_disk_distances = _wrap(_arrow.grid_disk_distances, ret_type=pd.DataFrame)
grid_ring_distances = _wrap(_arrow.grid_ring_distances, ret_type=pd.DataFrame)
grid_disk_aggregate_k = _wrap(_arrow.grid_disk_aggregate_k, ret_type=pd.DataFrame)
cells_area_m2 = _wrap(_arrow.cells_area_m2, ret_type=pd.Series)
cells_area_km2 = _wrap(_arrow.cells_area_km2, ret_type=pd.Series)
cells_area_rads2 = _wrap(_arrow.cells_area_rads2, ret_type=pd.Series)
cells_to_string = _wrap(_arrow.cells_to_string, ret_type=pd.Series)
vertexes_to_string = _wrap(_arrow.vertexes_to_string, ret_type=pd.Series)
directededges_to_string = _wrap(_arrow.directededges_to_string, ret_type=pd.Series)


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
