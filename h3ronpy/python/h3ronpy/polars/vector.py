from . import _wrap
from ..arrow import vector as _av
import polars as pl

cells_to_coordinates = _wrap(_av.cells_to_coordinates, ret_type=pl.DataFrame)
coordinates_to_cells = _wrap(_av.coordinates_to_cells, ret_type=pl.Series)
cells_bounds = _av.cells_bounds
cells_bounds_arrays = _wrap(_av.cells_bounds_arrays, ret_type=pl.DataFrame)
cells_to_wkb_polygons = _wrap(_av.cells_to_wkb_polygons, ret_type=pl.Series)
cells_to_wkb_points = _wrap(_av.cells_to_wkb_points, ret_type=pl.Series)
vertexes_to_wkb_points = _wrap(_av.vertexes_to_wkb_points, ret_type=pl.Series)
wkb_to_cells = _wrap(_av.wkb_to_cells, ret_type=pl.Series)
geometry_to_cells = _wrap(_av.geometry_to_cells, ret_type=pl.Series)

__all__ = [
    cells_to_coordinates.__name__,
    coordinates_to_cells.__name__,
    cells_bounds.__name__,
    cells_bounds_arrays.__name__,
    cells_to_wkb_polygons.__name__,
    cells_to_wkb_points.__name__,
    vertexes_to_wkb_points.__name__,
    wkb_to_cells.__name__,
    geometry_to_cells.__name__,
]
