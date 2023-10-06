import polars as pl
from . import some_cell_series

# register expressions with polars
import h3ronpy.polars as _


def test_series_cells_resolution():
    resolution = some_cell_series().h3.cells_resolution()
    assert resolution.dtype == pl.UInt8
    assert resolution[0] == 8
