import polars as pl
from . import some_cell_series

# register expressions with polars
import h3ronpy.polars as _


def test_expr_cells_resolution():
    df = (
        pl.DataFrame({"cells": some_cell_series()})
        .lazy()
        .with_columns(
            [
                pl.col("cells").h3.cells_resolution().alias("resolution"),
            ]
        )
        .collect()
    )
    assert df["resolution"].dtype == pl.UInt8
    assert df["resolution"][0] == 8


def test_expr_grid_disk():
    df = (
        pl.DataFrame({"cells": some_cell_series()})
        .lazy()
        .with_columns(
            [
                pl.col("cells").h3.grid_disk(1).alias("disk"),
            ]
        )
        .collect()
    )
    assert df["disk"].dtype == pl.List
    assert df["disk"].dtype.inner == pl.UInt64
    assert len(df["disk"][0]) == 7
