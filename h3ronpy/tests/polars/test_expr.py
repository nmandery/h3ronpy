import h3.api.numpy_int as h3

# register expressions with polars
import h3ronpy.polars as _  # noqa: F401
import numpy as np
import polars as pl


def some_cell_series() -> pl.Series:
    return pl.Series(
        np.array(
            [
                h3.latlng_to_cell(10.3, 45.1, 8),
            ],
            dtype=np.uint64,
        )
    )


def test_expr_cells_resolution():
    df = pl.DataFrame({"cells": some_cell_series()})
    df.lazy().with_columns(
        [
            pl.col("cells").h3.cells_resolution().alias("resolution"),
        ]
    ).collect()

    pl.col("cells")

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


def test_series():
    s = some_cell_series()
    assert s.h3.cells_resolution()[0] == 8

    assert s.h3.change_resolution(5)[0] == 600436446234411007
