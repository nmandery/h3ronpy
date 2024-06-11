import polars as pl
import numpy as np
import h3.api.numpy_int as h3


def some_cell_series() -> pl.Series:
    return pl.Series(
        np.array(
            [
                h3.geo_to_h3(10.3, 45.1, 8),
            ],
            dtype=np.uint64,
        )
    )
