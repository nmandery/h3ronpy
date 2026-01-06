import h3.api.numpy_int as h3
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
