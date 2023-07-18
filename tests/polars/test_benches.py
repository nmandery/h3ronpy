import numpy as np
import h3.api.numpy_int as h3
from h3ronpy.polars import cells_to_string
import polars as pl


def some_cells() -> pl.Series:
    return np.full(1000, h3.geo_to_h3(45.5, 10.2, 5), dtype="uint64")


def benchmark_h3_to_string_python_list(cells):
    return [h3.h3_to_string(cell) for cell in cells]


def test_cells_to_string(benchmark):
    benchmark(cells_to_string, pl.Series(some_cells()))


def test_h3_to_string_python_list(benchmark):
    benchmark(benchmark_h3_to_string_python_list, list(some_cells()))


h3_to_string_numpy_vectorized = np.vectorize(
    h3.h3_to_string,
    otypes=[
        str,
    ],
)


def test_h3_to_string_numpy_vectorized(benchmark):
    benchmark(h3_to_string_numpy_vectorized, some_cells())
