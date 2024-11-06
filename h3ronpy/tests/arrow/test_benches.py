import h3.api.numpy_int as h3
import numpy as np
import polars as pl
from h3ronpy.arrow import cells_to_string


def some_cells() -> np.ndarray:
    return np.full(1000, h3.latlng_to_cell(45.5, 10.2, 5), dtype="uint64")


def benchmark_h3_to_string_python_list(cells):
    return [h3.int_to_str(cell) for cell in cells]


def test_cells_to_string(benchmark):
    benchmark(cells_to_string, pl.Series(some_cells()))


def test_h3_to_string_python_list(benchmark):
    benchmark(benchmark_h3_to_string_python_list, list(some_cells()))


h3_to_string_numpy_vectorized = np.vectorize(
    h3.int_to_str,
    otypes=[
        str,
    ],
)


def test_h3_to_string_numpy_vectorized(benchmark):
    benchmark(h3_to_string_numpy_vectorized, some_cells())
