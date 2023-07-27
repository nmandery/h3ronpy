from h3ronpy.polars.vector import cells_to_coordinates, cells_bounds, cells_bounds_arrays
import polars as pl
import numpy as np
import h3.api.numpy_int as h3


def test_cells_to_coordinates():
    h3indexes = np.array(
        [
            h3.geo_to_h3(10.3, 45.1, 8),
        ],
        dtype=np.uint64,
    )
    coords = cells_to_coordinates(h3indexes)
    assert len(coords) == 1
    assert 10.0 < coords["lat"][0] < 11.0
    assert 45.0 < coords["lng"][0] < 46.0


def test_cells_bounds():
    h3indexes = np.array(
        [
            h3.geo_to_h3(10.3, 45.1, 8),
        ],
        dtype=np.uint64,
    )
    bounds = cells_bounds(h3indexes)
    assert bounds is not None
    assert type(bounds) == tuple
    assert len(bounds) == 4
    assert bounds[0] < bounds[2]
    assert bounds[1] < bounds[3]


def test_cells_bounds_arrays():
    h3indexes = np.array(
        [
            h3.geo_to_h3(10.3, 45.1, 8),
            h3.geo_to_h3(10.3, 45.1, 5),
        ],
        dtype=np.uint64,
    )
    bounds_df = cells_bounds_arrays(h3indexes)
    assert bounds_df is not None
    assert isinstance(bounds_df, pl.DataFrame)
    assert len(bounds_df) == 2
    assert "minx" in bounds_df
    assert "maxx" in bounds_df
    assert "miny" in bounds_df
    assert "maxy" in bounds_df
    assert bounds_df["minx"][0] < 45.1
    assert bounds_df["maxx"][0] > 45.1
    assert bounds_df["miny"][0] < 10.3
    assert bounds_df["maxy"][0] > 10.3
