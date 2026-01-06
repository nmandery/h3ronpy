import h3.api.numpy_int as h3
import numpy as np
from arro3.core import RecordBatch
from h3ronpy.vector import (
    cells_bounds,
    cells_bounds_arrays,
    cells_to_coordinates,
    coordinates_to_cells,
)


def test_cells_to_coordinates():
    h3indexes = np.array(
        [
            h3.latlng_to_cell(10.3, 45.1, 8),
        ],
        dtype=np.uint64,
    )
    coords = cells_to_coordinates(h3indexes)
    assert coords.num_rows == 1
    assert 10.0 < coords["lat"][0].as_py() < 11.0
    assert 45.0 < coords["lng"][0].as_py() < 46.0


def test_coordinates_to_cells():
    lat = np.array([10.3, 23.1], dtype=np.float64)
    lng = np.array([45.1, 2.3], dtype=np.float64)
    r = 7
    cells = coordinates_to_cells(lat, lng, r)
    assert len(cells) == 2
    assert cells[0] == h3.latlng_to_cell(lat[0], lng[0], r)
    assert cells[1] == h3.latlng_to_cell(lat[1], lng[1], r)


def test_coordinates_to_cells_resolution_array():
    lat = np.array([10.3, 23.1], dtype=np.float64)
    lng = np.array([45.1, 2.3], dtype=np.float64)
    r = np.array([9, 12], dtype=np.uint8)
    cells = coordinates_to_cells(lat, lng, r)
    assert len(cells) == 2
    assert cells[0] == h3.latlng_to_cell(lat[0], lng[0], r[0])
    assert cells[1] == h3.latlng_to_cell(lat[1], lng[1], r[1])


def test_cells_bounds():
    h3indexes = np.array(
        [
            h3.latlng_to_cell(10.3, 45.1, 8),
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
            h3.latlng_to_cell(10.3, 45.1, 8),
            h3.latlng_to_cell(10.3, 45.1, 5),
        ],
        dtype=np.uint64,
    )
    bounds_df = cells_bounds_arrays(h3indexes)
    assert bounds_df is not None
    assert isinstance(bounds_df, RecordBatch)
    assert bounds_df.num_rows == 2
    assert "minx" in bounds_df.schema.names
    assert "maxx" in bounds_df.schema.names
    assert "miny" in bounds_df.schema.names
    assert "maxy" in bounds_df.schema.names
    assert bounds_df["minx"][0].as_py() < 45.1
    assert bounds_df["maxx"][0].as_py() > 45.1
    assert bounds_df["miny"][0].as_py() < 10.3
    assert bounds_df["maxy"][0].as_py() > 10.3
