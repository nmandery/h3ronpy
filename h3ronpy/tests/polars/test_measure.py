import numpy as np
import h3.api.numpy_int as h3
from h3ronpy.polars import cells_area_km2
import polars as pl


def test_cells_area_km2():
    cells = np.array(
        [
            h3.geo_to_h3(10.3, 45.1, 8),
            h3.geo_to_h3(10.3, 45.1, 5),
            h3.geo_to_h3(10.3, 45.1, 3),
        ],
        dtype=np.uint64,
    )
    areas = cells_area_km2(cells)
    assert isinstance(areas, pl.Series)
    assert len(areas) == 3
    assert int(areas[0] * 100) == 62
    assert int(areas[1]) == 213
    assert int(areas[2]) == 10456
