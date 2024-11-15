import h3.api.numpy_int as h3
import numpy as np
from arro3.core import Array
from h3ronpy.arrow import cells_area_km2


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
    assert isinstance(areas, Array)
    assert len(areas) == 3
    assert int(areas[0].as_py() * 100) == 62
    assert int(areas[1].as_py()) == 213
    assert int(areas[2].as_py()) == 10456
