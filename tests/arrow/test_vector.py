from h3ronpy.arrow.vector import geometry_to_cells, cells_to_coordinates
import pyarrow as pa
import shapely
from shapely.geometry import Point
import h3.api.numpy_int as h3


def test_geometry_to_cells():
    geom = shapely.Polygon(((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)))
    cells = geometry_to_cells(geom, 5, all_intersecting=True)
    assert isinstance(cells, pa.Array)
    assert cells.type == pa.uint64()
    assert len(cells) > 10


def test_geometry_to_cells_central_park():
    # Manhattan Central Park
    point = Point(-73.9575, 40.7938)

    arr = geometry_to_cells(point, 8).to_numpy()
    assert len(arr) == 1
    assert arr[0] == h3.geo_to_h3(point.y, point.x, 8)