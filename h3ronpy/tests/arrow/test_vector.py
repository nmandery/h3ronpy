from h3ronpy.arrow.vector import geometry_to_cells, ContainmentMode, cells_to_wkb_points
import pyarrow as pa
import shapely
from shapely.geometry import Point
from shapely import from_wkb
import h3.api.numpy_int as h3
from arro3.core import Array, DataType


def test_geometry_to_cells():
    geom = shapely.Polygon(((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)))
    cells = geometry_to_cells(geom, 5, containment_mode=ContainmentMode.IntersectsBoundary)
    assert isinstance(cells, Array)
    assert cells.type == DataType.uint64()
    assert len(cells) > 10


def test_geometry_to_cells_central_park():
    # Manhattan Central Park
    point = Point(-73.9575, 40.7938)

    arr = geometry_to_cells(point, 8).to_numpy()
    assert len(arr) == 1
    assert arr[0] == h3.geo_to_h3(point.y, point.x, 8)


def test_coordinate_values_are_not_equal_issue_58():
    # Step 1: Create a point (latitude and longitude)
    lat, lon = 37.7749, -122.4194  # Example coordinates (San Francisco)
    point = Point(lon, lat)  # shapely expects (longitude, latitude)

    # Step 2: Convert the point to an H3 cell (resolution 9 for example)
    resolution = 9
    h3_cells = geometry_to_cells(point, resolution)

    # Step 3: Convert the H3 cell back to WKB points
    wkb_points = cells_to_wkb_points(h3_cells)

    assert len(wkb_points) == 1

    # Step 4: Decode the WKB point to a Shapely geometry
    for wkb_point in wkb_points:
        shapely_point = shapely.from_wkb(wkb_point.as_py())
        assert int(lat) == int(shapely_point.y)
        assert int(lon) == int(shapely_point.x)
