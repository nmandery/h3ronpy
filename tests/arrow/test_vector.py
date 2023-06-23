from h3ronpy.arrow.vector import geometry_to_cells
import pyarrow as pa
import shapely


def test_geometry_to_cells():
    geom = shapely.Polygon(((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)))
    cells = geometry_to_cells(geom, 5, all_intersecting=True)
    assert isinstance(cells, pa.Array)
    assert cells.type == pa.uint64()
    assert len(cells) > 10
