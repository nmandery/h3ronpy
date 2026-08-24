import h3.api.numpy_int as h3
import numpy as np
import pytest
import shapely
from arro3.core import Array, DataType, Scalar
from h3ronpy import uncompact
from h3ronpy.vector import (
    ContainmentMode,
    cells_to_wkb_points,
    cells_to_wkb_polygons,
    directededges_to_wkb_linestrings,
    geometry_to_cells,
    vertexes_to_wkb_points,
    wkb_to_cells,
)
from shapely import wkb
from shapely.geometry import Point


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
    assert arr[0] == h3.latlng_to_cell(point.y, point.x, 8)


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
    for wkb_point in iter(wkb_points):
        assert isinstance(wkb_point, Scalar)  # Ensure it's an arro3 Scalar
        shapely_point = wkb.loads(wkb_point.as_py())
        assert int(lat) == int(shapely_point.y)
        assert int(lon) == int(shapely_point.x)


SOME_POLYGON = shapely.Polygon(((10.0, 45.0), (10.0, 46.0), (11.0, 46.0), (11.0, 45.0), (10.0, 45.0)))


def test_wkb_outputs_are_large_binary():
    # regression test: with geoarrow-array 0.8, the `WkbArray` alias uses i32
    # offsets. All WKB-producing functions must keep returning large_binary
    # (i64 offsets) as in h3ronpy 0.22.
    cells = geometry_to_cells(SOME_POLYGON, 5)
    assert len(cells) > 0

    assert cells_to_wkb_polygons(cells).type == DataType.large_binary()
    assert cells_to_wkb_polygons(cells, link_cells=True).type == DataType.large_binary()
    assert cells_to_wkb_points(cells).type == DataType.large_binary()

    some_cell = h3.latlng_to_cell(45.5, 10.2, 5)
    vertexes = np.array([h3.cell_to_vertex(some_cell, v) for v in range(6)], dtype=np.uint64)
    assert vertexes_to_wkb_points(vertexes).type == DataType.large_binary()

    edges = np.array(h3.origin_to_directed_edges(some_cell), dtype=np.uint64)
    assert directededges_to_wkb_linestrings(edges).type == DataType.large_binary()


def test_wkb_polygons_field_name_and_extension_metadata():
    cells = geometry_to_cells(SOME_POLYGON, 5)
    field = cells_to_wkb_polygons(cells).field

    assert field.name == "geometry"
    metadata = {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in field.metadata.items()
    }
    assert metadata.get("ARROW:extension:name") == "geoarrow.wkb"


def test_wkb_to_cells_flatten_and_compact():
    wkb_array = Array([shapely.to_wkb(SOME_POLYGON)], type=DataType.binary())

    list_result = wkb_to_cells(wkb_array, 7)
    flat_result = wkb_to_cells(wkb_array, 7, flatten=True)

    assert flat_result.type == DataType.uint64()
    cells_from_list = [cell for cells in list_result.to_pylist() for cell in cells]
    assert sorted(cells_from_list) == sorted(flat_result.to_pylist())

    compact_lists = wkb_to_cells(wkb_array, 7, compact=True)
    compact_cells = [cell for cells in compact_lists.to_pylist() for cell in cells]
    assert 0 < len(compact_cells) < len(flat_result)
    # uncompacting must restore the exact same cell set
    compact_array = Array(compact_cells, type=DataType.uint64())
    assert sorted(uncompact(compact_array, 7).to_pylist()) == sorted(flat_result.to_pylist())


@pytest.mark.xfail(
    reason="compact+flatten compacts the already per-geometry-compacted (mixed resolution) "
    "cells a second time and fails with 'heterogeneous resolution'. Present in 0.22.0 as well.",
    strict=True,
)
def test_wkb_to_cells_compact_and_flatten_combined():
    wkb_array = Array([shapely.to_wkb(SOME_POLYGON)], type=DataType.binary())
    wkb_to_cells(wkb_array, 7, compact=True, flatten=True)


def test_wkb_to_cells_containment_modes():
    wkb_array = Array([shapely.to_wkb(SOME_POLYGON)], type=DataType.binary())

    counts = {
        mode: len(wkb_to_cells(wkb_array, 6, containment_mode=mode, flatten=True))
        for mode in (
            ContainmentMode.ContainsBoundary,
            ContainmentMode.ContainsCentroid,
            ContainmentMode.IntersectsBoundary,
            ContainmentMode.Covers,
        )
    }
    assert all(count > 0 for count in counts.values())
    assert counts[ContainmentMode.ContainsBoundary] <= counts[ContainmentMode.IntersectsBoundary]
    assert counts[ContainmentMode.ContainsCentroid] <= counts[ContainmentMode.IntersectsBoundary]
    assert counts[ContainmentMode.IntersectsBoundary] <= counts[ContainmentMode.Covers]


def test_wkb_to_cells_null_entries_are_preserved():
    wkb_array = Array([shapely.to_wkb(SOME_POLYGON), None], type=DataType.binary())

    result = wkb_to_cells(wkb_array, 5).to_pylist()
    assert len(result) == 2
    assert len(result[0]) > 0
    assert result[1] is None
