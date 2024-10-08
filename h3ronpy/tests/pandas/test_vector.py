import shapely

import pandas as pd
from shapely.geometry import Point, GeometryCollection, Polygon
import pytest
from h3ronpy.pandas import change_resolution
from h3ronpy.pandas.vector import (
    cells_to_points,
    cells_to_polygons,
    cells_dataframe_to_geodataframe,
    geodataframe_to_cells,
    geoseries_to_cells,
)
from h3ronpy import DEFAULT_CELL_COLUMN_NAME, ContainmentMode
import geopandas as gpd
from .. import load_africa, TESTDATA_PATH


def test_cells_to_points():
    gs = cells_to_points(pd.Series(
        [
            0x8009FFFFFFFFFFF,
        ]
    ))
    assert isinstance(gs, gpd.GeoSeries)
    assert gs.geom_type[0] == "Point"


def test_cells_to_polygons():
    cells = change_resolution(
        [
            0x8009FFFFFFFFFFF,
        ],
        3,
    )
    gs = cells_to_polygons(cells)
    assert isinstance(gs, gpd.GeoSeries)
    assert gs.geom_type[0] == "Polygon"
    assert len(gs) == 286

    linked_gs = cells_to_polygons(cells, link_cells=True)
    assert isinstance(linked_gs, gpd.GeoSeries)
    assert linked_gs.geom_type[0] == "Polygon"
    assert len(linked_gs) == 1
    assert shapely.get_num_coordinates(linked_gs[0]) > 120


def test_cells_dataframe_to_geodataframe():
    df = pd.DataFrame(
        {
            DEFAULT_CELL_COLUMN_NAME: [
                0x8009FFFFFFFFFFF,
            ],
            "id": [
                5,
            ],
        }
    )
    gdf = cells_dataframe_to_geodataframe(df)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) == len(df)
    assert (gdf[DEFAULT_CELL_COLUMN_NAME] == df[DEFAULT_CELL_COLUMN_NAME]).all()
    assert (gdf["id"] == df["id"]).all()
    assert gdf.geometry.geom_type[0] == "Polygon"


def test_cells_dataframe_to_geodataframe_empty():
    # https://github.com/nmandery/h3ron/issues/17
    df = pd.DataFrame({DEFAULT_CELL_COLUMN_NAME: []})
    gdf = cells_dataframe_to_geodataframe(df)  # should not raise an ValueError.
    assert gdf.empty


def test_cells_geodataframe_to_cells():
    africa = load_africa()
    df = geodataframe_to_cells(africa, 4)
    assert len(df) > len(africa)
    assert df.dtypes[DEFAULT_CELL_COLUMN_NAME] == "uint64"


def test_geoseries_to_cells_flatten():
    africa = load_africa()
    cells = geoseries_to_cells(africa.geometry, 4, flatten=True)
    assert len(cells) >= len(africa)
    assert cells.dtype == "uint64"


@pytest.mark.skip(
    reason="GeometryCollections are unsupported until https://github.com/geoarrow/geoarrow-rs/blob/3a2aaa883126274037cabaf46b1f5f6459938297/src/io/wkb/reader/geometry_collection.rs#L23 is fixed"
)
def test_empty_geometrycollection_omitted():
    gdf = gpd.GeoDataFrame(
        {
            "geometry": [
                GeometryCollection(),
            ]
        },
        crs="epsg:4326",
    )
    df = geodataframe_to_cells(gdf, 4)
    assert len(df) == 0


def test_fail_on_empty_point():
    gdf = gpd.GeoDataFrame(
        {
            "geometry": [
                Point(),
            ]
        },
        crs="epsg:4326",
    )
    with pytest.raises(ValueError):
        geodataframe_to_cells(gdf, 4)


def test_geometry_results_in_no_cells():
    gdf = gpd.GeoDataFrame(
        {
            "geometry": [
                Polygon(
                    [
                        (1.100000, 4.50000),
                        (1.100001, 4.50000),
                        (1.100001, 4.50001),
                        (1.100000, 4.50001),
                        (1.100000, 4.50000),
                    ]
                ),
            ],
            "col1": [1],
        },
        crs="epsg:4326",
    )
    df = geodataframe_to_cells(gdf, 4, containment_mode=ContainmentMode.ContainsCentroid)
    assert len(df) == 0


def test_non_standard_geometry_column_name():
    africa = load_africa()
    assert africa.geometry.name == "geometry"
    africa.rename_geometry("something_else", inplace=True)
    assert africa.geometry.name == "something_else"
    df = geodataframe_to_cells(africa, 4)
    assert len(df) > len(africa)
    assert df.dtypes[DEFAULT_CELL_COLUMN_NAME] == "uint64"
