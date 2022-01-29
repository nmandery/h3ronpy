import geopandas as gpd
from shapely.geometry import Point, GeometryCollection
from h3ronpy.vector import geodataframe_to_h3


def test_geodataframe_to_h3():
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    africa = world[world["continent"] == "Africa"]
    df = geodataframe_to_h3(africa, 4)
    assert len(df) > 1000
    assert df.dtypes["h3index"] == "uint64"


def test_empty_geometry_omitted():
    gdf = gpd.GeoDataFrame({"geometry": [Point(),]}, crs="epsg:4326")
    df = geodataframe_to_h3(gdf, 4)
    assert len(df) == 0

    gdf = gpd.GeoDataFrame({"geometry": [GeometryCollection(),]}, crs="epsg:4326")
    df = geodataframe_to_h3(gdf, 4)
    assert len(df) == 0
