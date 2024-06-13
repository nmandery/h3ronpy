from pathlib import Path
import geopandas as gpd

TESTDATA_PATH = Path(__file__).parent.parent / "data"


def load_africa() -> gpd.GeoDataFrame:
    world = gpd.read_file(TESTDATA_PATH / "naturalearth_110m_admin_0_countries.fgb")
    return world[world["CONTINENT"] == "Africa"]
