from . import _wrap
from ..arrow import vector as _av
from .. import ContainmentMode
from ..arrow import util as _arrow_util
import pyarrow as pa
import pandas as pd
import geopandas as gpd
from functools import wraps
from typing import Optional
from .. import H3_CRS, DEFAULT_CELL_COLUMN_NAME


def _geoseries_from_wkb(func, doc: Optional[str] = None, name: Optional[str] = None):
    @wraps(func)
    def wrapper(*args, **kw):
        return gpd.GeoSeries.from_wkb(func(*args, **kw), crs=H3_CRS)

    # create a copy to avoid modifying the dict of the wrapped function
    wrapper.__annotations__ = dict(**wrapper.__annotations__)
    wrapper.__annotations__["return"] = gpd.GeoSeries
    if doc is not None:
        wrapper.__doc__ = doc
    if name is not None:
        wrapper.__name__ = name

    return wrapper


cells_to_coordinates = _wrap(_av.cells_to_coordinates, ret_type=pd.DataFrame)
coordinates_to_cells = _wrap(_av.coordinates_to_cells, ret_type=pd.Series)
cells_bounds = _av.cells_bounds
cells_bounds_arrays = _wrap(_av.cells_bounds_arrays, ret_type=pd.DataFrame)
cells_to_wkb_polygons = _wrap(_av.cells_to_wkb_polygons, ret_type=pd.Series)
cells_to_polygons = _geoseries_from_wkb(
    cells_to_wkb_polygons,
    doc="Create a geoseries containing the polygon geometries of a cell array",
    name="cells_to_polygons",
)
cells_to_wkb_points = _wrap(_av.cells_to_wkb_points, ret_type=pd.Series)
cells_to_points = _geoseries_from_wkb(
    cells_to_wkb_points,
    doc="Create a geoseries containing the centroid point geometries of a cell array",
    name="cells_to_points",
)
vertexes_to_wkb_points = _wrap(_av.vertexes_to_wkb_points, ret_type=pd.Series)
vertexes_to_points = _geoseries_from_wkb(
    vertexes_to_wkb_points,
    doc="Create a geoseries containing the point geometries of a vertex array",
    name="vertexes_to_points",
)
directededges_to_wkb_linestrings = _wrap(_av.directededges_to_wkb_linestrings, ret_type=pd.Series)
directededges_to_linestrings = _geoseries_from_wkb(
    directededges_to_wkb_linestrings,
    doc="Create a geoseries containing the linestrings geometries of a directededge array",
    name="directededges_to_linestrings",
)
wkb_to_cells = _wrap(_av.wkb_to_cells, ret_type=pd.Series)
geometry_to_cells = _wrap(_av.geometry_to_cells, ret_type=pd.Series)


@wraps(wkb_to_cells)
def geoseries_to_cells(geoseries: gpd.GeoSeries, *args, **kw):
    return _av.wkb_to_cells(geoseries.to_wkb(), *args, **kw).to_pandas()


geoseries_to_cells.__name__ = "geoseries_to_cells"


def cells_dataframe_to_geodataframe(
    df: pd.DataFrame, cell_column_name: str = DEFAULT_CELL_COLUMN_NAME
) -> gpd.GeoDataFrame:
    """
    Convert a dataframe with a column containing cells to a geodataframe

    :param df: input dataframe
    :param cell_column_name: name of the column containing the h3 indexes
    :return: GeoDataFrame
    """
    return gpd.GeoDataFrame(df, geometry=cells_to_polygons(df[cell_column_name]), crs=H3_CRS)


def geodataframe_to_cells(
    gdf: gpd.GeoDataFrame,
    resolution: int,
    containment_mode: ContainmentMode = ContainmentMode.ContainsCentroid,
    compact: bool = False,
    cell_column_name: str = DEFAULT_CELL_COLUMN_NAME,
) -> pd.DataFrame:
    """
    Convert a `GeoDataFrame` to H3 cells while exploding all other columns according to the number of cells derived
    from the rows geometry.

    The conversion of GeoDataFrames is parallelized using the available CPUs.

    The duplication of all non-cell columns leads to increased memory requirements. Depending on the use-case
    some of the more low-level conversion functions should be preferred.

    :param gdf:
    :param resolution: H3 resolution
    :param containment_mode: Containment mode used to decide if a cell is contained in a polygon or not.
            See the ContainmentMode class.
    :param compact: Compact the returned cells by replacing cells with their parent cells when all children
            of that cell are part of the set.
    :param cell_column_name:
    :return:
    """
    cells = _av.wkb_to_cells(
        gdf.geometry.to_wkb(),
        resolution,
        containment_mode=containment_mode,
        compact=compact,
        flatten=False,
    )
    table = pa.Table.from_pandas(pd.DataFrame(gdf.drop(columns=gdf.geometry.name))).append_column(
        cell_column_name, cells
    )
    return _arrow_util.explode_table_include_null(table, cell_column_name).to_pandas().reset_index(drop=True)


__all__ = [
    cells_to_coordinates.__name__,
    coordinates_to_cells.__name__,
    cells_bounds.__name__,
    cells_bounds_arrays.__name__,
    cells_to_wkb_polygons.__name__,
    cells_to_polygons.__name__,
    cells_to_wkb_points.__name__,
    cells_to_points.__name__,
    vertexes_to_wkb_points.__name__,
    vertexes_to_points.__name__,
    directededges_to_wkb_linestrings.__name__,
    directededges_to_linestrings.__name__,
    cells_dataframe_to_geodataframe.__name__,
    wkb_to_cells.__name__,
    geometry_to_cells.__name__,
    geoseries_to_cells.__name__,
    geodataframe_to_cells.__name__,
]
