from functools import wraps
from typing import Optional

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

import h3ronpy.vector as _hv
from h3ronpy import DEFAULT_CELL_COLUMN_NAME, H3_CRS, ContainmentMode


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


cells_to_polygons = _geoseries_from_wkb(
    _hv.cells_to_wkb_polygons,
    doc="Create a geoseries containing the polygon geometries of a cell array",
    name="cells_to_polygons",
)
cells_to_points = _geoseries_from_wkb(
    _hv.cells_to_wkb_points,
    doc="Create a geoseries containing the centroid point geometries of a cell array",
    name="cells_to_points",
)
vertexes_to_points = _geoseries_from_wkb(
    _hv.vertexes_to_wkb_points,
    doc="Create a geoseries containing the point geometries of a vertex array",
    name="vertexes_to_points",
)
directededges_to_linestrings = _geoseries_from_wkb(
    _hv.directededges_to_wkb_linestrings,
    doc="Create a geoseries containing the linestrings geometries of a directededge array",
    name="directededges_to_linestrings",
)


@wraps(_hv.wkb_to_cells)
def geoseries_to_cells(geoseries: gpd.GeoSeries, *args, **kw):
    return pa.array(_hv.wkb_to_cells(geoseries.to_wkb(), *args, **kw)).to_pandas()


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
    # wkb_polygons = uv.cells_to_wkb_polygons(df[cell_column_name])
    # geometry = shapely.from_wkb(wkb_polygons)
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
    cells = _hv.wkb_to_cells(
        gdf.geometry.to_wkb(),
        resolution,
        containment_mode=containment_mode,
        compact=compact,
        flatten=False,
    )
    table = pa.Table.from_pandas(pd.DataFrame(gdf.drop(columns=gdf.geometry.name))).append_column(
        cell_column_name, cells
    )
    return _explode_table_include_null(table, cell_column_name).to_pandas().reset_index(drop=True)


# from https://issues.apache.org/jira/browse/ARROW-12099
def _explode_table_include_null(table: pa.Table, column: str) -> pa.Table:
    other_columns = list(table.schema.names)
    other_columns.remove(column)
    indices = pc.list_parent_indices(pc.fill_null(table[column], [None]))
    result = table.select(other_columns)
    try:
        # may result in a large memory allocation
        result = result.take(indices)
    except pa.ArrowIndexError:
        # See https://github.com/nmandery/h3ronpy/issues/40
        # Using RuntimeWarning as ResourceWarning is often not displayed to the user.
        import warnings

        warnings.warn(
            "This ArrowIndexError may be a sign of the process running out of memory.",
            RuntimeWarning,
        )
        raise
    result = result.append_column(
        pa.field(column, table.schema.field(column).type.value_type),
        pc.list_flatten(pc.fill_null(table[column], [None])),
    )
    return result


__all__ = [
    cells_dataframe_to_geodataframe.__name__,
    geodataframe_to_cells.__name__,
    cells_to_polygons.__name__,
    cells_to_points.__name__,
    vertexes_to_points.__name__,
    directededges_to_linestrings.__name__,
]
