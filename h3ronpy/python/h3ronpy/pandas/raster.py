import geopandas as gpd
import numpy as np
import pandas as pd
import typing

from ..arrow import raster as arrow_raster
from .vector import cells_dataframe_to_geodataframe

__doc__ = arrow_raster.__doc__

nearest_h3_resolution = arrow_raster.nearest_h3_resolution
rasterize_cells = arrow_raster.rasterize_cells


def raster_to_dataframe(
    in_raster: np.array,
    transform,
    h3_resolution: int,
    nodata_value=None,
    axis_order: str = "yx",
    compact: bool = True,
    geo: bool = False,
) -> typing.Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Convert a raster/array to a pandas `DataFrame` containing H3 indexes

    This function is parallelized and uses the available CPUs by distributing tiles to a thread pool.

    The input geometry must be in WGS84.

    :param in_raster: input 2-d array
    :param transform:  the affine transformation
    :param nodata_value: the nodata value. For these cells of the array there will be no h3 indexes generated
    :param axis_order: axis order of the 2d array. Either "xy" or "yx"
    :param h3_resolution: target h3 resolution
    :param compact: Return compacted h3 indexes (see H3 docs). This results in mixed H3 resolutions, but also can
            reduce the amount of required memory.
    :param geo: Return a geopandas `GeoDataFrame` with geometries. increases the memory usage.
    :return: pandas `DataFrame` or `GeoDataFrame`
    """

    df = arrow_raster.raster_to_dataframe(
        in_raster, transform, h3_resolution, nodata_value=nodata_value, axis_order=axis_order, compact=compact
    ).to_pandas()

    if geo:
        return cells_dataframe_to_geodataframe(df)
    else:
        return df


def raster_to_geodataframe(*a, **kw) -> gpd.GeoDataFrame:
    """
    convert to a geodataframe

    Uses the same parameters as array_to_dataframe
    """
    kw["geo"] = True
    return raster_to_dataframe(*a, **kw)
