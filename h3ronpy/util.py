import warnings
from typing import Union, List, Iterable

import geopandas as gpd
import h3.api.numpy_int as h3
import numpy as np
import pandas as pd
from shapely.geometry import Polygon

from . import H3_CRS


def h3indexes_to_geodataframe(
        h3indexes: Union[np.array, List[int], Iterable[int]]) -> gpd.GeoDataFrame:
    """
    convert a numpy-array with uint64 h3 indexes to a geodataframe
    """
    i_arr = np.asarray(h3indexes)
    return gpd.GeoDataFrame({
        "geometry": [Polygon(h3.h3_to_geo_boundary(h, geo_json=True)) for h in _array_nditer(i_arr)],
        "h3resolution": [h3.h3_get_resolution(h) for h in _array_nditer(i_arr)],
        "h3index": [h3.h3_to_string(h) for h in _array_nditer(i_arr)]
    }, crs=H3_CRS)


def h3index_column_to_geodataframe(df: pd.DataFrame, column_name: str = "h3index") -> gpd.GeoDataFrame:
    """
    convert a dataframe with a column containing h3indexes to a geodataframe

    :param df: input dataframe
    :param column_name: name of the column containing the h3 indexes
    :return: GeoDataFrame
    """
    warnings.warn("h3index_column_to_geodataframe has been deprecated in favor of dataframe_to_geodataframe",
                  DeprecationWarning)
    return dataframe_to_geodataframe(df, column_name=column_name)


def dataframe_to_geodataframe(df: pd.DataFrame, column_name: str = "h3index") -> gpd.GeoDataFrame:
    """
    convert a dataframe with a column containing h3indexes to a geodataframe

    :param df: input dataframe
    :param column_name: name of the column containing the h3 indexes
    :return: GeoDataFrame
    """
    return gpd.GeoDataFrame(df,
                            geometry=[Polygon(h3.h3_to_geo_boundary(h, geo_json=True)) for h in
                                      _array_nditer(df[column_name].to_numpy())],
                            crs=H3_CRS)


def _array_nditer(a: np.array):
    return np.nditer(a, flags=["zerosize_ok", ])
