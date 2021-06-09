from typing import Tuple, Generator

import geopandas as gpd
import math
import numpy as np
import pandas as pd

from .h3ronpy import vector


def geometries_to_h3_generator(geometries: np.array, ids: np.array, h3_resolution: int, do_compact: bool = False,
                               chunk_size: int = 1000) -> Generator[Tuple[np.array, np.array], None, None]:
    """
    Generator to convert shapely geometries and ids to two numpy arrays with h3indexes and the correlated Ids.
    Yields (ids, h3indexes)-tuples.

    This function is parallelized and uses the available CPUs by distributing the geometries to a thread pool.

    :param geometries: numpy-array with shapely geometries (in WGS84).
    :param ids: numpy array with ids. must be unit64
    :param h3_resolution: H3 resolution to use.
    :param do_compact: Compacts the h3index when this is set to `True`. Default is `False`
    :param chunk_size: Number of geometries to include in the yielded chunks
    :return: None
    """
    num_chunks = math.ceil(len(geometries) / chunk_size)
    for (chunk_geometries, chunk_ids) in zip(np.array_split(geometries, num_chunks),
                                             np.array_split(ids, num_chunks)):
        chunk_wkb_list = [item.wkb for item in chunk_geometries]

        (ids, h3indexes) = vector.wkbbytes_with_ids_to_h3(chunk_ids, chunk_wkb_list, h3_resolution, do_compact)
        yield ids, h3indexes


def geodataframe_to_h3(df: gpd.GeoDataFrame, h3_resolution: int, do_compact: bool = False, geometry_column="geometry",
                       index_column_name="to_h3_idx", chunk_size=1000):
    """
    convert the geometries of a geodataframe to h3 indexes.

    This function is parallelized and uses the available CPUs by distributing the geometries to a thread pool.

    The geometry column will be removed from the resulting dataframe. The input geometry must be in WGS84.

    :param df: The input geodataframe
    :param h3_resolution: H3 resolution to use.
    :param do_compact: Compacts the h3index when this is set to `True`. Default is `False`
    :param geometry_column: The name of the column containing the geometry. Defaults to `geometry`.
    :param index_column_name: The name for a temporary column used to join the H3 data to the input dataframe
    :param chunk_size:
    :return:
    """
    # add a column with a sequence to merge later with
    df.insert(loc=0, column=index_column_name, value=np.arange(len(df), dtype=np.uint64))
    dataframes = []
    for (ids, h3indexes) in geometries_to_h3_generator(df[geometry_column].to_numpy(), df[index_column_name].to_numpy(),
                                                       h3_resolution,
                                                       do_compact=do_compact, chunk_size=1000):
        dataframes.append(pd.DataFrame({
            index_column_name: ids,
            "h3index": h3indexes
        }))

    if not dataframes:
        return pd.DataFrame({})
    output_df = pd.DataFrame(df.drop(columns=geometry_column)) \
        .merge(pd.concat(dataframes), on=index_column_name)

    # remove the column used for the merge again
    del output_df[index_column_name]

    return output_df
