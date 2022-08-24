import typing

import numpy as np
import pandas as pd

from .h3ronpy import op as native_op


def grid_disk_distances(h3indexes: np.ndarray, k_max: int, k_min: int = 0) -> pd.DataFrame:
    """
    Vectorized grid-disk building

    Returns a dataframe with the columns `h3index` (the ring center), `ring_h3index` and `ring_k`.

    :param h3indexes:
    :param k_max:
    :param k_min:
    :return:
    """
    h3index, ring_h3index, ring_k = native_op.grid_disk_distances(h3indexes, k_min, k_max)
    return pd.DataFrame({
        "h3index": h3index,
        "ring_h3index": ring_h3index,
        "ring_k": ring_k
    })


def grid_disk_distances_agg_np(h3indexes: np.ndarray, k_max: int, k_min: int = 0, aggregation_method: str = 'min') -> \
        typing.Tuple[np.ndarray, np.ndarray]:
    """
    Vectorized grid-disk building, with the k-values of the rings being aggregated to their `min` or
    `max` value for each cell.

    :param h3indexes:
    :param k_max:
    :param k_min:
    :param aggregation_method:
    :return:
    """
    return native_op.grid_disk_distances_agg(h3indexes, k_min, k_max, aggregation_method)


def grid_disk_distances_agg(h3indexes: np.ndarray, k_max: int, k_min: int = 0,
                            aggregation_method: str = 'min') -> pd.DataFrame:
    h3indexes_out, k_out = grid_disk_distances_agg_np(h3indexes, k_max, k_min=k_min, aggregation_method=aggregation_method)
    return pd.DataFrame({"h3index": h3indexes_out, "k": k_out})


def change_resolution(h3indexes: np.ndarray, h3_resolution: int) -> np.ndarray:
    """Change the resolution of the given `h3indexes` to `h3_resolution`"""
    return native_op.change_resolution(h3indexes, h3_resolution)


def change_resolution_paired(h3indexes: np.ndarray, h3_resolution: int) -> pd.DataFrame:
    """Change the resolution of the given `h3indexes` to `h3_resolution`

    Returns a dataframe with a column for the `h3index_before` and a `h3index_after` the
    resolution change.
    """
    h3index_before, h3index_after = native_op.change_resolution_paired(h3indexes, h3_resolution)
    return pd.DataFrame({
        "h3index_before": h3index_before,
        "h3index_after": h3index_after,
    })
