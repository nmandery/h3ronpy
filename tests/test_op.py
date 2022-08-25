import h3.api.numpy_int as h3
import numpy as np
import math

from h3ronpy.op import grid_disk_distances_agg, grid_disk_distances, change_resolution, change_resolution_paired


def test_kring_distances_agg():
    h3indexes = np.array([h3.geo_to_h3(10.2, 45.5, 8), h3.geo_to_h3(10.3, 45.1, 8)], dtype=np.uint64)
    k_max = 4
    df = grid_disk_distances_agg(h3indexes, k_max, aggregation_method='min')
    assert len(df) > 100
    assert df['k'].min() == 0
    assert df['k'].max() == k_max
    assert len(np.unique(df["h3index"])) == len(df)


def test_kring_distances():
    h3indexes = np.array([h3.geo_to_h3(10.2, 45.5, 8), h3.geo_to_h3(10.3, 45.1, 8)], dtype=np.uint64)
    k_max = 4
    k_min = 2
    df = grid_disk_distances(h3indexes, 4, k_min=k_min)
    assert len(df) > 100
    assert df['ring_k'].min() == k_min
    assert df['ring_k'].max() == k_max
    assert 'h3index' in df
    assert 'ring_h3index' in df


def test_change_resolution_down():
    h3indexes = np.array([h3.geo_to_h3(10.2, 45.5, 5), h3.geo_to_h3(10.3, 45.1, 8)], dtype=np.uint64)
    out_res = 4
    changed = change_resolution(h3indexes, out_res)
    assert changed.shape[0] == 2
    assert h3.h3_get_resolution(changed[0]) == out_res
    assert h3.h3_get_resolution(changed[1]) == out_res


def test_change_resolution_up():
    h3indexes = np.array([h3.geo_to_h3(10.2, 45.5, 5), h3.geo_to_h3(10.3, 45.1, 8)], dtype=np.uint64)
    out_res = 9
    changed = change_resolution(h3indexes, out_res)
    assert changed.shape[0] == (int(math.pow(7, 4)) + 7)
    for i in range(len(changed)):
        assert h3.h3_get_resolution(changed[i]) == out_res


def test_change_resolution_paired_up():
    h3indexes = np.array([h3.geo_to_h3(10.3, 45.1, 8),], dtype=np.uint64)
    out_res = 9
    changed_df = change_resolution_paired(h3indexes, out_res)
    assert len(changed_df) == 7
    for i in range(len(changed_df)):
        assert h3.h3_get_resolution(changed_df["h3index_before"][i]) == 8
        assert h3.h3_get_resolution(changed_df["h3index_after"][i]) == out_res
