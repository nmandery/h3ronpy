from h3ronpy.pandas import change_resolution, change_resolution_paired, cells_resolution
import numpy as np
import math
import h3.api.numpy_int as h3


def test_change_resolution_up():
    h3indexes = np.array([h3.geo_to_h3(10.2, 45.5, 5), h3.geo_to_h3(10.3, 45.1, 8)], dtype=np.uint64)
    out_res = 9
    changed = change_resolution(h3indexes, out_res)
    assert changed.shape[0] == (int(math.pow(7, 4)) + 7)
    for i in range(len(changed)):
        assert h3.h3_get_resolution(changed[i]) == out_res


def test_change_resolution_paired_up():
    h3indexes = np.array(
        [
            h3.geo_to_h3(10.3, 45.1, 8),
        ],
        dtype=np.uint64,
    )
    out_res = 9
    changed_df = change_resolution_paired(h3indexes, out_res)
    assert len(changed_df) == 7
    for i in range(len(changed_df)):
        assert h3.h3_get_resolution(changed_df["cell_before"][i]) == 8
        assert h3.h3_get_resolution(changed_df["cell_after"][i]) == out_res


def test_change_resolution_down():
    h3indexes = np.array([h3.geo_to_h3(10.2, 45.5, 5), h3.geo_to_h3(10.3, 45.1, 8)], dtype=np.uint64)
    out_res = 4
    changed = change_resolution(h3indexes, out_res)
    assert changed.shape[0] == 2
    assert h3.h3_get_resolution(changed[0]) == out_res
    assert h3.h3_get_resolution(changed[1]) == out_res


def test_cells_resolution():
    h3indexes = np.array([h3.geo_to_h3(10.2, 45.5, 5), h3.geo_to_h3(10.3, 45.1, 8)], dtype=np.uint64)
    res = cells_resolution(h3indexes)
    assert len(res) == 2
    assert res[0] == 5
    assert res[1] == 8
