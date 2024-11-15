import math

import h3.api.numpy_int as h3
import numpy as np
from h3ronpy.arrow import cells_resolution, change_resolution, change_resolution_paired


def test_change_resolution_up():
    h3indexes = np.array([h3.geo_to_h3(10.2, 45.5, 5), h3.geo_to_h3(10.3, 45.1, 8)], dtype=np.uint64)
    out_res = 9
    changed = change_resolution(h3indexes, out_res)
    assert len(changed) == (int(math.pow(7, 4)) + 7)
    for i in range(len(changed)):
        assert h3.h3_get_resolution(changed[i].as_py()) == out_res


def test_change_resolution_paired_up():
    h3indexes = np.array(
        [
            h3.geo_to_h3(10.3, 45.1, 8),
        ],
        dtype=np.uint64,
    )
    out_res = 9
    changed_df = change_resolution_paired(h3indexes, out_res)
    assert changed_df.num_rows == 7
    for i in range(changed_df.num_rows):
        assert h3.h3_get_resolution(changed_df["cell_before"][i].as_py()) == 8
        assert h3.h3_get_resolution(changed_df["cell_after"][i].as_py()) == out_res


def test_change_resolution_down():
    h3indexes = np.array([h3.geo_to_h3(10.2, 45.5, 5), h3.geo_to_h3(10.3, 45.1, 8)], dtype=np.uint64)
    out_res = 4
    changed = change_resolution(h3indexes, out_res)
    assert len(changed) == 2
    assert h3.h3_get_resolution(changed[0].as_py()) == out_res
    assert h3.h3_get_resolution(changed[1].as_py()) == out_res


def test_cells_resolution():
    h3indexes = np.array([h3.geo_to_h3(10.2, 45.5, 5), h3.geo_to_h3(10.3, 45.1, 8)], dtype=np.uint64)
    res = cells_resolution(h3indexes)
    assert len(res) == 2
    assert res[0] == 5
    assert res[1] == 8
