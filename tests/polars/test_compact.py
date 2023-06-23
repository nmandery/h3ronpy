import pytest

from h3ronpy.polars import compact, change_resolution, uncompact
import numpy as np
import h3.api.numpy_int as h3


def compact_to_one(expected_cell, input_cells, **kw):
    compacted = compact(input_cells, **kw)
    assert len(compacted) == 1
    assert compacted[0] == expected_cell


def test_compact():
    cell = h3.geo_to_h3(10.3, 45.1, 8)
    h3indexes = change_resolution(
        np.array(
            [
                cell,
            ],
            dtype=np.uint64,
        ),
        10,
    )
    compact_to_one(cell, h3indexes)


def test_compact_mixed_fail():
    cell = h3.geo_to_h3(10.3, 45.1, 8)
    with pytest.raises(ValueError, match="heterogen"):
        compact_to_one(cell, [cell, h3.h3_to_parent(cell, 4)])


def test_compact_mixed():
    cell = h3.geo_to_h3(10.3, 45.1, 8)
    compact_to_one(cell, [cell, h3.geo_to_h3(10.3, 45.1, 9)], mixed_resolutions=True)


def test_uncompact():
    cells = uncompact(
        [
            h3.geo_to_h3(10.3, 45.1, 8),
        ],
        9,
    )
    assert len(cells) == 7
