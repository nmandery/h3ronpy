import pytest

from h3ronpy.polars import cells_parse, cells_valid, cells_to_string
import numpy as np
import h3.api.numpy_int as h3
import polars as pl
from arro3.core import DataType
import pyarrow as pa


def test_cells_parse():
    strings = np.array([h3.h3_to_string(h3.geo_to_h3(45.5, 10.2, 5)), "10.2, 45.5, 5"])
    cells = cells_parse(strings)
    assert len(cells) == 2
    assert cells[0] == cells[1]


def test_cells_parse_largeutf8():
    # polars uses LargeUtf8 datatype for strings
    cells = cells_parse(pl.Series(["801ffffffffffff"]))
    assert len(cells) == 1


def test_parse_cell_fail():
    strings = np.array(
        [
            "invalid",
        ]
    )
    with pytest.raises(ValueError, match="non-parsable CellIndex"):
        cells_parse(strings)


def test_parse_cell_set_invalid():
    strings = np.array(
        [
            "invalid",
        ]
    )
    cells = cells_parse(strings, set_failing_to_invalid=True)
    assert len(cells) == 1
    assert not cells[0].is_valid


def test_cells_valid():
    input = np.array(
        [45, h3.geo_to_h3(45.5, 10.2, 5)],
        dtype=np.uint64,
    )
    cells = cells_valid(input, booleanarray=False)
    assert len(cells) == 2
    assert cells.type == pa.uint64()
    assert not cells[0].is_valid
    assert cells[1].is_valid

    bools = cells_valid(input, booleanarray=True)
    assert len(bools) == 2
    assert bools.type == pa.bool_()
    assert bools[0].as_py() is False
    assert bools[1].as_py() is True

    assert pa.array(cells).is_valid() == pa.array(bools)


def test_cells_to_string():
    cells = np.array(
        [
            h3.geo_to_h3(45.5, 10.2, 5),
        ]
    )
    strings = cells_to_string(cells)
    assert len(strings) == len(cells)
    assert isinstance(strings, pl.Series)
    assert strings.dtype == pl.Utf8
    assert strings[0] == "851f9923fffffff"
