from h3ronpy.polars import cells_to_localij, cells_parse, localij_to_cells
from polars.testing import assert_series_equal
import polars as pl

anchors = cells_parse(
    [
        "85283473fffffff",
    ]
)
cells = cells_parse(
    [
        "8528342bfffffff",
    ]
)


def test_cells_to_localij_array():
    df = cells_to_localij(cells, anchors)
    assert len(df) == 1
    assert_series_equal(df["anchor"], anchors, check_names=False)
    assert df["i"][0] == 25
    assert df["j"][0] == 13


def test_cells_to_localij_single_anchor():
    df = cells_to_localij(cells, anchors[0])
    assert len(df) == 1
    assert_series_equal(df["anchor"], anchors, check_names=False)
    assert df["i"][0] == 25
    assert df["j"][0] == 13


def test_localij_to_cells():
    cells2 = localij_to_cells(
        anchors,
        pl.Series(
            [
                25,
            ],
            dtype=pl.Int32(),
        ),
        pl.Series(
            [
                13,
            ],
            dtype=pl.Int32(),
        ),
    )
    assert_series_equal(cells, cells2, check_names=False)
