import polars as pl
from h3ronpy.arrow import cells_parse, cells_to_localij, localij_to_cells
from polars.testing import assert_series_equal

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
    assert df.num_rows == 1

    left = pl.Series(df["anchor"])
    right = pl.Series(anchors)
    assert_series_equal(left, right, check_names=False)
    assert df["i"][0] == 25
    assert df["j"][0] == 13


def test_cells_to_localij_single_anchor():
    df = cells_to_localij(cells, anchors[0])
    assert df.num_rows == 1

    left = pl.Series(df["anchor"])
    right = pl.Series(anchors)
    assert_series_equal(left, right, check_names=False)
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

    left = pl.Series(cells)
    right = pl.Series(cells2)
    assert_series_equal(left, right, check_names=False)
