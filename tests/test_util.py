import pandas as pd

from h3ronpy.util import dataframe_to_geodataframe


def test_empty_to_geo():
    # https://github.com/nmandery/h3ron/issues/17
    df = pd.DataFrame({"h3index": []})
    gdf = dataframe_to_geodataframe(df)  # should not raise an ValueError.
    assert gdf.empty
