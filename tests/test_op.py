import h3.api.numpy_int as h3
import numpy as np

from h3ronpy.op import kring_distances_agg, kring_distances


def test_kring_distances_agg():
    h3indexes = np.array([h3.geo_to_h3(10.2, 45.5, 8), h3.geo_to_h3(10.3, 45.1, 8)], dtype=np.uint64)
    k_max = 4
    df = kring_distances_agg(h3indexes, k_max, aggregation_method='min')
    assert len(df) > 100
    assert df['k'].min() == 0
    assert df['k'].max() == k_max
    assert len(np.unique(df["h3index"])) == len(df)


def test_kring_distances():
    h3indexes = np.array([h3.geo_to_h3(10.2, 45.5, 8), h3.geo_to_h3(10.3, 45.1, 8)], dtype=np.uint64)
    k_max = 4
    k_min = 2
    df = kring_distances(h3indexes, 4, k_min=k_min)
    assert len(df) > 100
    assert df['ring_k'].min() == k_min
    assert df['ring_k'].max() == k_max
    assert 'h3index' in df
    assert 'ring_h3index' in df
