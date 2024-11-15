from h3ronpy.raster import Transform


def test_transform_cmp():
    assert Transform(1, 1, 0, 1, 0, 1) == Transform(1, 1, 0, 1, 0, 1)
    assert Transform(1, 1, 0, 0, 0, 1) != Transform(1, 1, 0, 1, 0, 1)
