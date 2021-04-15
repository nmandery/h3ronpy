import rasterio
from h3ronpy.raster import raster_to_dataframe

from . import TESTDATA_PATH


def test_r_tiff():
    dataset = rasterio.open(TESTDATA_PATH / "r.tiff")
    band = dataset.read(1)
    df = raster_to_dataframe(band, dataset.transform, 8, nodata_value=0, compacted=True, geo=False)
    assert len(df) > 100
    assert df.dtypes["h3index"] == "uint64"
    assert df.dtypes["value"] == "uint8"
