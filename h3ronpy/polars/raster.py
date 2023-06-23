from ..arrow import raster as arrow_raster
from . import _wrap
import polars as pl

nearest_h3_resolution = arrow_raster.nearest_h3_resolution
raster_to_dataframe = _wrap(arrow_raster.raster_to_dataframe, ret_type=pl.DataFrame)

__doc__ = arrow_raster.__doc__

__all__ = [nearest_h3_resolution.__name__, raster_to_dataframe.__name__]
