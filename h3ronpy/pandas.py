import h3.api.numpy_int as h3
import geopandas as gp
import numpy as np
from shapely.geometry import Polygon

def h3indexes_to_geodataframe(h3indexes):
    """convert h3 indexes to a geodataframe"""
    i_arr = np.asarray(h3indexes)
    return gp.GeoDataFrame({
        "geometry": [Polygon(h3.h3_to_geo_boundary(h, geo_json=True)) for h in np.nditer(i_arr)],
        "h3resolution": [h3.h3_get_resolution(h) for h in np.nditer(i_arr)],
        "h3index": [h3.h3_to_string(h) for h in np.nditer(i_arr)]
    }, crs="EPSG:4326")
