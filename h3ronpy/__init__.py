"""
h3ron python bindings
"""

# import from rust library
from .h3ronpy import version, Polygon

__version__ = version()

H3_CRS = "EPSG:4326"
