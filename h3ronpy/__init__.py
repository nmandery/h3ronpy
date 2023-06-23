from . import h3ronpy as _native
from .h3ronpy import version, DEFAULT_CELL_COLUMN_NAME  # noqa: F401

__version__ = version()

H3_CRS = "EPSG:4326"

if not _native.is_release_build():
    import warnings

    warnings.warn("h3ronpy has not been compiled in release mode. Performance will be degraded.", RuntimeWarning)
