from . import h3ronpyrs as _native
from .h3ronpyrs import version, DEFAULT_CELL_COLUMN_NAME, ContainmentMode  # noqa: F401

__version__ = version()

H3_CRS = "EPSG:4326"

if not _native.is_release_build():
    import warnings

    warnings.warn("h3ronpy has not been compiled in release mode. Performance will be degraded.", RuntimeWarning)


__all__ = [
    "H3_CRS",
    "DEFAULT_CELL_COLUMN_NAME",
    ContainmentMode.__name__,
    version.__name__,
]
