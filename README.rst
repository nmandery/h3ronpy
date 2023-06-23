h3ronpy
=======

A data science toolkit for the `H3 geospatial grid <https://h3geo.org/>`_.

.. image:: https://img.shields.io/pypi/v/h3ronpy
    :alt: PyPI
    :target: https://pypi.python.org/pypi/h3ronpy/

.. image:: https://readthedocs.org/projects/h3ronpy/badge/?version=latest
    :alt: ReadTheDocs
    :target: https://h3ronpy.readthedocs.io/

.. image:: https://zenodo.org/badge/402118389.svg
    :alt: DOI
    :target: https://zenodo.org/badge/latestdoi/402118389


This library is not a substitute for the official `python h3 library <https://github.com/uber/h3-py>`_ - instead it provides more
high-level functions on top of H3 and integrations into common dataframe libraries.

Features
--------

* Build on `Apache Arrow <https://arrow.apache.org>`_ and `pyarrow <https://arrow.apache.org/docs/python/index.html>`_ for efficient data handling.
* Dedicated APIs for the the `pandas <https://pandas.pydata.org>`_ and `polars <https://www.pola.rs/>`_ dataframe libraries. The `pandas` support includes `geopandas <https://geopandas.org>`_.
* Multi-threaded conversion of raster data to the H3 grid using `numpy arrays <https://numpy.org/>`_.
* Multi-threaded conversion of vector data, including `geopandas` `GeoDataFrames` and any object which supports the python `__geo_interface__` protocol (`shapely`, `geojson`, ...).


Documentation is available on `<https://h3ronpy.readthedocs.io/>`_.


License
-------

MIT