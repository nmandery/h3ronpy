h3ronpy
=======

A data science toolkit for the `H3 geospatial grid <https://h3geo.org/>`_.

.. image:: https://img.shields.io/pypi/v/h3ronpy
    :alt: PyPI
    :target: https://pypi.python.org/pypi/h3ronpy/

.. image:: https://readthedocs.org/projects/h3ronpy/badge/?version=latest
    :alt: ReadTheDocs
    :target: https://h3ronpy.readthedocs.io/


This library is not a substitute for the official `python h3 library <https://github.com/uber/h3-py>`_ - instead it provides more
high-level functions on top of H3 and integrations into common dataframe libraries.

Documentation is available on `<https://h3ronpy.readthedocs.io/>`_.

Features
--------

* H3 algorithms provided using the performant `h3o <https://github.com/HydroniumLabs/h3o>`_ library.
* Build on `Apache Arrow <https://arrow.apache.org>`_ and `pyarrow <https://arrow.apache.org/docs/python/index.html>`_ for efficient data handling.
* Dedicated APIs for the the `pandas <https://pandas.pydata.org>`_ and `polars <https://www.pola.rs/>`_ dataframe libraries. The `pandas` support includes `geopandas <https://geopandas.org>`_.
* Multi-threaded conversion of raster data to the H3 grid using `numpy arrays <https://numpy.org/>`_.
* Multi-threaded conversion of vector data, including `geopandas` `GeoDataFrames` and any object which supports the python `__geo_interface__` protocol (`shapely`, `geojson`, ...).


Limitations
-----------

Not all functionalities of the H3 grid are wrapped by this library, the current feature-set was implemented
when there was a need and the time for it. As a opensource library new features can be requested in the form of github issues
or contributed using pull requests.

License
-------

MIT
