h3ronpy
=======

A data science toolkit for the `H3 geospatial grid <https://h3geo.org/>`_.

.. image:: https://img.shields.io/pypi/v/h3ronpy
    :alt: PyPI
    :target: https://pypi.python.org/pypi/h3ronpy/

.. image:: https://readthedocs.org/projects/h3ronpy/badge/?version=latest
    :alt: ReadTheDocs
    :target: https://h3ronpy.readthedocs.io/

.. image:: https://img.shields.io/conda/vn/conda-forge/h3ronpy.svg
    :alt: conda-forge
    :target: https://prefix.dev/channels/conda-forge/packages/h3ronpy


This library is not a substitute for the official `python h3 library <https://github.com/uber/h3-py>`_ - instead it provides more
high-level functions on top of H3 and integrations into common dataframe libraries.

Documentation is available on `<https://h3ronpy.readthedocs.io/>`_.

Looking for maintainers
-----------------------

My personal focus has shifted quite a bit since I started this project. Currently I am not working much with H3 any more,
so I have very little time to spend on this library, or even to keep up with standard maintenance tasks like upgrades
or answering pull requests and issues. To keep this project somewhat alive, additional maintainers would be very welcome.

Features
--------

* H3 algorithms provided using the performant `h3o <https://github.com/HydroniumLabs/h3o>`_ library.
* Build on `Apache Arrow <https://arrow.apache.org>`_ and the lightweight `arro3 <https://github.com/kylebarron/arro3>`_ for efficient data handling. The arrow memory model is compatible with dataframe libraries like `pandas <https://pandas.pydata.org>`_ and `polars <https://www.pola.rs/>`_.
* Extensions for the polars `Series`` and  `Expr` APIs.
* Some dedicated functions to work with `geopandas <https://geopandas.org>`_ `GeoSeries`.
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
