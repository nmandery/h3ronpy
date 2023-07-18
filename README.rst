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

Documentation is available on `<https://h3ronpy.readthedocs.io/>`_.

Features
--------

* Build on `Apache Arrow <https://arrow.apache.org>`_ and `pyarrow <https://arrow.apache.org/docs/python/index.html>`_ for efficient data handling.
* Dedicated APIs for the the `pandas <https://pandas.pydata.org>`_ and `polars <https://www.pola.rs/>`_ dataframe libraries. The `pandas` support includes `geopandas <https://geopandas.org>`_.
* Multi-threaded conversion of raster data to the H3 grid using `numpy arrays <https://numpy.org/>`_.
* Multi-threaded conversion of vector data, including `geopandas` `GeoDataFrames` and any object which supports the python `__geo_interface__` protocol (`shapely`, `geojson`, ...).

Most parts of this library aim to be well-performing. Benchmarking the conversion of 1000 uint64 cell
values to strings using

* a simplistic list comprehension calling ``h3-py`` ``h3_to_string``
* a numpy vectorized (``numpy.vectorize``) variant of ``h3-py`` ``h3_to_string``
* the ``cells_to_string`` function of this library (release build)

leads to the following result on a standard laptop:

.. code-block::

    ---------------------------------------------------------------------------------------------- benchmark: 3 tests ---------------------------------------------------------------------------------------------
    Name (time in us)                           Min                 Max                Mean            StdDev              Median               IQR            Outliers  OPS (Kops/s)            Rounds  Iterations
    ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    test_cells_to_string                    48.4710 (1.0)       75.5000 (1.0)       52.4252 (1.0)      1.5461 (1.0)       52.0330 (1.0)      0.4890 (1.0)       307;448       19.0748 (1.0)        4090           1
    test_h3_to_string_python_list          290.5460 (5.99)     325.8180 (4.32)     297.5644 (5.68)     4.8769 (3.15)     296.1350 (5.69)     8.2420 (16.85)       806;4        3.3606 (0.18)       2863           1
    test_h3_to_string_numpy_vectorized     352.9870 (7.28)     393.8450 (5.22)     360.1159 (6.87)     3.7195 (2.41)     359.4820 (6.91)     3.8420 (7.86)      447;131        2.7769 (0.15)       2334           1
    ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    Legend:
      Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
      OPS: Operations Per Second, computed as 1 / Mean


The benchmark implementation can be found in ``tests/polars/test_benches.py`` and uses `pytest-benchmark <https://pypi.org/project/pytest-benchmark/>`_.


License
-------

MIT