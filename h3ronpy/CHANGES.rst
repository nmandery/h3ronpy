Changelog
=========

All notable changes to this project will be documented in this file.

The format is loosely based on `Keep a
Changelog <https://keepachangelog.com/en/1.0.0/>`__, and this project
adheres to `Semantic
Versioning <https://semver.org/spec/v2.0.0.html>`__.


Unreleased
----------

- *Breaking*: Remove lots the dataframe-library specific functions and instead work with arrow arrays and record-batches directly using the `Arrow PyCapsule interface <https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html>`_.
  The core and polars parts of this library are no longer depending on `pyarrow` - instead the `lightweight 'arro3' library <https://github.com/kylebarron/arro3>`_ is used. The pandas-parts still require `pyarrow` which needs the be installed manually.
- Support for numpy 2.
- Upgrade to h3o 0.7.
- Release the GIL more often to allow other threads to run.
- The minimum supported python version is now 3.9.

0.21.1 - 2024-10-04
-------------------

- Fixed coordinate messup when converting H3 entities to points (issue #58).

0.21.0 - 2024-07-01
-------------------

- Restrict numpy version to <2 until incompatibilities of rust-numpy with version 2 are resolved
- Add `cells_to_localij` and `localij_to_cells` functions.
- Polars v1.0.0 compatibility.

0.20.2 - 2024-04-16
-------------------

- Loosen sanity-checks for wgs84 bounds. Fixes #48

0.20.1 - 2024-03-01
-------------------

- Upgrade h3o, rasterh3 and h3arrow dependencies. This includes a fix for converting datasets spanning the antimeridian
  by splitting and normalizing tiles before generating cells from them.

0.20.0 - 2024-02-03
-------------------

- Migrate from the unmaintained arrow2 arrow implementation to the official arrow-rs from apache. This comes along
  with a few changes:
  - `directededges_to_wkb_lines` and `directededges_to_lines` have been removed. Use the linestring-versions instead.
  - Geometry collections are currently unsupported when working with WKB. This is still work-in-progress within geoarrow-rs.
- Add support for the h3o containment mode "covers"
- Finally removed the "all_intersecting" parameter in geometry to cells conversion as announced in v0.18

0.19.2 - 2023-11-16
-------------------

- Fix bug which required geopandas geometry columns to be named "geometry" in `geodataframe_to_cells`.
- Warn about possible memory exhaustion when encountering a `ArrowIndexError` in
  `explode_table_include_null` / `geodataframe_to_cells`.

0.19.1 - 2023-10-18
-------------------

- Add missing resolution param for change_resolution in polars namespaces. #38
- Upgrade pyo3 from 0.19 to 0.20

0.19.0 - 2023-10-11
-------------------

- Polars expressions and series shortcuts. #33
- Parse directed edges and vertexes from utf8 strings.
- Extend documentation of ``cells_parse``.
- Add ``change_resolution_list`` #35.

0.18.0 - 2023-08-31
-------------------

- Added ``coordinates_to_cells`` function.
- Added ``rasterize_cells`` function to generate raster arrays from cells.
- Updated h3o from v0.3 to v0.4. Due to the new polyfill modes this leads to API changes in all functions converting
  geometries to cells. The ``all_intersecting`` parameter is now deprecated (will be removed in v0.19) and is replaced
  by ``containment_mode``.

0.17.5 - 2023-08-22
-------------------

- Rework packaging and build process in CI. Adds support for apple silicon. `#23 <https://github.com/nmandery/h3ronpy/issues/23>`_.

0.17.4 - 2023-07-28
-------------------

- Rebuild with h3o 0.3.4 to fix `#25 <https://github.com/nmandery/h3ronpy/issues/25>`_.

0.17.3 - 2023-07-27
-------------------

- Fixed ``maxx`` value returned by ``cells_bounds_arrays`` - a bug caused this to be identical to ``minx``.
- Added ``cells_to_string``, ``vertexes_to_string`` and ``directededges_to_string`` functions to convert to Utf8Array.
- Added more documentation for the ``vector`` modules.

0.17.2 - 2023-07-13
-------------------

- Support arrow2 ``Utf8Array<i64>`` / polars ``LargeUtf8`` in ``cells_parse``. Fixes #24

0.17.1 - 2023-07-06
-------------------

- Improved documentation of the raster modules.
- Validate bounds of input raster arrays to be within WGS84 lat/lon coordinates.
- Make the GeoSeries-returning function show up in the ``h3ronpy.pandas.vector`` module.


0.17.0 - 2023-06-27
-------------------

In this release the project migrated to the `arrow memory model <https://arrow.apache.org/>`_ and switched
from the `h3ron library <https://github.com/nmandery/h3ron>`_ to the Rust implementation of H3 named `h3o <https://github.com/HydroniumLabs/h3o>`_.
This comes along with safer code and `performance improvements <https://github.com/nmandery/rasterh3/issues/1>`_. Although ``h3ronpy`` is no longer
build on the ``h3ron`` rust crate, the name remains.

As a result of these migrations, the Python API has completely changed. Due to this it is easier to refer to new
new documentation than listing the changes here. That is another aspect of these changes - there now exists a sphinx generated
documentation.

Other changes:

-  Upgrade from pyo3 0.18 to 0.19.

0.16.1 - 2023-02-16
-------------------

-  Upgrade from pyo3 0.17 to 0.18.
-  Support minimum supported python version to 3.7 again

0.16.0 - 2022-12-28
--------------------

-  Directly support GeoSeries in vector to H3 conversion by
   automatically exchanging geometries using WKB.
   `#7 <https://github.com/nmandery/h3ronpy/pull/7>`__
-  Raise minimum supported python version to 3.8.
-  ``intersecting`` argument for ``geodataframe_to_h3`` to also include
   cells which are only intersecting with the geometry, but whose
   centroid is not contained in the geometry.

0.15.1 - 2022-10-28
-------------------

-  Upgrade to h3ron v0.16.0.
-  Reduced the durations the GIL is held.

0.15.0 - 2022-09-11
-------------------

-  Dependency upgrades incl. upgrade to H3 v4.0.0 / h3ron v0.15.0.

   -  The migration to H3 v4.0.0 comes with renaming a few functions to
      follow H3 conventions:

      -  In ``h3ronpy.op`` module:

         -  ``kring_distances`` -> ``grid_disk_distances``
         -  ``kring_distances_agg`` -> ``grid_disk_distances_agg``
         -  ``kring_distances_agg_np`` -> ``grid_disk_distances_agg_np``

0.14.0 - 2022-01-29
-------------------

Added
~~~~~

-  Add ``h3ronpy.op.change_resolution`` and
   ``h3ronpy.op.change_resolution_paired``

Changed
~~~~~~~

-  Replace usage of ``wkb`` crate with ``geozero`` because of licensing.
-  Stop supporting Python 3.6 (was EOL on 23 Dec 2021). Minimum
   supported python version now is Python 3.7.
-  Omit empty geometries when converting vector data.
-  Dependency upgrades.

0.13.1 - 2021-12-13
-------------------

.. _added-1:

Added
~~~~~

-  Building wheels for Windows and Mac using github actions.

0.13.0 - 2021-12-10
-------------------

.. _added-2:

Added
~~~~~

-  ``h3ronpy.op.kring_distances`` and
   ``h3ronpy.op.kring_distances_agg``.

.. _changed-1:

Changed
~~~~~~~

-  The python extension has been removed from this repository and moved
   to its own repository at
   `github.com/nmandery/h3ronpy <https://github.com/nmandery/h3ronpy>`__.
-  Upgrade h3ron dependency to 0.13, h3ron-ndarray to 0.13.
-  Upgrade ``pyo3`` and ``rust-numpy`` dependencies to 0.15.
-  Raise ``geopandas`` version requirement from 0.8 to 0.10.

0.12.0 - 2021-08-10
-------------------

.. _changed-2:

Changed
~~~~~~~

-  dependency updates

0.11.0 - 2021-06-12
-------------------

.. _added-3:

Added
~~~~~

-  Support for transforming ``numpy.float32`` and ``numpy.float64``
   raster arrays to H3 dataframes by warping the array values in
   ``OrderedFloat<T>``.

.. _changed-3:

Changed
~~~~~~~

-  Fix ``ValueError`` when converting empty dataframes.
   `#17 <https://github.com/nmandery/h3ron/issues/17>`__
-  Deprecate ``h3ronpy.util.h3index_column_to_geodataframe`` in favor of
   ``h3ronpy.util.dataframe_to_geodataframe``.
-  Update dependencies: ``geo-types`` 0.6->0.7, ``ndarray`` 0.14->0.15


0.10.0 - 2021-04-24
-------------------

.. _added-4:

Added
~~~~~

-  Unittests for ``raster_to_dataframe`` and ``geodataframe_to_h3``
   using ``pytest``

.. _changed-4:

Changed
~~~~~~~

Removed
~~~~~~~

0.9.0 - 2021-04-11
------------------

.. _added-5:

Added
~~~~~

-  Integration with geopandas ``GeoDataFrame`` to convert the contained
   geometries to H3.
-  Update of ``maturin`` to 0.10.2

.. _changed-5:

Changed
~~~~~~~

-  Simplified API of raster integration.

Earlier versions
----------------

The changes done in earlier versions where not documented in this
changelog and can only be reconstructed from the commits in git.
