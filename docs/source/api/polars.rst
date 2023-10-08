Polars
======


Functions
---------

.. automodule:: h3ronpy.polars
   :members:
   :undoc-members:
   :exclude-members: H3SeriesShortcuts, H3Expr


Polars API extensions
---------------------

Polars itself provides `multiple ways to extend its API <https://pola-rs.github.io/polars/py-polars/html/reference/api.html>`_ - h3ronpy makes
use of this to provide custom extensions for ``Series`` and ``Expr`` types in the ``h3`` namespace.

To make these extensions available, the ``h3ronpy.polars`` module needs to be imported.

Expressions
^^^^^^^^^^^

Example:

.. jupyter-execute::

    import polars as pl
    # to register extension functions in the polars API
    import h3ronpy.polars

    df = pl.DataFrame({
        "cell": ["8852dc41cbfffff", "8852dc41bbfffff"],
        "value": ["a", "b"]
    })

    (df.lazy()
        .select([
            pl.col("cell")
                .h3.cells_parse()
                .h3.grid_disk(2)
                .alias("disk"),
            pl.col("value")
        ])

        .groupby("value")
        .agg([
            pl.col("disk")
                .explode()
                .h3.cells_area_km2()
                .sum()
        ])
        .collect()
    )

All methods of the ``H3Expr`` class are available in the ``h3`` object of a polars ``Expr``:

.. autoclass:: h3ronpy.polars.H3Expr
   :members:
   :undoc-members:

Series
^^^^^^

Example:

.. jupyter-execute::

    import polars as pl
    # to register extension functions in the polars API
    import h3ronpy.polars

    cell_strings = (pl.Series("cells", ["8852dc41cbfffff"])
        .h3.cells_parse()
        .h3.grid_disk(1)
        .explode()
        .sort()
        .h3.cells_to_string())

    cell_strings


All methods of the ``H3SeriesShortcuts`` class are available in the ``h3`` object of a polars ``Series``:

.. autoclass:: h3ronpy.polars.H3SeriesShortcuts
   :members:
   :undoc-members:

Raster module
-------------

.. automodule:: h3ronpy.polars.raster
   :members:
   :undoc-members:


Vector module
-------------

.. automodule:: h3ronpy.polars.vector
   :members:
   :undoc-members:
