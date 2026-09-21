Installation
============

.. note::

    To avoid pulling in unused dependencies, `h3ronpy` does not declare a dependency to `pandas`,
    `geopandas` and `polars`. These packages need to be installed separately.


Requirements
------------

`h3ronpy` requires **Python 3.12 or newer** (3.12, 3.13 and 3.14 are tested) and **numpy 2.0 or newer**.
The native extension is built against the numpy 2 ABI and the CPython 3.12 stable ABI (``abi3``), so it will not
install or import on older Python or numpy 1.x releases.

When using the optional integrations, mind their version requirements as well:

- the ``pandas`` / ``geopandas`` integration requires **pyarrow >= 24** (older releases are built against the numpy 1.x
  ABI and cannot be imported under numpy 2)
- the test suite additionally requires **Shapely >= 2.0**


From PyPi
---------

.. code-block:: shell

   pip install h3ronpy


From conda-forge
----------------

See `h3ronpy-feedstock <https://github.com/conda-forge/h3ronpy-feedstock>`_.


From source
-----------

To build from source a recent version of the `rust language <https://www.rust-lang.org/>`_ is required. The easiest
way to install is by using `rustup <https://rustup.rs/>`_.

An recent version of `pip` is required - version 23.1.2 works. `pip` can be upgraded by running

.. code-block:: shell

    pip install --upgrade pip


After that, everything is set up to build and install `h3ronpy`:

.. code-block:: shell

    git clone https://github.com/nmandery/h3ronpy.git
    cd h3ronpy
    pip install .

This will build the rust code using `maturin <https://www.maturin.rs/>`_. For more information on this see its website.

When encountering a circular import error after this installation procedure, just change the directory out of the
h3ronpy source directory.
