Installation
============

.. note::

    To avoid pulling in unused dependencies, `h3ronpy` does not declare a dependency to `pandas`,
    `geopandas` and `polars`. These packages need to be installed separately.


From PyPi
---------

.. code-block:: shell

   pip install h3ronpy


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
