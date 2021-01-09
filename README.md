# the `h3ronpy` python extension

... exposes some functionalities of the `h3ron*` rust crates to the python language and integrates with the [numpy](https://numpy.org/), [pandas](https://pandas.pydata.org/),
[geopandas](https://geopandas.org/), [rasterio](https://rasterio.readthedocs.io/en/latest/) and [gdal](https://gdal.org/) libraries. One goal is to not duplicate any
functions already implemented by the [official H3 python bindings](https://github.com/uber/h3-py).

This library is in parts parallelized using [rayon](https://github.com/rayon-rs/rayon). The number of threads can be controlled as 
described in [the rayon FAQ](https://github.com/rayon-rs/rayon/blob/master/FAQ.md#how-many-threads-will-rayon-spawn)

## Usage

See the included jupyter notebook [documentation.ipynb](./documentation.ipynb). The notebook requires [these dependencies](requirements.documentation.txt) to run.

## Logging

This library uses rusts [`log` crate](https://docs.rs/log) together with the [`env_logger` crate](https://docs.rs/env_logger). 
This means that logging to `stdout` can be  controlled via environment variables. Set `RUST_LOG` to `debug`, `error`, `info`, 
`warn`, or `trace` for the corresponding log output.

For more fine-grained logging settings, see the documentation of `env_logger`.

## Build using [maturin](https://github.com/PyO3/maturin):

There are three main commands:

* `maturin publish` builds the crate into python packages and publishes them to pypi.
* `maturin build` builds the wheels and stores them in a folder (`../target/wheels` by default), but doesn't upload them. It's possible to upload those with [twine](https://github.com/pypa/twine).
* `maturin develop` builds the crate and installs it as a python module directly in the current virtualenv.
