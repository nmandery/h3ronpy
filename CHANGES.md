# Changelog

All notable changes to this project will be documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## h3ronpy [Unreleased]

## h3ronpy [0.15.0] - 2022-09-11

- Dependency upgrades incl. upgrade to H3 v4.0.0 / h3ron v0.15.0.
  - The migration to H3 v4.0.0 comes with renaming a few functions to follow H3 conventions:
    - In `h3ronpy.op` module:
      - `kring_distances` -> `grid_disk_distances`
      - `kring_distances_agg` -> `grid_disk_distances_agg`
      - `kring_distances_agg_np` -> `grid_disk_distances_agg_np`
     
## h3ronpy [0.14.0] - 2022-01-29

### Added
- Add `h3ronpy.op.change_resolution` and `h3ronpy.op.change_resolution_paired`

### Changed
- Replace usage of `wkb` crate with `geozero` because of licensing.
- Stop supporting Python 3.6 (was EOL on 23 Dec 2021). Minimum supported python version now is Python 3.7.
- Omit empty geometries when converting vector data.
- Dependency upgrades.

## h3ronpy [0.13.1] - 2021-12-13

### Added 
- Building wheels for Windows and Mac using github actions.

## h3ronpy [0.13.0] - 2021-12-10
### Added
- `h3ronpy.op.kring_distances` and `h3ronpy.op.kring_distances_agg`.

### Changed
- The python extension has been removed from this repository and moved to its own repository at
  [github.com/nmandery/h3ronpy](https://github.com/nmandery/h3ronpy).
- Upgrade h3ron dependency to 0.13, h3ron-ndarray to 0.13.
- Upgrade `pyo3` and `rust-numpy` dependencies to 0.15.
- Raise `geopandas` version requirement from 0.8 to 0.10.

## h3ronpy [0.12.0] - 2021-08-10
### Changed
- dependency updates

## h3ronpy [0.11.0] - 2021-06-12
### Added
- Support for transforming `numpy.float32` and `numpy.float64` raster arrays to H3 dataframes by warping the array values in `OrderedFloat<T>`.

### Changed
- Fix `ValueError` when converting empty dataframes. [#17](https://github.com/nmandery/h3ron/issues/17)
- Deprecate `h3ronpy.util.h3index_column_to_geodataframe` in favor of `h3ronpy.util.dataframe_to_geodataframe`.
- Update dependencies: `geo-types` 0.6->0.7, `ndarray` 0.14->0.15

## h3ronpy [0.10.0] - 2021-04-24
### Added
- Unittests for `raster_to_dataframe` and `geodataframe_to_h3` using `pytest`

### Changed
### Removed

## h3ronpy [0.9.0] - 2021-04-11
### Added
- Integration with geopandas `GeoDataFrame` to convert the contained geometries to H3.
- Update of `maturin` to 0.10.2

### Changed
- Simplified API of raster integration.

## Earlier versions

The changes done in earlier versions where not documented in this changelog and can only be reconstructed from the
commits in git.

[Unreleased]: https://github.com/nmandery/h3ronpy/compare/v0.12.0...HEAD
[0.12.0]: https://github.com/nmandery/h3ronpy/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/nmandery/h3ronpy/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/nmandery/h3ronpy/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/nmandery/h3ronpy/compare/v0.8.1...v0.9.0
