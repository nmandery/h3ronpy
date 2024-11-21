# Changelog

All notable changes to this project will be documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased (YYYY-MM-DD TBD)

* Update h3o to 0.7.
* Added H3ArrayBuilder type.
* Added LocalIj coordinate support.

## v0.4.0 (2024-03-01)

* Update h3o to 0.6.
* Upgrade geo to 0.28
* Upgrade rstar to 0.12
* Upgrade geozero to 0.12

## v0.3.0 (2024-02-06)

* Extend documentation on ParseUtf8Array::parse_utf8array.
* Add ChangeResolutionOp::change_resolution_list.
* Update geozero to 0.11.
* Update h3o to 0.5.
* Migrate from arrow2 to the official apache arrow implementation and aligned naming. This comes along with many API
  changes. `geoarrow::ToWKBLines` has been removed.

## v0.2.0 (2023-08-31)

* Upgrade h3o from v0.3 to v0.4. Due to the new polyfill modes this lead to API breakages in the `ToCellsOptions`
  struct.

## v0.1.0 (2023-07-24)

* Initial release
