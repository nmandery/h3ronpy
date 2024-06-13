from h3ronpy.polars import grid_disk, grid_disk_distances, grid_ring_distances, grid_disk_aggregate_k
import numpy as np
import h3.api.numpy_int as h3
import polars as pl


def test_grid_disk():
    h3indexes = np.array(
        [
            h3.geo_to_h3(10.3, 45.1, 8),
            h3.geo_to_h3(5.3, -5.1, 8),
        ],
        dtype=np.uint64,
    )
    disks = grid_disk(h3indexes, 2)
    assert len(disks) == 2
    assert disks.dtype == pl.List(pl.UInt64())

    disks_flat = grid_disk(h3indexes, 2, flatten=True)
    assert len(disks_flat) > 20
    assert disks_flat.dtype == pl.UInt64()


def test_grid_disk_distances():
    h3indexes = np.array(
        [
            h3.geo_to_h3(10.3, 45.1, 8),
            h3.geo_to_h3(5.3, -5.1, 8),
        ],
        dtype=np.uint64,
    )
    disks = grid_disk_distances(h3indexes, 2)
    assert type(disks) == pl.DataFrame
    assert len(disks) == len(h3indexes)
    assert disks["cell"].dtype == pl.List(pl.UInt64())
    assert disks["k"].dtype == pl.List(pl.UInt32())

    centers = (
        grid_disk_distances(h3indexes, 2, flatten=True)
        .lazy()
        .filter(pl.col("cell").is_in(pl.Series(h3indexes)))
        .collect()
    )
    assert len(centers) == len(h3indexes)
    assert len(centers["k"].unique()) == 1
    assert centers["k"].unique()[0] == 0

    # TODO: check values


def test_grid_ring_distances():
    h3indexes = np.array(
        [
            h3.geo_to_h3(10.3, 45.1, 8),
            h3.geo_to_h3(5.3, -5.1, 8),
        ],
        dtype=np.uint64,
    )
    disks = grid_ring_distances(h3indexes, 1, 2)
    assert type(disks) == pl.DataFrame
    assert len(disks) == len(h3indexes)
    assert disks["cell"].dtype == pl.List(pl.UInt64())
    assert disks["k"].dtype == pl.List(pl.UInt32())

    centers = (
        grid_ring_distances(h3indexes, 1, 2, flatten=True)
        .lazy()
        .filter(pl.col("cell").is_in(pl.Series(h3indexes)))
        .collect()
    )
    assert len(centers) == 0

    # TODO: check values


def test_grid_disk_aggregate_k():
    h3indexes = np.array(
        [
            h3.geo_to_h3(10.3, 45.1, 8),
            h3.geo_to_h3(5.3, -5.1, 8),
        ],
        dtype=np.uint64,
    )
    disks = grid_disk_aggregate_k(h3indexes, 2, "max")
    assert type(disks) == pl.DataFrame
    assert disks["cell"].dtype == pl.UInt64()
    assert disks["k"].dtype == pl.UInt32()

    # TODO: check values
