import h3.api.numpy_int as h3
import numpy as np
import polars as pl
import pyarrow as pa
from arro3.core import RecordBatch
from h3ronpy import (
    grid_disk,
    grid_disk_aggregate_k,
    grid_disk_distances,
    grid_ring_distances,
)


def test_grid_disk():
    h3indexes = np.array(
        [
            h3.latlng_to_cell(10.3, 45.1, 8),
            h3.latlng_to_cell(5.3, -5.1, 8),
        ],
        dtype=np.uint64,
    )
    disks = grid_disk(h3indexes, 2)
    assert len(disks) == 2
    # Arro3 has some bugs to fix around data type equality for nested types
    assert pa.field(disks.type).type == pa.large_list(pa.uint64())

    disks_flat = grid_disk(h3indexes, 2, flatten=True)
    assert len(disks_flat) > 20
    assert disks_flat.type == pa.uint64()


def test_grid_disk_distances():
    h3indexes = np.array(
        [
            h3.latlng_to_cell(10.3, 45.1, 8),
            h3.latlng_to_cell(5.3, -5.1, 8),
        ],
        dtype=np.uint64,
    )
    disks = grid_disk_distances(h3indexes, 2)
    assert type(disks) == RecordBatch
    assert disks.num_rows == len(h3indexes)

    # Arro3 has some bugs to fix around data type equality for nested types
    assert pa.field(disks["cell"].type).type == pa.large_list(pa.uint64())
    assert pa.field(disks["k"].type).type == pa.large_list(pa.uint32())

    centers = pl.DataFrame(grid_disk_distances(h3indexes, 2, flatten=True)).filter(
        pl.col("cell").is_in(pl.Series(h3indexes))
    )
    assert len(centers) == len(h3indexes)
    assert len(centers["k"].unique()) == 1
    assert centers["k"].unique()[0] == 0

    # TODO: check values


def test_grid_ring_distances():
    h3indexes = np.array(
        [
            h3.latlng_to_cell(10.3, 45.1, 8),
            h3.latlng_to_cell(5.3, -5.1, 8),
        ],
        dtype=np.uint64,
    )
    disks = grid_ring_distances(h3indexes, 1, 2)
    assert type(disks) == RecordBatch
    assert disks.num_rows == len(h3indexes)

    # Arro3 has some bugs to fix around data type equality for nested types
    assert pa.field(disks["cell"].type).type == pa.large_list(pa.uint64())
    assert pa.field(disks["k"].type).type == pa.large_list(pa.uint32())

    centers = pl.DataFrame(grid_ring_distances(h3indexes, 1, 2, flatten=True)).filter(
        pl.col("cell").is_in(pl.Series(h3indexes))
    )
    assert len(centers) == 0

    # TODO: check values


def test_grid_disk_aggregate_k():
    h3indexes = np.array(
        [
            h3.latlng_to_cell(10.3, 45.1, 8),
            h3.latlng_to_cell(5.3, -5.1, 8),
        ],
        dtype=np.uint64,
    )
    disks = grid_disk_aggregate_k(h3indexes, 2, "max")
    assert type(disks) == RecordBatch
    assert disks["cell"].type == pa.uint64()
    assert disks["k"].type == pa.uint32()

    # TODO: check values
