use crate::array::{CellIndexArray, H3Array, H3ListArray, H3ListArrayBuilder};
use crate::error::Error;
use ahash::{HashMap, HashMapExt};
use arrow::array::{
    Array, GenericListArray, GenericListBuilder, OffsetSizeTrait, PrimitiveArray, UInt32Array,
    UInt32Builder,
};
use h3o::{max_grid_disk_size, CellIndex};
use std::cmp::{max, min};
use std::collections::hash_map::Entry;

pub struct GridDiskDistances<O: OffsetSizeTrait> {
    pub cells: H3ListArray<CellIndex, O>,
    pub distances: GenericListArray<O>,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum KAggregationMethod {
    Min,
    Max,
}

pub struct GridDiskAggregateK {
    pub cells: CellIndexArray,
    pub distances: UInt32Array,
}

pub trait GridOp
where
    Self: Sized,
{
    fn grid_disk<O: OffsetSizeTrait>(&self, k: u32) -> Result<H3ListArray<CellIndex, O>, Error>;
    fn grid_disk_distances<O: OffsetSizeTrait>(
        &self,
        k: u32,
    ) -> Result<GridDiskDistances<O>, Error>;
    fn grid_ring_distances<O: OffsetSizeTrait>(
        &self,
        k_min: u32,
        k_max: u32,
    ) -> Result<GridDiskDistances<O>, Error>;
    fn grid_disk_aggregate_k(
        &self,
        k: u32,
        k_agg_method: KAggregationMethod,
    ) -> Result<GridDiskAggregateK, Error>;
}

impl GridOp for H3Array<CellIndex> {
    fn grid_disk<O: OffsetSizeTrait>(&self, k: u32) -> Result<H3ListArray<CellIndex, O>, Error> {
        let mut builder = H3ListArrayBuilder::with_capacity(
            self.len(),
            self.len() * max_grid_disk_size(k) as usize,
        );

        for cell in self.iter() {
            match cell {
                Some(cell) => {
                    let disc: Vec<_> = cell.grid_disk(k);
                    builder.values().append_many(disc);
                    builder.append(true);
                }
                None => {
                    builder.append(false);
                }
            }
        }
        builder.finish()
    }

    fn grid_disk_distances<O: OffsetSizeTrait>(
        &self,
        k: u32,
    ) -> Result<GridDiskDistances<O>, Error> {
        build_grid_disk(self, k, |_, _| true)
    }

    fn grid_ring_distances<O: OffsetSizeTrait>(
        &self,
        k_min: u32,
        k_max: u32,
    ) -> Result<GridDiskDistances<O>, Error> {
        build_grid_disk(self, k_max, |_, k| k >= k_min)
    }

    fn grid_disk_aggregate_k(
        &self,
        k: u32,
        k_agg_method: KAggregationMethod,
    ) -> Result<GridDiskAggregateK, Error> {
        let mut cellmap: HashMap<CellIndex, u32> = HashMap::with_capacity(self.len());
        for cell in self.iter().flatten() {
            for (grid_cell, grid_distance) in cell.grid_disk_distances::<Vec<_>>(k).into_iter() {
                match cellmap.entry(grid_cell) {
                    Entry::Occupied(mut e) => {
                        e.insert(match k_agg_method {
                            KAggregationMethod::Min => min(*e.get(), grid_distance),
                            KAggregationMethod::Max => max(*e.get(), grid_distance),
                        });
                    }
                    Entry::Vacant(e) => {
                        e.insert(grid_distance);
                    }
                };
            }
        }

        let mut cells = Vec::with_capacity(cellmap.len());
        let mut distances = Vec::with_capacity(cellmap.len());

        for (cell, distance) in cellmap.into_iter() {
            cells.push(cell);
            distances.push(distance);
        }

        Ok(GridDiskAggregateK {
            cells: CellIndexArray::from(cells),
            distances: PrimitiveArray::new(distances.into(), None),
        })
    }
}

fn build_grid_disk<F, O: OffsetSizeTrait>(
    cellindexarray: &CellIndexArray,
    k: u32,
    filter: F,
) -> Result<GridDiskDistances<O>, Error>
where
    F: Fn(CellIndex, u32) -> bool,
{
    let mut grid_cells_builder = H3ListArrayBuilder::with_capacity(
        cellindexarray.len(),
        cellindexarray.len(), // TODO: multiply with k or k_max-k_min
    );
    let mut grid_distancess_builder = GenericListBuilder::with_capacity(
        UInt32Builder::with_capacity(
            cellindexarray.len(), // TODO: multiply with k or k_max-k_min
        ),
        cellindexarray.len(),
    );

    for cell in cellindexarray.iter() {
        let is_valid = match cell {
            Some(cell) => {
                for (grid_cell, grid_distance) in cell.grid_disk_distances::<Vec<_>>(k).into_iter()
                {
                    if filter(grid_cell, grid_distance) {
                        grid_cells_builder.values().append_value(grid_cell);
                        grid_distancess_builder.values().append_value(grid_distance);
                    }
                }

                true
            }
            None => false,
        };

        grid_cells_builder.append(is_valid);
        grid_distancess_builder.append(is_valid)
    }

    let grid_cells = grid_cells_builder.finish()?;
    let grid_distances = grid_distancess_builder.finish();

    debug_assert_eq!(grid_cells.len(), grid_distances.len());

    Ok(GridDiskDistances {
        cells: grid_cells,
        distances: grid_distances,
    })
}
