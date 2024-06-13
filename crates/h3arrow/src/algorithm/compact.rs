use crate::array::CellIndexArray;
use crate::error::Error;
use ahash::HashSet;
use h3o::{CellIndex, Resolution};

pub trait CompactOp
where
    Self: Sized,
{
    /// expects all indexes to be at the same resolution
    fn compact(&self) -> Result<Self, Error>;

    fn compact_mixed_resolutions(&self) -> Result<Self, Error>;

    fn uncompact(&self, resolution: Resolution) -> Self;
}

impl CompactOp for CellIndexArray {
    fn compact(&self) -> Result<Self, Error> {
        Ok(CellIndex::compact(self.iter().flatten())?.collect())
    }

    fn compact_mixed_resolutions(&self) -> Result<Self, Error> {
        let mut cellset = CellSet::default();
        for cell in self.iter().flatten() {
            cellset.insert(cell);
        }
        cellset.finalize(true)?;

        Ok(Self::from_iter(cellset.iter_compacted()))
    }

    fn uncompact(&self, resolution: Resolution) -> Self {
        CellIndex::uncompact(self.iter().flatten(), resolution).collect()
    }
}

struct CellSet {
    pub(crate) modified_resolutions: [bool; 16],

    /// cells by their resolution. The index of the array is the resolution for the referenced vec
    pub(crate) cells_by_resolution: [Vec<CellIndex>; 16],
}

impl CellSet {
    #[allow(unused)]
    pub(crate) fn append(&mut self, other: &mut Self) {
        for ((r_idx, sink), source) in self
            .cells_by_resolution
            .iter_mut()
            .enumerate()
            .zip(other.cells_by_resolution.iter_mut())
        {
            if source.is_empty() {
                continue;
            }
            self.modified_resolutions[r_idx] = true;
            sink.append(source);
        }
    }

    pub(crate) fn compact(&mut self) -> Result<(), Error> {
        self.dedup(false, false);

        if let Some((min_touched_res, _)) = self
            .modified_resolutions
            .iter()
            .enumerate()
            .rev()
            .find(|(_, modified)| **modified)
        {
            let mut res = Some(Resolution::try_from(min_touched_res as u8)?);

            while let Some(h3_res) = res {
                let r_idx: usize = h3_res.into();
                let mut compacted_in = std::mem::take(&mut self.cells_by_resolution[r_idx]);
                compacted_in.sort_unstable();
                compacted_in.dedup();
                for cell in CellIndex::compact(compacted_in.into_iter())? {
                    self.insert(cell);
                }
                res = h3_res.pred();
            }

            // mark all resolutions as not-modified
            self.modified_resolutions
                .iter_mut()
                .for_each(|r| *r = false);
        }

        self.dedup(true, true);

        Ok(())
    }

    pub fn iter_compacted(&self) -> Box<dyn Iterator<Item = CellIndex> + '_> {
        Box::new(
            self.cells_by_resolution
                .iter()
                .flat_map(|v| v.iter())
                .copied(),
        )
    }

    #[allow(unused)]
    pub fn iter_uncompacted(&self, r: Resolution) -> Box<dyn Iterator<Item = CellIndex> + '_> {
        let r_idx: usize = r.into();
        Box::new((0..=r_idx).flat_map(move |r_idx| {
            self.cells_by_resolution[r_idx]
                .iter()
                .flat_map(move |cell| cell.children(r))
        }))
    }

    #[allow(unused)]
    pub fn len(&self) -> usize {
        self.cells_by_resolution.iter().map(|v| v.len()).sum()
    }

    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        !self.cells_by_resolution.iter().any(|v| !v.is_empty())
    }

    pub(crate) fn insert(&mut self, cell: CellIndex) {
        let idx: usize = cell.resolution().into();
        self.cells_by_resolution[idx].push(cell);
        self.modified_resolutions[idx] = true;
    }

    pub(crate) fn dedup(&mut self, shrink: bool, parents: bool) {
        fn dedup_vec(v: &mut Vec<CellIndex>, shrink: bool) {
            v.sort_unstable();
            v.dedup();
            if shrink {
                v.shrink_to_fit();
            }
        }

        #[cfg(feature = "rayon")]
        {
            use rayon::prelude::{IntoParallelRefMutIterator, ParallelIterator};
            self.cells_by_resolution.par_iter_mut().for_each(|v| {
                dedup_vec(v, shrink);
            });
        }

        #[cfg(not(feature = "rayon"))]
        self.cells_by_resolution.iter_mut().for_each(|v| {
            dedup_vec(v, shrink);
        });

        if parents
            && self
                .cells_by_resolution
                .iter()
                .filter(|v| !v.is_empty())
                .count()
                > 1
        {
            // remove cells whose parents are already contained
            let mut seen = HashSet::default();
            for v in self.cells_by_resolution.iter_mut() {
                if !seen.is_empty() {
                    v.retain(|cell| {
                        let mut is_contained = false;
                        let mut r = Some(cell.resolution());
                        while let Some(resolution) = r {
                            if let Some(cell) = cell.parent(resolution) {
                                if seen.contains(&cell) {
                                    is_contained = true;
                                    break;
                                }
                            }
                            r = resolution.pred();
                        }
                        !is_contained
                    });
                }
                seen.extend(v.iter().copied());
            }
        }
    }

    pub(crate) fn finalize(&mut self, compact: bool) -> Result<(), Error> {
        if compact {
            self.compact()?;
        } else {
            self.dedup(true, true);
        }
        Ok(())
    }
}

#[allow(clippy::derivable_impls)]
impl Default for CellSet {
    fn default() -> Self {
        Self {
            modified_resolutions: [false; 16],
            cells_by_resolution: Default::default(),
        }
    }
}
