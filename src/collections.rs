use pyo3::prelude::*;

use crate::cells_to_h3indexes;
use crate::error::IntoPyResult;
use h3ron::error::check_valid_h3_resolution;
use h3ron::{collections as h3c, H3Cell, Index};
use numpy::{IntoPyArray, PyArray1};

#[pyclass]
pub struct H3CompactedVec {
    pub(crate) inner: h3c::CompactedCellVec,
}

#[pymethods]
impl H3CompactedVec {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn len_resolutions(&self) -> Vec<usize> {
        self.inner.len_resolutions()
    }

    #[getter]
    fn get_is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// check if the stack contains the index or any of its parents
    ///
    /// This function is pretty inefficient.
    fn contains(&self, h3index: u64) -> bool {
        self.inner.contains(H3Cell::new(h3index))
    }

    fn compacted_indexes(&self) -> Py<PyArray1<u64>> {
        let indexes: Vec<_> = self.inner.iter_compacted_cells().collect();
        return_h3index_array(indexes)
    }

    fn compacted_indexes_at_resolution(&self, h3_resolution: u8) -> PyResult<Py<PyArray1<u64>>> {
        check_valid_h3_resolution(h3_resolution).into_pyresult()?;
        let cells = self
            .inner
            .get_compacted_cells_at_resolution(h3_resolution)
            .to_vec();
        Ok(return_h3index_array(cells))
    }

    fn uncompacted_indexes_at_resolution(&self, h3_resolution: u8) -> PyResult<Py<PyArray1<u64>>> {
        check_valid_h3_resolution(h3_resolution).into_pyresult()?;
        let cells = self
            .inner
            .iter_uncompacted_cells(h3_resolution)
            .collect::<Result<Vec<_>, _>>()
            .into_pyresult()?;
        Ok(return_h3index_array(cells))
    }
}

#[inline]
fn return_h3index_array(cells: Vec<H3Cell>) -> Py<PyArray1<u64>> {
    Python::with_gil(|py| cells_to_h3indexes(cells).into_pyarray(py).to_owned())
}
