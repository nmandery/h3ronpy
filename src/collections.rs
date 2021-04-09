use pyo3::prelude::*;

use crate::error::IntoPyResult;
use h3ron::collections as h3c;
use h3ron::error::check_valid_h3_resolution;
use h3ron_h3_sys::H3Index;
use numpy::{IntoPyArray, PyArray1};

#[pyclass]
pub struct H3CompactedVec {
    pub(crate) inner: h3c::H3CompactedVec,
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
        self.inner.contains(h3index)
    }

    fn compacted_indexes(&self) -> Py<PyArray1<u64>> {
        let indexes: Vec<_> = self.inner.iter_compacted_indexes().collect();
        return_h3indexes_array(indexes)
    }

    fn compacted_indexes_at_resolution(&self, h3_resolution: u8) -> PyResult<Py<PyArray1<u64>>> {
        check_valid_h3_resolution(h3_resolution).into_pyresult()?;
        let indexes = self
            .inner
            .get_compacted_indexes_at_resolution(h3_resolution)
            .to_vec();
        Ok(return_h3indexes_array(indexes))
    }

    fn uncompacted_indexes_at_resolution(&self, h3_resolution: u8) -> PyResult<Py<PyArray1<u64>>> {
        check_valid_h3_resolution(h3_resolution).into_pyresult()?;
        let indexes = self.inner.iter_uncompacted_indexes(h3_resolution).collect();
        Ok(return_h3indexes_array(indexes))
    }
}

fn return_h3indexes_array(h3indexes: Vec<H3Index>) -> Py<PyArray1<u64>> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    h3indexes.into_pyarray(py).to_owned()
}
