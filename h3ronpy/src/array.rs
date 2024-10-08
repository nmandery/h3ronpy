use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use h3arrow::array::{CellIndexArray, DirectedEdgeIndexArray, VertexIndexArray};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use pyo3_arrow::ffi::to_array_pycapsules;

#[pyclass(name = "CellArray")]
pub struct PyCellArray(CellIndexArray);

#[pymethods]
impl PyCellArray {
    fn __arrow_c_array__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let array = self.0.primitive_array();
        let field = Arc::new(Field::new("", DataType::UInt64, true));
        Ok(to_array_pycapsules(py, field, array, requested_schema)?)
    }

    fn __len__(&self) -> usize {
        self.0.len()
    }

    fn slice(&self, offset: usize, length: usize) -> Self {
        Self(self.0.slice(offset, length))
    }
}

#[pyclass(name = "DirectedEdgeArray")]
pub struct PyDirectedEdgeArray(DirectedEdgeIndexArray);

#[pymethods]
impl PyDirectedEdgeArray {
    fn __arrow_c_array__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let array = self.0.primitive_array();
        let field = Arc::new(Field::new("", DataType::UInt64, true));
        Ok(to_array_pycapsules(py, field, array, requested_schema)?)
    }

    fn __len__(&self) -> usize {
        self.0.len()
    }

    pub fn origin(&self) -> PyCellArray {
        PyCellArray(self.0.origin())
    }

    pub fn destination(&self) -> PyCellArray {
        PyCellArray(self.0.destination())
    }

    fn slice(&self, offset: usize, length: usize) -> Self {
        Self(self.0.slice(offset, length))
    }
}

#[pyclass(name = "VertexArray")]
pub struct PyVertexArray(VertexIndexArray);

#[pymethods]
impl PyVertexArray {
    fn __arrow_c_array__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let array = self.0.primitive_array();
        let field = Arc::new(Field::new("", DataType::UInt64, true));
        Ok(to_array_pycapsules(py, field, array, requested_schema)?)
    }

    fn __len__(&self) -> usize {
        self.0.len()
    }

    pub fn owner(&self) -> PyCellArray {
        PyCellArray(self.0.owner())
    }

    fn slice(&self, offset: usize, length: usize) -> Self {
        Self(self.0.slice(offset, length))
    }
}
