use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use h3arrow::array::{CellIndexArray, DirectedEdgeIndexArray, VertexIndexArray};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use pyo3_arrow::ffi::to_array_pycapsules;

use crate::arrow_interop::{
    pyarray_to_cellindexarray, pyarray_to_directededgeindexarray, pyarray_to_vertexindexarray,
};
use crate::resolution::PyResolution;

#[pyclass(name = "CellArray")]
pub struct PyCellArray(CellIndexArray);

impl PyCellArray {
    pub fn into_inner(self) -> CellIndexArray {
        self.0
    }
}

#[pymethods]
impl PyCellArray {
    #[pyo3(signature = (requested_schema = None))]
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

    fn parent(&self, resolution: PyResolution) -> Self {
        Self(self.0.parent(resolution.into()))
    }

    fn slice(&self, offset: usize, length: usize) -> Self {
        Self(self.0.slice(offset, length))
    }
}

impl AsRef<CellIndexArray> for PyCellArray {
    fn as_ref(&self) -> &CellIndexArray {
        &self.0
    }
}

impl<'py> FromPyObject<'py> for PyCellArray {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(Self(pyarray_to_cellindexarray(ob)?))
    }
}

#[pyclass(name = "DirectedEdgeArray")]
pub struct PyDirectedEdgeArray(DirectedEdgeIndexArray);

#[pymethods]
impl PyDirectedEdgeArray {
    #[pyo3(signature = (requested_schema = None))]
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

impl AsRef<DirectedEdgeIndexArray> for PyDirectedEdgeArray {
    fn as_ref(&self) -> &DirectedEdgeIndexArray {
        &self.0
    }
}

impl<'py> FromPyObject<'py> for PyDirectedEdgeArray {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(Self(pyarray_to_directededgeindexarray(ob)?))
    }
}

#[pyclass(name = "VertexArray")]
pub struct PyVertexArray(VertexIndexArray);

#[pymethods]
impl PyVertexArray {
    #[pyo3(signature = (requested_schema = None))]
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

impl AsRef<VertexIndexArray> for PyVertexArray {
    fn as_ref(&self) -> &VertexIndexArray {
        &self.0
    }
}

impl<'py> FromPyObject<'py> for PyVertexArray {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(Self(pyarray_to_vertexindexarray(ob)?))
    }
}
