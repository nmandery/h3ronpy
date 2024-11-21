use std::sync::Arc;

use arrow::array::{Array, LargeStringArray, StringArray};
use h3arrow::algorithm::{ParseGenericStringArray, ToGenericStringArray};
use h3arrow::array::{CellIndexArray, DirectedEdgeIndexArray, VertexIndexArray};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::PyArray;

use crate::array::PyCellArray;
use crate::arrow_interop::*;
use crate::error::IntoPyResult;

#[pyfunction]
#[pyo3(signature = (stringarray, set_failing_to_invalid = false))]
pub(crate) fn cells_parse(
    py: Python<'_>,
    stringarray: PyArray,
    set_failing_to_invalid: bool,
) -> PyResult<PyObject> {
    let (boxed_array, _field) = stringarray.into_inner();
    let cells = py.allow_threads(|| {
        if let Some(stringarray) = boxed_array.as_any().downcast_ref::<StringArray>() {
            CellIndexArray::parse_genericstringarray(stringarray, set_failing_to_invalid)
                .into_pyresult()
        } else if let Some(stringarray) = boxed_array.as_any().downcast_ref::<LargeStringArray>() {
            CellIndexArray::parse_genericstringarray(stringarray, set_failing_to_invalid)
                .into_pyresult()
        } else {
            Err(PyValueError::new_err(
                "unsupported array type to parse cells from",
            ))
        }
    })?;

    h3array_to_pyarray(cells, py)
}

#[pyfunction]
#[pyo3(signature = (stringarray, set_failing_to_invalid = false))]
pub(crate) fn vertexes_parse(
    py: Python<'_>,
    stringarray: PyArray,
    set_failing_to_invalid: bool,
) -> PyResult<PyObject> {
    let (boxed_array, _field) = stringarray.into_inner();
    let vertexes = py.allow_threads(|| {
        if let Some(utf8array) = boxed_array.as_any().downcast_ref::<StringArray>() {
            VertexIndexArray::parse_genericstringarray(utf8array, set_failing_to_invalid)
                .into_pyresult()
        } else if let Some(utf8array) = boxed_array.as_any().downcast_ref::<LargeStringArray>() {
            VertexIndexArray::parse_genericstringarray(utf8array, set_failing_to_invalid)
                .into_pyresult()
        } else {
            Err(PyValueError::new_err(
                "unsupported array type to parse vertexes from",
            ))
        }
    })?;

    h3array_to_pyarray(vertexes, py)
}

#[pyfunction]
#[pyo3(signature = (stringarray, set_failing_to_invalid = false))]
pub(crate) fn directededges_parse(
    py: Python<'_>,
    stringarray: PyArray,
    set_failing_to_invalid: bool,
) -> PyResult<PyObject> {
    let (boxed_array, _field) = stringarray.into_inner();
    let edges = py.allow_threads(|| {
        if let Some(stringarray) = boxed_array.as_any().downcast_ref::<StringArray>() {
            DirectedEdgeIndexArray::parse_genericstringarray(stringarray, set_failing_to_invalid)
                .into_pyresult()
        } else if let Some(stringarray) = boxed_array.as_any().downcast_ref::<LargeStringArray>() {
            DirectedEdgeIndexArray::parse_genericstringarray(stringarray, set_failing_to_invalid)
                .into_pyresult()
        } else {
            Err(PyValueError::new_err(
                "unsupported array type to parse directededges from",
            ))
        }
    })?;

    h3array_to_pyarray(edges, py)
}

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_to_string(py: Python, cellarray: PyCellArray) -> PyResult<PyObject> {
    let stringarray: LargeStringArray =
        cellarray.as_ref().to_genericstringarray().into_pyresult()?;
    PyArray::from_array_ref(Arc::new(stringarray)).to_arro3(py)
}

#[pyfunction]
#[pyo3(signature = (vertexarray))]
pub(crate) fn vertexes_to_string(py: Python, vertexarray: &Bound<PyAny>) -> PyResult<PyObject> {
    let stringarray: LargeStringArray = pyarray_to_vertexindexarray(vertexarray)?
        .to_genericstringarray()
        .into_pyresult()?;
    PyArray::from_array_ref(Arc::new(stringarray)).to_arro3(py)
}

#[pyfunction]
#[pyo3(signature = (directededgearray))]
pub(crate) fn directededges_to_string(
    py: Python,
    directededgearray: &Bound<PyAny>,
) -> PyResult<PyObject> {
    let stringarray: LargeStringArray = pyarray_to_directededgeindexarray(directededgearray)?
        .to_genericstringarray()
        .into_pyresult()?;
    PyArray::from_array_ref(Arc::new(stringarray)).to_arro3(py)
}
