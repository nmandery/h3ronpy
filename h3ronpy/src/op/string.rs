use arrow::array::{make_array, Array, ArrayData, LargeStringArray, StringArray};
use arrow::pyarrow::{FromPyArrow, IntoPyArrow};
use h3arrow::algorithm::{ParseGenericStringArray, ToGenericStringArray};
use h3arrow::array::{CellIndexArray, DirectedEdgeIndexArray, VertexIndexArray};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::arrow_interop::*;
use crate::error::IntoPyResult;

#[pyfunction]
#[pyo3(signature = (stringarray, set_failing_to_invalid = false))]
pub(crate) fn cells_parse(stringarray: &PyAny, set_failing_to_invalid: bool) -> PyResult<PyObject> {
    let boxed_array = make_array(ArrayData::from_pyarrow(stringarray)?);
    let cells = if let Some(stringarray) = boxed_array.as_any().downcast_ref::<StringArray>() {
        CellIndexArray::parse_genericstringarray(stringarray, set_failing_to_invalid)
            .into_pyresult()?
    } else if let Some(stringarray) = boxed_array.as_any().downcast_ref::<LargeStringArray>() {
        CellIndexArray::parse_genericstringarray(stringarray, set_failing_to_invalid)
            .into_pyresult()?
    } else {
        return Err(PyValueError::new_err(
            "unsupported array type to parse cells from",
        ));
    };

    Python::with_gil(|py| h3array_to_pyarray(cells, py))
}

#[pyfunction]
#[pyo3(signature = (stringarray, set_failing_to_invalid = false))]
pub(crate) fn vertexes_parse(
    stringarray: &PyAny,
    set_failing_to_invalid: bool,
) -> PyResult<PyObject> {
    let boxed_array = make_array(ArrayData::from_pyarrow(stringarray)?);
    let vertexes = if let Some(utf8array) = boxed_array.as_any().downcast_ref::<StringArray>() {
        VertexIndexArray::parse_genericstringarray(utf8array, set_failing_to_invalid)
            .into_pyresult()?
    } else if let Some(utf8array) = boxed_array.as_any().downcast_ref::<LargeStringArray>() {
        VertexIndexArray::parse_genericstringarray(utf8array, set_failing_to_invalid)
            .into_pyresult()?
    } else {
        return Err(PyValueError::new_err(
            "unsupported array type to parse vertexes from",
        ));
    };

    Python::with_gil(|py| h3array_to_pyarray(vertexes, py))
}

#[pyfunction]
#[pyo3(signature = (stringarray, set_failing_to_invalid = false))]
pub(crate) fn directededges_parse(
    stringarray: &PyAny,
    set_failing_to_invalid: bool,
) -> PyResult<PyObject> {
    let boxed_array = make_array(ArrayData::from_pyarrow(stringarray)?);
    let edges = if let Some(stringarray) = boxed_array.as_any().downcast_ref::<StringArray>() {
        DirectedEdgeIndexArray::parse_genericstringarray(stringarray, set_failing_to_invalid)
            .into_pyresult()?
    } else if let Some(stringarray) = boxed_array.as_any().downcast_ref::<LargeStringArray>() {
        DirectedEdgeIndexArray::parse_genericstringarray(stringarray, set_failing_to_invalid)
            .into_pyresult()?
    } else {
        return Err(PyValueError::new_err(
            "unsupported array type to parse directededges from",
        ));
    };

    Python::with_gil(|py| h3array_to_pyarray(edges, py))
}

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_to_string(cellarray: &PyAny) -> PyResult<PyObject> {
    let stringarray: LargeStringArray = pyarray_to_cellindexarray(cellarray)?
        .to_genericstringarray()
        .into_pyresult()?;

    Python::with_gil(|py| stringarray.into_data().into_pyarrow(py))
}

#[pyfunction]
#[pyo3(signature = (vertexarray))]
pub(crate) fn vertexes_to_string(vertexarray: &PyAny) -> PyResult<PyObject> {
    let stringarray: LargeStringArray = pyarray_to_vertexindexarray(vertexarray)?
        .to_genericstringarray()
        .into_pyresult()?;

    Python::with_gil(|py| stringarray.into_data().into_pyarrow(py))
}

#[pyfunction]
#[pyo3(signature = (directededgearray))]
pub(crate) fn directededges_to_string(directededgearray: &PyAny) -> PyResult<PyObject> {
    let stringarray: LargeStringArray = pyarray_to_directededgeindexarray(directededgearray)?
        .to_genericstringarray()
        .into_pyresult()?;

    Python::with_gil(|py| stringarray.into_data().into_pyarrow(py))
}
