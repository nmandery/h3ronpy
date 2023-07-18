use h3arrow::algorithm::{ParseUtf8Array, ToUtf8Array};
use h3arrow::array::CellIndexArray;
use h3arrow::export::arrow2::array::{Array, Utf8Array};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::arrow_interop::*;
use crate::error::IntoPyResult;

#[pyfunction]
#[pyo3(signature = (utf8array, set_failing_to_invalid = false))]
pub(crate) fn cells_parse(utf8array: &PyAny, set_failing_to_invalid: bool) -> PyResult<PyObject> {
    let boxed_array = pyarray_to_boxed(utf8array)?;
    let cells = if let Some(utf8array) = boxed_array.as_any().downcast_ref::<Utf8Array<i32>>() {
        CellIndexArray::parse_utf8array(utf8array, set_failing_to_invalid).into_pyresult()?
    } else if let Some(utf8array) = boxed_array.as_any().downcast_ref::<Utf8Array<i64>>() {
        CellIndexArray::parse_utf8array(utf8array, set_failing_to_invalid).into_pyresult()?
    } else {
        return Err(PyValueError::new_err(
            "unsupported array type to parse cells from",
        ));
    };

    with_pyarrow(|py, pyarrow| h3array_to_pyarray(cells, py, pyarrow))
}

#[pyfunction]
#[pyo3(signature = (cellarray))]
pub(crate) fn cells_to_string(cellarray: &PyAny) -> PyResult<PyObject> {
    let utf8array: Utf8Array<i64> = pyarray_to_cellindexarray(cellarray)?
        .to_utf8array()
        .into_pyresult()?;

    with_pyarrow(|py, pyarrow| native_to_pyarray(utf8array.to_boxed(), py, pyarrow))
}

#[pyfunction]
#[pyo3(signature = (vertexarray))]
pub(crate) fn vertexes_to_string(vertexarray: &PyAny) -> PyResult<PyObject> {
    let utf8array: Utf8Array<i64> = pyarray_to_vertexindexarray(vertexarray)?
        .to_utf8array()
        .into_pyresult()?;

    with_pyarrow(|py, pyarrow| native_to_pyarray(utf8array.to_boxed(), py, pyarrow))
}

#[pyfunction]
#[pyo3(signature = (directededgearray))]
pub(crate) fn directededges_to_string(directededgearray: &PyAny) -> PyResult<PyObject> {
    let utf8array: Utf8Array<i64> = pyarray_to_directededgeindexarray(directededgearray)?
        .to_utf8array()
        .into_pyresult()?;

    with_pyarrow(|py, pyarrow| native_to_pyarray(utf8array.to_boxed(), py, pyarrow))
}
